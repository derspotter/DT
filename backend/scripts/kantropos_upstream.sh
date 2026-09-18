#!/usr/bin/env bash
set -Eeuo pipefail

CONTAINER="${RAG_FEEDER_BACKEND_CONTAINER:-rag_feeder_backend}"
TARGET_ID="${RAG_FEEDER_UPSTREAM_TARGET_ID:-anthropozan-nachhaltiges-management}"
TOOL="/usr/src/app/scripts/upstream_update.py"
PYTHON="${RAG_FEEDER_BACKEND_PYTHON:-/opt/venv/bin/python}"
HOST_PYTHON="${RAG_FEEDER_HOST_PYTHON:-python3}"
OCR_HOME="${RAG_FEEDER_OCR_HOME:-/home/spott/rechtmaschine-debian-rag-ocr}"
OCR_HOST_URL="${RAG_FEEDER_OCR_HOST_URL:-http://127.0.0.1:8004}"
OCR_CONTAINER_URL="${RAG_FEEDER_OCR_SERVICE_URL:-}"
CORPUS_UPDATER_CONTAINER="${RAG_FEEDER_KANTROPOS_UPDATER_CONTAINER:-kantropos-corpus-updater}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DT_OCR_PYTHON="${DT_OCR_VENV:-$REPO_DIR/.runtime/ocr/debian-20260918}/bin/python"
FLOW_STAGE="initialization"
FLOW_DRAFT=""
trap 'status=$?; echo "STOP: stage $FLOW_STAGE failed (exit $status). Later stages were NOT started. Saved draft: ${FLOW_DRAFT:-none}." >&2; exit "$status"' ERR

usage() {
  cat <<'EOF'
Usage:
  bash backend/scripts/kantropos_upstream.sh count
  bash backend/scripts/kantropos_upstream.sh draft [--limit N]
  bash backend/scripts/kantropos_upstream.sh scan-text [--draft-dir DIR]
  bash backend/scripts/kantropos_upstream.sh ocr <draft_dir> [--ocr-url URL]
  bash backend/scripts/kantropos_upstream.sh rag-flow [--draft-dir DIR] [--yes] [--skip-ocr] [--skip-apply] [--skip-markdown] [--skip-embed]
  bash backend/scripts/kantropos_upstream.sh validate <draft_dir>
  bash backend/scripts/kantropos_upstream.sh apply <draft_dir> [--yes]
  bash backend/scripts/kantropos_upstream.sh commands

Environment overrides:
  RAG_FEEDER_BACKEND_CONTAINER=rag_feeder_backend
  RAG_FEEDER_BACKEND_PYTHON=/opt/venv/bin/python
  RAG_FEEDER_HOST_PYTHON=python3
  RAG_FEEDER_UPSTREAM_TARGET_ID=anthropozan-nachhaltiges-management
  RAG_FEEDER_OCR_HOME=/home/spott/rechtmaschine-debian-rag-ocr
  RAG_FEEDER_OCR_HOST_URL=http://127.0.0.1:8004
  RAG_FEEDER_OCR_SERVICE_URL=http://<host-gateway>:8004
  RAG_FEEDER_KANTROPOS_UPDATER_CONTAINER=kantropos-corpus-updater
EOF
}

json_field() {
  "$HOST_PYTHON" -c "import json,sys; print(json.load(sys.stdin)$1)"
}

container_gateway() {
  docker inspect "$CONTAINER" | "$HOST_PYTHON" -c '
import json, sys
data = json.load(sys.stdin)[0]["NetworkSettings"]["Networks"]
for network in data.values():
    gateway = network.get("Gateway")
    if gateway:
        print(gateway)
        break
'
}

container_ocr_url() {
  if [[ -n "$OCR_CONTAINER_URL" ]]; then
    printf '%s\n' "$OCR_CONTAINER_URL"
    return
  fi
  local gateway
  gateway="$(container_gateway)"
  if [[ -z "$gateway" ]]; then
    echo "Could not discover Docker host gateway for $CONTAINER." >&2
    exit 1
  fi
  printf 'http://%s:8004\n' "$gateway"
}

ensure_ocr_service() {
  if curl -fsS "$OCR_HOST_URL/health" >/dev/null 2>&1; then
    return
  fi
  if [[ ! -x "$DT_OCR_PYTHON" ]]; then
    echo "Missing DT OCR runtime at $DT_OCR_PYTHON. See backend/ocr/README.md." >&2
    exit 1
  fi
  if [[ ! -f "$OCR_HOME/service_manager.py" ]]; then
    echo "Missing Rechtmaschine service_manager.py at $OCR_HOME." >&2
    exit 1
  fi
  mkdir -p "$REPO_DIR/logs"
  (
    cd "$REPO_DIR"
    env \
      SERVICE_MANAGER_ROLE=ocr \
      RAG_FEEDER_OCR_HOME="$OCR_HOME" \
      setsid "$DT_OCR_PYTHON" backend/ocr/launch_manager.py \
        >> "$REPO_DIR/logs/ocr-manager.log" 2>&1 < /dev/null &
  )
  for _ in $(seq 1 60); do
    if curl -fsS "$OCR_HOST_URL/health" >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  echo "OCR service did not become healthy at $OCR_HOST_URL." >&2
  echo "Check $REPO_DIR/logs/ocr-manager.log" >&2
  exit 1
}

call_updater_post() {
  local path="$1"
  docker exec "$CORPUS_UPDATER_CONTAINER" curl -fsS -X POST "http://localhost:8001${path}" &
  local request_pid=$! elapsed=0
  while kill -0 "$request_pid" 2>/dev/null; do
    sleep 2
    elapsed=$((elapsed + 2))
    if (( elapsed % 30 == 0 )); then
      echo "[$FLOW_STAGE] Still waiting (${elapsed}s). File-level progress: docker logs --tail 20 $CORPUS_UPDATER_CONTAINER" >&2
    fi
  done
  wait "$request_pid"
}

apply_draft() {
  local draft_dir="$1"
  shift
  local arg write=0
  for arg in "$@"; do
    [[ "$arg" != --yes ]] || write=1
    if [[ "$arg" == --target-path* ]]; then
      echo "Target override is not allowed in the scoped writer; use the reviewed manifest." >&2
      return 2
    fi
  done
  if [[ "$write" -eq 0 ]]; then
    docker exec "$CONTAINER" "$PYTHON" "$TOOL" apply --draft-dir "$draft_dir" "$@"
    return
  fi
  local target_dir image
  target_dir="$(docker exec "$CONTAINER" "$PYTHON" -c '
import json, pathlib, sys
manifest = json.loads((pathlib.Path(sys.argv[1]) / "manifest.json").read_text())
root = pathlib.Path("/data/projects/kantropos/corpora").resolve()
target = pathlib.Path(manifest["target"]["path"]).resolve()
if target.parent != root or not target.is_dir():
    raise SystemExit("Refusing writer: target must be one existing corpus directly below the corpora root")
print(target)
' "$draft_dir")"
  [[ -d "$target_dir" ]] || { echo "Missing host corpus: $target_dir" >&2; return 1; }
  image="$(docker inspect --format '{{.Image}}' "$CONTAINER")"
  # Only this corpus is writable. The web backend and all inherited mounts stay read-only.
  docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges \
    --user "$(id -u):$(id -g)" --volumes-from "$CONTAINER:ro" \
    --mount "type=bind,src=$target_dir,dst=$target_dir" \
    --entrypoint "$PYTHON" "$image" "$TOOL" apply --draft-dir "$draft_dir" "$@"
}

run_rag_flow() {
  local apply_yes=0
  local skip_ocr=0
  local skip_apply=0
  local skip_markdown=0
  local skip_embed=0
  local ocr_url_arg=""
  local resume_draft=""
  local draft_extra=()

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --yes) apply_yes=1 ;;
      --skip-ocr) skip_ocr=1 ;;
      --skip-apply) skip_apply=1 ;;
      --skip-markdown) skip_markdown=1 ;;
      --skip-embed) skip_embed=1 ;;
      --draft-dir)
        shift
        resume_draft="${1:?Missing value for --draft-dir}"
        ;;
      --ocr-url)
        shift
        ocr_url_arg="${1:-}"
        if [[ -z "$ocr_url_arg" ]]; then
          echo "Missing value for --ocr-url." >&2
          exit 2
        fi
        ;;
      --limit)
        draft_extra+=("$1" "${2:-}")
        shift
        ;;
      *)
        draft_extra+=("$1")
        ;;
    esac
    shift || true
  done

  local draft_json draft_dir target_name target_encoded ocr_url
  FLOW_STAGE="draft"
  if [[ -n "$resume_draft" ]]; then
    [[ ${#draft_extra[@]} -eq 0 ]] || { echo "Draft creation options cannot be combined with --draft-dir." >&2; return 2; }
    draft_dir="$resume_draft"
    target_name="$(docker exec "$CONTAINER" "$PYTHON" -c 'import json,pathlib,sys; print(json.loads((pathlib.Path(sys.argv[1]) / "manifest.json").read_text())["target"]["name"])' "$draft_dir")"
    echo "Resuming saved draft: $draft_dir"
  else
    draft_json="$(docker exec "$CONTAINER" "$PYTHON" "$TOOL" draft --target-id "$TARGET_ID" "${draft_extra[@]}")"
    echo "$draft_json"
    draft_dir="$(printf '%s\n' "$draft_json" | json_field "['draft_dir']")"
    target_name="$(printf '%s\n' "$draft_json" | json_field "['target']['name']")"
  fi
  FLOW_DRAFT="$draft_dir"
  target_encoded="$(docker exec "$CONTAINER" "$PYTHON" -c 'from urllib.parse import quote; import sys; print(quote(sys.argv[1], safe=""))' "$target_name")"

  FLOW_STAGE="text scan"
  docker exec "$CONTAINER" "$PYTHON" "$TOOL" scan-text --draft-dir "$draft_dir" --write

  if [[ "$skip_ocr" -eq 0 ]]; then
    FLOW_STAGE="OCR"
    ensure_ocr_service
    ocr_url="${ocr_url_arg:-$(container_ocr_url)}"
    docker exec \
      -e "RAG_FEEDER_OCR_SERVICE_URL=$ocr_url" \
      "$CONTAINER" "$PYTHON" "$TOOL" ocr --draft-dir "$draft_dir" --keep-going
  fi

  FLOW_STAGE="import validation"
  apply_draft "$draft_dir" --require-text-ready
  if [[ "$apply_yes" -ne 1 ]]; then
    echo "Dry run only. Rerun with --draft-dir '$draft_dir' --yes to apply, markdown, and embed." >&2
    return
  fi
  if [[ "$skip_apply" -eq 0 && "$apply_yes" -eq 1 ]]; then
    FLOW_STAGE="import"
    apply_draft "$draft_dir" --require-text-ready --yes
  else
    apply_draft "$draft_dir" --require-text-ready --require-applied
  fi

  if [[ "$skip_markdown" -eq 0 ]]; then
    FLOW_STAGE="markdown"
    echo "Running Kantropos markdown generation for $target_name..."
    call_updater_post "/markdowns/$target_encoded"
    echo
    echo "Kantropos markdown generation finished for $target_name."
  fi
  if [[ "$skip_embed" -eq 0 ]]; then
    FLOW_STAGE="markdown coverage verification"
    docker exec "$CONTAINER" "$PYTHON" "$TOOL" check-markdown --draft-dir "$draft_dir"
    FLOW_STAGE="embedding"
    echo "Requesting Kantropos incremental embedding for $target_name; waiting for HTTP result..."
    call_updater_post "/embeddings/$target_encoded?sync_mode=INSERT"
    echo
    echo "Kantropos embedding request succeeded for $target_name. Check corpus-updater logs for processing details."
  fi
}

cmd="${1:-}"
if [[ -z "$cmd" || "$cmd" == "-h" || "$cmd" == "--help" ]]; then
  usage
  exit 0
fi
shift || true

if [[ "$cmd" == rag-flow || "$cmd" == apply || "$cmd" == ocr ]]; then
  exec 9>"/tmp/dt-kantropos-upstream-${TARGET_ID}.lock"
  flock -n 9 || { echo "Another upstream operation for $TARGET_ID is already running." >&2; exit 1; }
fi

case "$cmd" in
  count|draft|scan-text|commands)
    docker exec "$CONTAINER" "$PYTHON" "$TOOL" "$cmd" --target-id "$TARGET_ID" "$@"
    ;;
  ocr)
    draft_dir="${1:-}"
    if [[ -z "$draft_dir" ]]; then
      echo "Missing draft_dir." >&2
      usage >&2
      exit 2
    fi
    shift || true
    ensure_ocr_service
    ocr_url="$(container_ocr_url)"
    docker exec -e "RAG_FEEDER_OCR_SERVICE_URL=$ocr_url" "$CONTAINER" "$PYTHON" "$TOOL" ocr --draft-dir "$draft_dir" "$@"
    ;;
  rag-flow)
    run_rag_flow "$@"
    ;;
  apply)
    draft_dir="${1:-}"
    if [[ -z "$draft_dir" ]]; then
      echo "Missing draft_dir." >&2
      usage >&2
      exit 2
    fi
    shift || true
    apply_draft "$draft_dir" "$@"
    ;;
  validate)
    draft_dir="${1:-}"
    if [[ -z "$draft_dir" ]]; then
      echo "Missing draft_dir." >&2
      usage >&2
      exit 2
    fi
    shift || true
    docker exec "$CONTAINER" "$PYTHON" "$TOOL" validate --draft-dir "$draft_dir" "$@"
    ;;
  *)
    docker exec "$CONTAINER" "$PYTHON" "$TOOL" "$cmd" "$@"
    ;;
esac
