#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${RAG_FEEDER_BACKEND_CONTAINER:-rag_feeder_backend}"
TARGET_ID="${RAG_FEEDER_UPSTREAM_TARGET_ID:-anthropozan-nachhaltiges-management}"
TOOL="/usr/src/app/scripts/upstream_update.py"
PYTHON="${RAG_FEEDER_BACKEND_PYTHON:-/opt/venv/bin/python}"
HOST_PYTHON="${RAG_FEEDER_HOST_PYTHON:-python3}"
OCR_HOME="${RAG_FEEDER_OCR_HOME:-/home/spott/rechtmaschine-debian-rag-ocr}"
OCR_HOST_URL="${RAG_FEEDER_OCR_HOST_URL:-http://127.0.0.1:8004}"
OCR_CONTAINER_URL="${RAG_FEEDER_OCR_SERVICE_URL:-}"
CORPUS_UPDATER_CONTAINER="${RAG_FEEDER_KANTROPOS_UPDATER_CONTAINER:-kantropos-corpus-updater}"

usage() {
  cat <<'EOF'
Usage:
  bash backend/scripts/kantropos_upstream.sh count
  bash backend/scripts/kantropos_upstream.sh draft [--limit N]
  bash backend/scripts/kantropos_upstream.sh scan-text [--draft-dir DIR]
  bash backend/scripts/kantropos_upstream.sh ocr <draft_dir> [--ocr-url URL]
  bash backend/scripts/kantropos_upstream.sh rag-flow [--yes] [--skip-ocr] [--skip-apply] [--skip-markdown] [--skip-embed]
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
  if [[ ! -x "$OCR_HOME/ocr/.venv_hpi/bin/python" ]]; then
    echo "Missing OCR venv at $OCR_HOME/ocr/.venv_hpi." >&2
    exit 1
  fi
  if [[ ! -f "$OCR_HOME/service_manager.py" ]]; then
    echo "Missing Rechtmaschine service_manager.py at $OCR_HOME." >&2
    exit 1
  fi
  mkdir -p "$OCR_HOME/logs"
  (
    cd "$OCR_HOME"
    env \
      SERVICE_MANAGER_ROLE=ocr \
      OCR_SERVICE_FILE=ocr_service_hibernate.py \
      setsid ocr/.venv_hpi/bin/python service_manager.py \
        > "$OCR_HOME/logs/service_manager.log" 2>&1 < /dev/null &
  )
  for _ in $(seq 1 60); do
    if curl -fsS "$OCR_HOST_URL/health" >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  echo "OCR service did not become healthy at $OCR_HOST_URL." >&2
  echo "Check $OCR_HOME/logs/service_manager.log" >&2
  exit 1
}

run_updater_post() {
  local path="$1"
  local log_name="$2"
  docker exec "$CORPUS_UPDATER_CONTAINER" sh -lc \
    "nohup curl -fsS -X POST 'http://localhost:8001${path}' > '/tmp/${log_name}.log' 2>&1 &"
}

call_updater_post() {
  local path="$1"
  docker exec "$CORPUS_UPDATER_CONTAINER" curl -fsS -X POST "http://localhost:8001${path}"
}

run_rag_flow() {
  local apply_yes=0
  local skip_ocr=0
  local skip_apply=0
  local skip_markdown=0
  local skip_embed=0
  local ocr_url_arg=""
  local draft_extra=()

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --yes) apply_yes=1 ;;
      --skip-ocr) skip_ocr=1 ;;
      --skip-apply) skip_apply=1 ;;
      --skip-markdown) skip_markdown=1 ;;
      --skip-embed) skip_embed=1 ;;
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
  draft_json="$(docker exec "$CONTAINER" "$PYTHON" "$TOOL" draft --target-id "$TARGET_ID" "${draft_extra[@]}")"
  echo "$draft_json"
  draft_dir="$(printf '%s\n' "$draft_json" | json_field "['draft_dir']")"
  target_name="$(printf '%s\n' "$draft_json" | json_field "['target']['name']")"
  target_encoded="$(docker exec "$CONTAINER" "$PYTHON" -c 'from urllib.parse import quote; import sys; print(quote(sys.argv[1], safe=""))' "$target_name")"

  docker exec "$CONTAINER" "$PYTHON" "$TOOL" scan-text --draft-dir "$draft_dir" --write

  if [[ "$skip_ocr" -eq 0 ]]; then
    ensure_ocr_service
    ocr_url="${ocr_url_arg:-$(container_ocr_url)}"
    docker exec \
      -e "RAG_FEEDER_OCR_SERVICE_URL=$ocr_url" \
      "$CONTAINER" "$PYTHON" "$TOOL" ocr --draft-dir "$draft_dir" --keep-going
  fi

  docker exec "$CONTAINER" "$PYTHON" "$TOOL" apply --draft-dir "$draft_dir"
  if [[ "$skip_apply" -eq 0 && "$apply_yes" -eq 1 ]]; then
    docker exec "$CONTAINER" "$PYTHON" "$TOOL" apply --draft-dir "$draft_dir" --yes
  elif [[ "$skip_apply" -eq 0 ]]; then
    echo "Dry run only. Rerun with --yes to apply, markdown, and embed." >&2
    return
  fi

  if [[ "$skip_markdown" -eq 0 ]]; then
    echo "Running Kantropos markdown generation for $target_name..."
    call_updater_post "/markdowns/$target_encoded"
    echo
    echo "Kantropos markdown generation finished for $target_name."
  fi
  if [[ "$skip_embed" -eq 0 ]]; then
    run_updater_post "/embeddings/$target_encoded?sync_mode=INSERT" "kantropos-embeddings-$TARGET_ID-$(date +%Y%m%d_%H%M%S)"
    echo "Started Kantropos incremental embedding for $target_name."
  fi
}

cmd="${1:-}"
if [[ -z "$cmd" || "$cmd" == "-h" || "$cmd" == "--help" ]]; then
  usage
  exit 0
fi
shift || true

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
    docker exec "$CONTAINER" "$PYTHON" "$TOOL" apply --draft-dir "$draft_dir" "$@"
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
