#!/usr/bin/env bash
# Deploy this checkout: pull master, rebuild/restart only what changed, verify.
#
#   scripts/deploy.sh [--dry-run] [--no-pull] [--frontend] [--workers] [--url URL]
#
#   --dry-run   show what would happen (pull, build, restart) and run the checks
#   --no-pull   deploy the checkout as it is (e.g. after a manual pull)
#   --frontend  rebuild the frontend image even if nothing under frontend/ changed
#   --workers   restart the worker containers even if nothing of theirs changed
#   --url URL   public URL to verify against (default: RAG_FEEDER_FRONTEND_URL in .env)
#
# What it decides from the pulled diff:
#   frontend/**                       -> rebuild + recreate rag_feeder, only on a host
#                                        that runs the production image (COMPOSE_FILE
#                                        includes docker-compose.prod.yml); the dev
#                                        stack serves the bind mount and needs nothing
#   backend/scripts/**, dl_lit_project/dl_lit/**
#                                     -> restart rag_enrich_worker + rag_download_worker
#                                        (the daemons read their code at start)
#   backend/src/**                    -> nothing; nodemon reloads it on the bind mount
#   backend/package*.json, frontend/package*.json, Dockerfiles, docker-compose*.yml
#                                     -> warning: needs a manual `docker compose up -d --build`
set -euo pipefail
cd "$(dirname "$0")/.."

DRY_RUN=0 NO_PULL=0 FORCE_FRONTEND=0 FORCE_WORKERS=0 URL=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --no-pull) NO_PULL=1 ;;
    --frontend) FORCE_FRONTEND=1 ;;
    --workers) FORCE_WORKERS=1 ;;
    --url) URL="${2:-}"; shift ;;
    -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
  shift
done

say() { printf '%s\n' "$*"; }
run() {
  if [ "$DRY_RUN" = 1 ]; then say "  [dry-run] $*"; else say "  + $*"; "$@"; fi
}
# Compose prints an interpolation warning per unset .env variable; keep real output.
compose() { docker compose "$@" 2> >(grep -v 'variable is not set' >&2 || true); }

if [ -z "$URL" ] && [ -f .env ]; then
  URL=$(grep -E '^RAG_FEEDER_FRONTEND_URL=' .env | tail -1 | cut -d= -f2- | tr -d '"'"'" || true)
fi
URL=${URL:-http://127.0.0.1:5175}
URL=${URL%/}

# A broken compose configuration must abort, not pass as "dev stack".
IMAGES=$(compose config --images) || { echo "docker compose config failed; fix the compose files/.env first" >&2; exit 1; }
PROD_IMAGE=0
if printf '%s\n' "$IMAGES" | grep -q '^rag_feeder_frontend:prod$'; then PROD_IMAGE=1; fi

# The last revision that was deployed AND verified. Actions are decided against
# it, not against the pre-pull HEAD, so a failed build or restart is redone on
# the next run (and --no-pull after a manual pull still does the right thing).
STATE_FILE=.deploy-state

# ---- 1. pull -------------------------------------------------------------------
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$BRANCH" != "master" ]; then
  echo "refusing to deploy branch '$BRANCH' (checkout master first)" >&2; exit 1
fi
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "refusing to deploy: tracked files are modified in the checkout" >&2; git status --short --untracked-files=no >&2; exit 1
fi
HEAD_BEFORE=$(git rev-parse HEAD)
if [ "$NO_PULL" = 1 ]; then
  AFTER=$HEAD_BEFORE
  say "deploying checkout as is: $(git log --oneline -1)"
else
  git fetch -q origin master
  AFTER=$(git rev-parse origin/master)
  if [ "$AFTER" = "$HEAD_BEFORE" ]; then
    say "already at origin/master: $(git log --oneline -1)"
  else
    say "pull: ${HEAD_BEFORE:0:7} -> ${AFTER:0:7} ($(git rev-list --count "$HEAD_BEFORE..$AFTER") commits)"
    git log --oneline "$HEAD_BEFORE..$AFTER" | sed 's/^/    /'
    run git merge --ff-only -q origin/master
  fi
fi
BEFORE=""
if [ -s "$STATE_FILE" ]; then
  DEPLOYED=$(tr -d '[:space:]' < "$STATE_FILE")
  if git merge-base --is-ancestor "$DEPLOYED" "$AFTER" 2>/dev/null; then
    BEFORE=$DEPLOYED
    [ "$BEFORE" != "$HEAD_BEFORE" ] && say "last verified deploy: ${BEFORE:0:7} (an earlier run did not finish); deciding from there"
  else
    say "WARNING: $STATE_FILE names ${DEPLOYED:0:7}, which is not in this history; deciding from the pre-pull HEAD"
  fi
fi
if [ -z "$BEFORE" ]; then
  BEFORE=$HEAD_BEFORE
  [ -s "$STATE_FILE" ] || say "no $STATE_FILE yet: deciding from the pre-pull HEAD (use --frontend/--workers to force)"
fi

# ---- 2. decide from the diff ---------------------------------------------------
CHANGED=$(git diff --name-only "$BEFORE" "$AFTER")
touches() { printf '%s\n' "$CHANGED" | grep -Eq "$1"; }
FRONTEND=0 WORKERS=0 MANUAL=""
if [ "$FORCE_FRONTEND" = 1 ] || touches '^frontend/'; then FRONTEND=1; fi
if [ "$FORCE_WORKERS" = 1 ] || touches '^(backend/scripts/|dl_lit_project/dl_lit/)'; then WORKERS=1; fi
if touches '^(backend/package.*\.json|backend/Dockerfile|dl_lit_project/(pyproject\.toml|requirements.*)|docker-compose.*\.yml|frontend/Dockerfile|frontend/package.*\.json)$'; then
  MANUAL=$(printf '%s\n' "$CHANGED" | grep -E '^(backend/package.*\.json|backend/Dockerfile|dl_lit_project/(pyproject\.toml|requirements.*)|docker-compose.*\.yml|frontend/Dockerfile|frontend/package.*\.json)$' | tr '\n' ' ')
fi

if [ -n "$CHANGED" ]; then
  say "changed: $(printf '%s\n' "$CHANGED" | wc -l) files"
fi

# ---- 3. frontend -----------------------------------------------------------------
if [ "$FRONTEND" = 1 ]; then
  if [ "$PROD_IMAGE" = 1 ]; then
    say "frontend: rebuilding the production image"
    run compose build rag_feeder
    run compose up -d rag_feeder
  else
    say "frontend: dev server on the bind mount, nothing to rebuild"
  fi
else
  say "frontend: unchanged"
fi

# ---- 4. workers ------------------------------------------------------------------
if [ "$WORKERS" = 1 ]; then
  say "workers: restarting rag_enrich_worker rag_download_worker"
  run compose restart rag_enrich_worker rag_download_worker
else
  say "workers: unchanged"
fi
say "backend: nodemon reloads backend/src on its own"
if [ -n "$MANUAL" ]; then
  say "WARNING: dependency/compose/Dockerfile changes need a manual 'docker compose up -d --build': $MANUAL"
fi

# ---- 5. verify -------------------------------------------------------------------
say "verify against $URL"
fail=0
check() { # label expected actual
  if [ "$2" = "$3" ]; then say "  ok   $1 -> $3"; else say "  FAIL $1 -> $3 (expected $2)"; fail=1; fi
}
# curl's exit status is ignored: it prints the status code it got, and after a
# successful WebSocket upgrade it only returns once --max-time expires.
code() { local out; out=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 "$URL$1") || true; echo "${out:-000}"; }
ws_code() {
  # HTTP/1.1 on purpose: over h2 the Upgrade header is dropped and the probe 404s.
  local out
  out=$(curl -s --http1.1 -o /dev/null -w '%{http_code}' --max-time 5 \
    -H 'Connection: Upgrade' -H 'Upgrade: websocket' -H 'Sec-WebSocket-Version: 13' \
    -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' "$URL/api/ws") || true
  echo "${out:-000}"
}
[ "$DRY_RUN" = 1 ] && [ "$FRONTEND$WORKERS" != "00" ] && say "  (dry run: checks reflect the current containers, not the planned changes)"
sleep 2
check "GET /"                    200 "$(code /)"
check "GET /api/openalex/quota"  401 "$(code /api/openalex/quota)"
check "WebSocket /api/ws"        101 "$(ws_code)"
if [ "$PROD_IMAGE" = 1 ]; then
  check "GET /src/App.svelte"    404 "$(code /src/App.svelte)"
  check "GET /.env"              404 "$(code /.env)"
  cc=$(curl -s -D - -o /dev/null --max-time 15 "$URL/" | tr -d '\r' | awk -F': ' 'tolower($1)=="cache-control"{print $2}')
  check "Cache-Control on /"     "no-cache" "$cc"
fi
for c in rag_feeder_backend rag_feeder_frontend rag_feeder_enrich_worker rag_feeder_download_worker; do
  check "container $c" running "$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo missing)"
done
if [ "$fail" = 1 ]; then say "DEPLOY CHECKS FAILED (state not advanced; the next run redoes the pending steps)"; exit 1; fi
if [ "$DRY_RUN" = 0 ]; then printf '%s\n' "$AFTER" > "$STATE_FILE"; fi
say "deploy OK at $(git log --oneline -1)"
