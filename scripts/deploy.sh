#!/usr/bin/env bash
# Deploy this checkout: pull master, rebuild/restart only what changed, verify.
#
#   scripts/deploy.sh [--dry-run] [--no-pull] [--frontend] [--workers] [--full]
#                     [--baseline REV] [--drain-seconds N] [--url URL]
#
#   --dry-run        show what would happen (pull, build, restart) and run the checks
#   --no-pull        deploy the checkout as it is (e.g. after a manual pull)
#   --frontend       rebuild the frontend image even if nothing under frontend/ changed
#   --workers        restart the worker containers even if nothing of theirs changed
#   --full           `docker compose up -d --build` of every service (needed when
#                    dependencies, Dockerfiles or compose files changed)
#   --baseline REV   the revision known to be running (seeds .deploy-state on a
#                    host that has none; must be an ancestor of HEAD)
#   --drain-seconds  how long to wait for the workers to finish their current
#                    batch before restarting them (default 120, 0 = no wait)
#   --url URL        public URL to verify against (default: RAG_FEEDER_FRONTEND_URL in .env)
#
# Actions are decided from `git diff <last verified deploy> <new HEAD>`:
#   frontend/**                       -> rebuild + recreate rag_feeder, only on a host
#                                        that runs the production image (COMPOSE_FILE
#                                        includes docker-compose.prod.yml); the dev
#                                        stack serves the bind mount and needs nothing
#   backend/scripts/**, dl_lit_project/dl_lit/**
#                                     -> restart rag_enrich_worker + rag_download_worker
#                                        (the daemons read their code at start)
#   backend/src/**                    -> nothing; nodemon reloads it on the bind mount
#   package*.json, Dockerfiles, nodemon.json, docker-compose*.yml, Python requirements
#                                     -> requires --full; without it the script stops
#                                        with exit 2 and does not advance .deploy-state
# .deploy-state (git-ignored) holds the last revision that was deployed AND verified.
# It advances only at the end of a successful run, so a run that failed half-way is
# completed by the next one. Without a trustworthy state the script deploys
# everything (frontend + workers) unless --baseline says what is running.
set -euo pipefail
cd "$(dirname "$0")/.."

DRY_RUN=0 NO_PULL=0 FORCE_FRONTEND=0 FORCE_WORKERS=0 FULL=0 BASELINE="" DRAIN_SECONDS=120 URL=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --no-pull) NO_PULL=1 ;;
    --frontend) FORCE_FRONTEND=1 ;;
    --workers) FORCE_WORKERS=1 ;;
    --full) FULL=1 ;;
    --baseline) BASELINE="${2:-}"; shift ;;
    --drain-seconds) DRAIN_SECONDS="${2:-}"; shift ;;
    --url) URL="${2:-}"; shift ;;
    -h|--help) sed -n '2,32p' "$0"; exit 0 ;;
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
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if [ -z "$URL" ] && [ -f .env ]; then
  URL=$(grep -E '^RAG_FEEDER_FRONTEND_URL=' .env | tail -1 | cut -d= -f2- | tr -d '"'"'" || true)
fi
URL=${URL:-http://127.0.0.1:5175}
URL=${URL%/}

# A broken compose configuration must abort, not pass as "dev stack".
IMAGES=$(compose config --images) || { echo "docker compose config failed; fix the compose files/.env first" >&2; exit 1; }
PROD_IMAGE=0
if printf '%s\n' "$IMAGES" | grep -c '^rag_feeder_frontend:prod$' >/dev/null; then PROD_IMAGE=1; fi

STATE_FILE=.deploy-state
WORKER_SERVICES="rag_enrich_worker rag_download_worker"
WORKER_CONTAINERS="rag_feeder_enrich_worker rag_feeder_download_worker"

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
  say "deploying checkout as is: $(git log --oneline -1)"
else
  git fetch -q origin master
  REMOTE=$(git rev-parse origin/master)
  if [ "$REMOTE" = "$HEAD_BEFORE" ]; then
    say "already at origin/master: $(git log --oneline -1)"
  elif git merge-base --is-ancestor "$REMOTE" "$HEAD_BEFORE"; then
    echo "refusing to deploy: this checkout has commits that are not on origin/master (${HEAD_BEFORE:0:7} is ahead of ${REMOTE:0:7}); use --no-pull to deploy it anyway" >&2; exit 1
  else
    say "pull: ${HEAD_BEFORE:0:7} -> ${REMOTE:0:7} ($(git rev-list --count "$HEAD_BEFORE..$REMOTE") commits)"
    git log --oneline "$HEAD_BEFORE..$REMOTE" | sed 's/^/    /'
    run git merge --ff-only -q origin/master
  fi
fi
# What is checked out now (in a dry run the planned revision).
if [ "$DRY_RUN" = 1 ] && [ "$NO_PULL" = 0 ]; then AFTER=$(git rev-parse origin/master); else AFTER=$(git rev-parse HEAD); fi

# ---- 2. baseline: the revision known to be running -------------------------------
BEFORE="" TRUSTED=1
if [ -n "$BASELINE" ]; then
  BASELINE_SHA=$(git rev-parse --verify "$BASELINE^{commit}" 2>/dev/null) || { echo "--baseline $BASELINE is not a commit" >&2; exit 1; }
  git merge-base --is-ancestor "$BASELINE_SHA" "$AFTER" || { echo "--baseline ${BASELINE_SHA:0:7} is not an ancestor of ${AFTER:0:7}" >&2; exit 1; }
  BEFORE=$BASELINE_SHA
  say "baseline: ${BEFORE:0:7} (given)"
elif [ -s "$STATE_FILE" ]; then
  DEPLOYED=$(tr -d '[:space:]' < "$STATE_FILE")
  if git merge-base --is-ancestor "$DEPLOYED" "$AFTER" 2>/dev/null; then
    BEFORE=$DEPLOYED
    if [ "$BEFORE" != "$HEAD_BEFORE" ]; then say "last verified deploy: ${BEFORE:0:7} (an earlier run did not finish); deciding from there"; fi
  else
    say "WARNING: $STATE_FILE names ${DEPLOYED:0:7}, which is not in this history"
    TRUSTED=0
  fi
else
  say "no $STATE_FILE on this host"
  TRUSTED=0
fi
if [ "$TRUSTED" = 0 ]; then
  say "  -> deploying everything (frontend + workers); pass --baseline REV if you know what is running"
  BEFORE=$AFTER
  FORCE_FRONTEND=1
  FORCE_WORKERS=1
fi

# ---- 3. decide from the diff ---------------------------------------------------
CHANGED=$(git diff --name-only "$BEFORE" "$AFTER")
# grep -c consumes the whole input: a -q that exits early would SIGPIPE printf
# under pipefail and read as "unchanged".
touches() { [ "$(printf '%s\n' "$CHANGED" | grep -Ec "$1")" != 0 ]; }
RE_FULL='^(backend/(package.*\.json|Dockerfile|nodemon\.json)|dl_lit_project/(pyproject\.toml|setup\.(py|cfg)|requirements[^/]*)|docker-compose[^/]*\.yml|frontend/(Dockerfile|package.*\.json))$'
FRONTEND=0 WORKERS=0 NEEDS_FULL=""
if [ "$FORCE_FRONTEND" = 1 ] || touches '^frontend/'; then FRONTEND=1; fi
if [ "$FORCE_WORKERS" = 1 ] || touches '^(backend/scripts/|dl_lit_project/dl_lit/)'; then WORKERS=1; fi
if touches "$RE_FULL"; then NEEDS_FULL=$(printf '%s\n' "$CHANGED" | grep -E "$RE_FULL" | tr '\n' ' '); fi
if [ -n "$CHANGED" ]; then say "changed since ${BEFORE:0:7}: $(printf '%s\n' "$CHANGED" | wc -l) files"; fi

if [ -n "$NEEDS_FULL" ] && [ "$FULL" = 0 ]; then
  say "STOP: these changes need every image rebuilt: $NEEDS_FULL"
  say "      re-run with --full (docker compose up -d --build); .deploy-state is left at ${BEFORE:0:7}"
  exit 2
fi

# ---- 4. frontend / full ------------------------------------------------------------
if [ "$FULL" = 1 ]; then
  say "full: rebuilding and recreating every service"
  run compose build
  run compose up -d
  FRONTEND=$PROD_IMAGE   # verified below like a frontend deploy
  WORKERS=1
elif [ "$FRONTEND" = 1 ]; then
  if [ "$PROD_IMAGE" = 1 ]; then
    say "frontend: rebuilding the production image"
    run compose build rag_feeder
    run compose up -d --no-deps rag_feeder
  else
    say "frontend: dev server on the bind mount, nothing to rebuild"
    FRONTEND=0
  fi
else
  say "frontend: unchanged"
fi

# ---- 5. workers ------------------------------------------------------------------
worker_states() { # prints "<daemon> <status>" per worker from the heartbeat table
  compose exec -T rag_enrich_worker /opt/venv/bin/python - <<'PY' 2>/dev/null || true
import sqlite3
c = sqlite3.connect("/usr/src/app/dl_lit_project/data/literature.db")
for name, status in c.execute("SELECT daemon_name, status FROM daemon_heartbeat WHERE daemon_name IN ('enrich','download')"):
    print(name, status)
PY
}
if [ "$WORKERS" = 1 ] && [ "$FULL" = 0 ]; then
  if [ "$DRY_RUN" = 0 ] && [ "${DRAIN_SECONDS:-0}" -gt 0 ]; then
    waited=0 busy=""
    while [ "$waited" -lt "$DRAIN_SECONDS" ]; do
      busy=$(worker_states | awk '$2 == "running" { print $1 }' | tr '\n' ' ')
      if [ -z "$busy" ]; then break; fi
      if [ "$waited" = 0 ]; then say "workers: waiting up to ${DRAIN_SECONDS}s for the current batch to finish (busy: $busy)"; fi
      sleep 5; waited=$((waited + 5))
    done
    if [ -n "$busy" ]; then say "workers: still busy after ${DRAIN_SECONDS}s; restarting anyway (the job is released back to the queue)"; fi
  fi
  say "workers: restarting $WORKER_SERVICES"
  # shellcheck disable=SC2086
  run compose restart $WORKER_SERVICES
elif [ "$WORKERS" = 0 ]; then
  say "workers: unchanged"
fi
say "backend: nodemon reloads backend/src on its own"

# ---- 6. verify -------------------------------------------------------------------
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
# Containers take a moment after (re)create; poll the first check up to 60 s.
wait_http() { # expected path
  local tries=0 got
  while :; do
    got=$(code "$2")
    if [ "$got" = "$1" ]; then echo "$got"; return; fi
    tries=$((tries + 1))
    if [ "$tries" -ge 12 ]; then echo "$got"; return; fi
    sleep 5
  done
}
if [ "$DRY_RUN" = 1 ] && [ "$FRONTEND$WORKERS" != "00" ]; then say "  (dry run: checks reflect the current containers, not the planned changes)"; fi
check "GET /"                    200 "$(wait_http 200 /)"
check "GET /api/openalex/quota"  401 "$(code /api/openalex/quota)"
check "WebSocket /api/ws"        101 "$(ws_code)"
if [ "$PROD_IMAGE" = 1 ]; then
  check "GET /src/App.svelte"    404 "$(code /src/App.svelte)"
  check "GET /.env"              404 "$(code /.env)"
  cc=$(curl -s -D - -o /dev/null --max-time 15 "$URL/" | tr -d '\r' | awk -F': ' 'tolower($1)=="cache-control"{print $2}') || true
  check "Cache-Control on /"     "no-cache" "${cc:-none}"
fi
for c in rag_feeder_backend rag_feeder_frontend $WORKER_CONTAINERS; do
  check "container $c" running "$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo missing)"
done
# Identity: what was (re)built or restarted must actually be the thing running.
started_after() { # container -> "fresh" if started after this script began
  local at; at=$(docker inspect -f '{{.State.StartedAt}}' "$1" 2>/dev/null || echo "")
  if [ -n "$at" ] && [ "$(date -u -d "$at" +%s 2>/dev/null || echo 0)" -ge "$(date -u -d "$STARTED_AT" +%s)" ]; then echo fresh; else echo stale; fi
}
if [ "$DRY_RUN" = 0 ]; then
  if [ "$FRONTEND" = 1 ] && [ "$PROD_IMAGE" = 1 ]; then
    want=$(docker image inspect -f '{{.Id}}' rag_feeder_frontend:prod 2>/dev/null || echo none)
    have=$(docker inspect -f '{{.Image}}' rag_feeder_frontend 2>/dev/null || echo missing)
    check "frontend container runs the freshly built image" "$want" "$have"
    check "frontend container restarted" fresh "$(started_after rag_feeder_frontend)"
  fi
  if [ "$WORKERS" = 1 ]; then
    for c in $WORKER_CONTAINERS; do
      check "$c restarted" fresh "$(started_after "$c")"
      lines=$(docker logs --since "$STARTED_AT" "$c" 2>&1 | grep -c 'daemon starting' || true)
      check "$c daemon started" started "$([ "${lines:-0}" -gt 0 ] && echo started || echo no-start-line)"
    done
    # Nothing left behind in 'running' from before the restart.
    stranded=$(compose exec -T rag_enrich_worker /opt/venv/bin/python - <<'PY' 2>/dev/null || echo "?"
import sqlite3
c = sqlite3.connect("/usr/src/app/dl_lit_project/data/literature.db")
live = {r[0] for r in c.execute("SELECT worker_id FROM daemon_heartbeat WHERE worker_id IS NOT NULL")}
rows = c.execute("SELECT worker_id FROM pipeline_jobs WHERE status = 'running'").fetchall()
print(sum(1 for (w,) in rows if w not in live))
PY
)
    check "pipeline jobs stranded in 'running'" 0 "$stranded"
  fi
fi
if [ "$fail" = 1 ]; then say "DEPLOY CHECKS FAILED (state stays at ${BEFORE:0:7}; the next run redoes the pending steps)"; exit 1; fi
if [ "$DRY_RUN" = 0 ]; then printf '%s\n' "$AFTER" > "$STATE_FILE"; fi
say "deploy OK at $(git log --oneline -1 "$AFTER")"
