#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
PROD="-f docker-compose.yml -f docker-compose.prod.yml"
docker compose $PROD build rag_feeder
NET=dt_rag-network
docker network inspect "$NET" >/dev/null
IMAGE=$(docker compose $PROD config --images | grep -m1 'rag_feeder')
CID=$(docker run -d --rm --network "$NET" -p 127.0.0.1:5176:5175 "$IMAGE")
trap 'docker stop "$CID" >/dev/null' EXIT
sleep 2
code() { curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:5176$1"; }
[ "$(code /)" = 200 ]
curl -s http://127.0.0.1:5176/ | grep -q '<div id="app"'
[ "$(code /api/openalex/quota)" = 401 ]          # proxied to the backend, which wants auth
for p in /src/App.svelte /Dockerfile /.env /Caddyfile /package.json; do
  [ "$(code "$p")" = 404 ] || { echo "not 404: $p" >&2; exit 1; }
done
curl -s -D - -o /dev/null http://127.0.0.1:5176/assets/ | head -1
echo "prod image OK"
