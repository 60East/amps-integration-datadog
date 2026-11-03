#!/usr/bin/env bash
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
network="amps-smoke-$$"
server="amps-fixture-$$"
agent="amps-agent-$$"
config=$(mktemp)
output=$(mktemp)
cleanup() {
  docker rm -f "$agent" >/dev/null 2>&1 || true
  docker rm -f "$server" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -f "$config" "$output"
}
trap cleanup EXIT
cat > "$config" <<YAML
init_config: {}
instances:
  - name: smoke
    url: http://$server:8085/instance.json
YAML
chmod 644 "$config"
docker network create "$network" >/dev/null
docker run -d --name "$server" --network "$network" \
  -v "$repo/tests/fixtures:/data:ro" python:3.13-slim \
  python -m http.server 8085 --directory /data >/dev/null
if ! docker run -d --name "$agent" --network "$network" \
  -e DD_API_KEY=00000000000000000000000000000000 \
  -e DD_HOSTNAME=amps-smoke \
  -v "$repo/amps.py:/etc/datadog-agent/checks.d/amps.py:ro" \
  -v "$config:/etc/datadog-agent/conf.d/amps.d/conf.yaml:ro" \
  registry.datadoghq.com/agent:7.85.0 >/dev/null; then
  docker logs "$agent" >&2 || true
  exit 1
fi
ready=false
for attempt in {1..30}; do
  if timeout 5 docker exec "$agent" agent status >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 2
done
if [ "$ready" != true ]; then
  echo "Datadog Agent did not become ready" >&2
  docker logs "$agent" >&2 || true
  exit 1
fi
docker exec "$agent" agent check amps --json > "$output"
python3 - "$output" <<'PY'
import json
import sys
from pathlib import Path
text = Path(sys.argv[1]).read_text()
assert 'amps.memory.rss' in text, text

def observations(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from observations(child)
    elif isinstance(value, list):
        for child in value:
            yield from observations(child)
assert any(row.get('check') == 'amps.admin.check' and row.get('status') == 0
           for row in observations(json.loads(text))), text
PY
