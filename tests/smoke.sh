#!/usr/bin/env bash
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
network="amps-smoke-$$"
server="amps-fixture-$$"
config=$(mktemp)
output=$(mktemp)
cleanup() {
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
docker run --rm --network "$network" \
  -v "$repo/amps.py:/etc/datadog-agent/checks.d/amps.py:ro" \
  -v "$config:/etc/datadog-agent/conf.d/amps.d/conf.yaml:ro" \
  --entrypoint agent registry.datadoghq.com/agent:7.85.0 check amps --json > "$output"
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
