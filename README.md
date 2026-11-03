# AMPS Datadog check

Copy-in custom check targeting Datadog Agent 7.85, its Python 3.13 runtime,
and the AMPS 5.3.5 monitoring interface.

## What end users need

This is a **custom check loaded by the Datadog Agent**, not a standalone Python
application or a package to install with pip. Deploy only these two files:

| Repository file | Purpose |
| --- | --- |
| `amps.py` | Check code, loaded by the Agent |
| `amps.yaml` | Example configuration; edit it for your AMPS instances |

The Agent supplies Python and the runtime dependencies. End users do **not**
need to install Python, uv, pytest, an AMPS client SDK, or the development
dependencies in `pyproject.toml` / `uv.lock`.

## Installation on a Linux Agent host

### 1. Prepare the services

- Install and configure the [Datadog Agent](https://docs.datadoghq.com/agent/basic_agent_usage/)
  with your Datadog API key and site. This check targets Agent 7.85 with its
  embedded Python 3.13 runtime.
- Have a running AMPS server with the Admin monitoring interface enabled. The
  check targets the AMPS 5.3.5 monitoring API.
- Ensure the Agent host can reach the AMPS Admin endpoint, including any
  required authentication. The Agent may run on the AMPS host or a separate host.

For example, include this block in your AMPS server configuration and apply the
configuration using your normal AMPS restart procedure:

```xml
<Admin>
  <InetAddr>localhost:8085</InetAddr>
  <Interval>10s</Interval>
</Admin>
```

This example listens locally and is suitable when the Agent and AMPS share a
host. For a separate Agent host, configure an appropriately accessible Admin
address. Use the **Admin port**, not an AMPS messaging transport port. See
[AMPS monitoring configuration](https://crankuptheamps.com/docs/amps-user-guide/monitoring/monitor-configuration)
for authentication, HTTPS, and statistics storage options.

### 2. Copy the check and configuration

Download this repository and extract it, or clone it:

```sh
git clone https://github.com/60East/amps-integration-datadog.git
cd amps-integration-datadog
```

From the repository directory, on the Agent host:

```sh
sudo install -d /etc/datadog-agent/checks.d /etc/datadog-agent/conf.d/amps.d
sudo install -m 755 amps.py /etc/datadog-agent/checks.d/amps.py
sudo install -m 644 amps.yaml /etc/datadog-agent/conf.d/amps.d/conf.yaml
```

The configuration copy is for a **new installation**. When updating the check,
replace `amps.py` and retain your configured `conf.yaml`.

### 3. Configure your instances

Edit `/etc/datadog-agent/conf.d/amps.d/conf.yaml`, for example:

```yaml
init_config: {}
instances:
  - name: production-amps
    url: http://localhost:8085/amps/instance.json
    timeout: 5
    min_collection_interval: 15
    collect_client_metrics: false
    tags:
      - env:production
```

Add another entry under `instances` for each server. Give each entry a unique,
stable `name`. The full `url` must return the AMPS instance JSON document;
`localhost` refers to the **Agent's host**.

Use the Agent's standard `username`, `password`, proxy and TLS settings for
protected endpoints. TLS verification is enabled by default. The request
`timeout` defaults to five seconds. `tags` applies to every observation, along
with `amps_instance:<name>`. Set `collect_client_metrics: true` to enable
per-client series; total client counts are collected independently. Per-client
collection increases the number of custom metric series.

### 4. Verify and enable collection

First verify the configuration:

```sh
sudo -u dd-agent datadog-agent check amps
```

The check output should show `amps.admin.check` with status **OK** and metrics
such as `amps.memory.rss`. If verification fails, check the endpoint URL,
connectivity, credentials, TLS settings, and file readability by `dd-agent`.
Resolve the failure before enabling collection. Then restart the Agent:

```sh
sudo systemctl restart datadog-agent
sudo datadog-agent status
```

Confirm that `amps` appears in the Agent status and that `amps.*` metrics arrive
in Datadog. Cumulative counters need successive collection samples before
producing deltas. Check that `amps.admin.check` recovers to OK after an outage.

These commands use standard Linux host-Agent paths and systemd. Containerized
Agents need the check and configuration mounted into the corresponding Agent
directories and a URL reachable from inside the container.

## Metrics

Coverage includes clients, subscriptions, memory, processors, queued queries,
API command depth, SOW topics, message queues and views. Snapshots and existing
per-second rates are gauges; cumulative denials, query time/count, expired
leases, transfers and processor throttling use monotonic counts. Initial
counter samples establish a baseline. Values retain AMPS-native units:
processor `last_active` is milliseconds, HTTP response time and queue ages are
seconds. Missing optional fields are omitted. Malformed values produce WARNING;
HTTP, network and payload-envelope errors produce CRITICAL.

See the [AMPS monitoring reference](https://crankuptheamps.com/docs/amps-monitoring-guide)
for field definitions and units and the
[Datadog HTTP options](https://datadoghq.dev/integrations-core/base/http/) for configuration.

## Development

Install uv, then run:

```sh
uv sync --locked
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

The development environment uses Python 3.13; the production Agent provides its
own dependencies. Tests use synthetic data, not a running AMPS server. Before
deployment, validate against a real AMPS instance with representative clients,
topics, queues and views, and compare reported metrics with its Admin JSON
endpoint.

The container smoke test is `bash tests/smoke.sh` (requires Docker daemon
access). It mounts the same copy-in check in Agent 7.85 and serves the synthetic
fixture on an isolated Docker network. CI runs this alongside the unit tests.
