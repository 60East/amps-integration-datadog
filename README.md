# amps-integration-datadog
Datadog AgentCheck for AMPS.

# Installation

Here are the steps to install the AMPS AgentCheck for Datadog:

1. Install the datadog agent on your AMPS host.

2. Copy the files from this github repo to your host:

```
    amps.py   -> /etc/dd-agent/checks.d
    amps.yaml -> /etc/dd-agent/conf.d
```

3. Edit the amps.yaml configuration file in /etc/dd-agent/conf.d to contain a list of AMPS instances on the host and their Admin ports. Example:

```
init_config:
    slow_client_threshold: 1.0
    message_processor_activity_threshold: 20.0

instances:
    - name:  monolith
      admin: localhost:2345

    - name:  sample
      admin: localhost:8085
```

# Metric Descriptions

Coming Soon!
