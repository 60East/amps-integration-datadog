# amps-integration-datadog
Datadog AgentCheck for AMPS.

# Installation

Here are the steps to install the AMPS AgentCheck for Datadog:

1. Install the datadog agent on your AMPS host.

2. Copy the amps.py AgentCheck to /etc/dd-agent/checks.d

3. Create an amps.yaml configuration file in /etc/dd-agent/conf.d, containing a list of all instances and their admin ports. Example:

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
