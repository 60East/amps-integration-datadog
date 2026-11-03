"""AMPS monitoring check for Datadog Agent 7 and Python 3.13."""

import math
import time
from urllib.parse import urlsplit

from datadog_checks.base import AgentCheck, ConfigurationError
from requests.exceptions import RequestException

__version__ = "2.0.0"

GROUPS = {
    "client": [
        "bytes_in_per_sec",
        "bytes_out_per_sec",
        "denied_writes",
        "denied_reads",
        "messages_in_per_sec",
        "messages_out_per_sec",
        "query_time",
        "queue_depth_out",
        "queue_max_latency",
        "subscription_count",
        "transport_rx_queue",
        "transport_tx_queue",
    ],
    "sow": [
        "valid_keys",
        "deletes_per_sec",
        "inserts_per_sec",
        "updates_per_sec",
        "queries_per_sec",
        "query_count",
        "stored_bytes",
        "memory_bytes",
    ],
    "queue": [
        "age_of_oldest_lease",
        "backlog",
        "expired_leases",
        "owned",
        "queue_depth",
        "seconds_behind",
        "transferred_in",
        "transferred_out",
    ],
    "view": ["conflation_ratio", "queue_depth"],
}
COUNTERS = {
    "denied_reads",
    "denied_writes",
    "query_time",
    "query_count",
    "expired_leases",
    "transferred_in",
    "transferred_out",
    "throttle_count",
}


class AMPSCheck(AgentCheck):
    def check(self, instance: dict) -> None:
        name = instance.get("name")
        url = instance.get("url", "")
        if not isinstance(name, str) or not name.strip():
            raise ConfigurationError("name must be a nonempty string")
        try:
            parsed = urlsplit(url)
            valid = isinstance(url, str) and parsed.scheme in {"http", "https"} and parsed.hostname and parsed.port != 0
        except (ValueError, TypeError, AttributeError):
            valid = False
        if not valid or parsed.username or parsed.password or parsed.fragment:
            raise ConfigurationError("url must be an HTTP(S) endpoint without embedded credentials or a fragment")
        tags = instance.get("tags", [])
        if not isinstance(tags, list) or any(not isinstance(t, str) for t in tags):
            raise ConfigurationError("tags must be a list of strings")
        collect_clients = instance.get("collect_client_metrics", False)
        if not isinstance(collect_clients, bool):
            raise ConfigurationError("collect_client_metrics must be a boolean")
        try:
            timeout = float(instance.get("timeout", 5))
            if not math.isfinite(timeout) or timeout <= 0 or isinstance(instance.get("timeout"), bool):
                raise ValueError
        except (TypeError, ValueError, OverflowError):
            raise ConfigurationError("timeout must be a positive finite number") from None
        tags = [*tags, f"amps_instance:{name}"]
        start = time.monotonic()
        try:
            response = self.http.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()["amps"]["instance"]
            if not isinstance(data, dict):
                raise TypeError
        except (RequestException, ValueError, KeyError, TypeError) as exc:
            self.service_check(
                "amps.admin.check", self.CRITICAL, tags=tags, message=f"AMPS collection failed ({type(exc).__name__})"
            )
            return
        self.gauge("amps.admin.response_time", time.monotonic() - start, tags=tags)
        malformed = False

        def emit(source: dict, prefix: str, fields: list, metric_tags: list) -> None:
            nonlocal malformed
            for field in fields:
                if field not in source:
                    continue
                try:
                    value = source[field]
                    if isinstance(value, bool):
                        raise TypeError
                    value = float(value)
                    if not math.isfinite(value):
                        raise ValueError
                except (TypeError, ValueError, OverflowError):
                    malformed = True
                    continue
                submit = self.monotonic_count if field in COUNTERS else self.gauge
                metric_field = "queued" if prefix == "amps.queries" and field == "queued_queries" else field
                submit(f"{prefix}.{metric_field}", value, tags=metric_tags)

        for section, fields in {
            "memory": ["vmsize", "rss"],
            "queries": ["queued_queries"],
            "api": ["command_queue_depth"],
        }.items():
            source = data.get(section, {})
            if isinstance(source, dict):
                emit(source, f"amps.{section}", fields, tags)
            else:
                malformed = True
        for section in ("clients", "subscriptions", "processors", "sow", "queues", "views"):
            if section not in data:
                continue
            rows = data[section]
            if not isinstance(rows, list):
                malformed = True
                continue
            if section in {"clients", "subscriptions"}:
                self.gauge(f"amps.{section}.total", len(rows), tags=tags)
            if section == "subscriptions" or (section == "clients" and not collect_clients):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    malformed = True
                    continue
                if section == "processors":
                    if row.get("id") == "all":
                        emit(
                            row,
                            "amps.processing",
                            ["messages_received_per_sec", "matches_found_per_sec", "denied_reads", "denied_writes"],
                            tags,
                        )
                        emit(row, "amps.processors", ["throttle_count", "last_active"], tags)
                        break
                    continue
                group = {"clients": "client", "queues": "queue", "views": "view", "sow": "sow"}[section]
                identity = row.get("client_name" if group == "client" else "topic")
                if not isinstance(identity, str) or not identity:
                    malformed = True
                    continue
                key = "topic" if group == "sow" else group
                emit(row, f"amps.{group}", GROUPS[group], [*tags, f"{key}:{identity}"])
        self.service_check(
            "amps.admin.check",
            self.WARNING if malformed else self.OK,
            tags=tags,
            message="Some AMPS metric data was malformed" if malformed else "",
        )
