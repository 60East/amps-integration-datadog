import copy
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from datadog_checks.base import ConfigurationError
from datadog_checks.base.stubs import aggregator
from requests.exceptions import HTTPError, Timeout

from amps import COUNTERS, GROUPS, AMPSCheck


@pytest.fixture
def check(monkeypatch):
    config = {"name": "test", "url": "http://localhost:8085/amps/instance.json"}
    c = AMPSCheck("amps", {}, [config])
    monkeypatch.setattr(type(c.http), "get", Mock())
    c.gauge = Mock()
    c.monotonic_count = Mock()
    c.service_check = Mock()
    return c, config


def payload(c, data):
    c.http.get.return_value.json.return_value = {"amps": {"instance": data}}


@pytest.mark.parametrize("group", ["client", "sow", "queue", "view"])
def test_registered_fields(check, group):
    c, config = check
    config["collect_client_metrics"] = True
    section = {"client": "clients", "queue": "queues", "view": "views", "sow": "sow"}[group]
    row = dict.fromkeys(GROUPS[group], 2.5)
    row["client_name" if group == "client" else "topic"] = "entity"
    payload(c, {section: [row]})
    c.check(config)
    gauges = {call.args[0]: call.args[1] for call in c.gauge.call_args_list}
    counters = {call.args[0]: call.args[1] for call in c.monotonic_count.call_args_list}
    for field in GROUPS[group]:
        assert (counters if field in COUNTERS else gauges)[f"amps.{group}.{field}"] == 2.5
    assert c.service_check.call_args.args[1] == c.OK


def test_opt_in(check):
    c, config = check
    payload(c, {"clients": [{"client_name": "c", "query_time": 10}]})
    c.check(config)
    c.monotonic_count.assert_not_called()
    assert len(c.gauge.call_args_list) == 2


@pytest.mark.parametrize("data", [{"memory": {"rss": "nan"}}, {"queues": None}, {"views": [None]}])
def test_partial(check, data):
    c, config = check
    payload(c, data)
    c.check(config)
    assert c.service_check.call_args.args[1] == c.WARNING


def test_failure_and_recovery(check):
    c, config = check
    c.http.get.side_effect = Timeout("secret")
    c.check(config)
    assert c.service_check.call_args.args[1] == c.CRITICAL
    assert "secret" not in str(c.service_check.call_args)
    c.http.get.side_effect = None
    payload(c, {"clients": []})
    c.check(config)
    assert c.service_check.call_args.args[1] == c.OK


@pytest.mark.parametrize(
    "key,value",
    [
        ("name", ""),
        ("url", "ftp://host"),
        ("url", 123),
        ("timeout", 0),
        ("tags", "bad"),
        ("collect_client_metrics", "false"),
    ],
)
def test_config(check, key, value):
    c, config = check
    config[key] = value
    with pytest.raises(ConfigurationError):
        c.check(config)
    c.http.get.assert_not_called()


def test_all_families(check):
    c, config = check
    c.http.get.return_value.json.return_value = json.loads(Path("tests/fixtures/instance.json").read_text())
    c.check(config)
    names = {call.args[0] for call in c.gauge.call_args_list}
    assert {
        "amps.memory.rss",
        "amps.api.command_queue_depth",
        "amps.queries.queued",
        "amps.processing.messages_received_per_sec",
        "amps.processors.last_active",
        "amps.sow.updates_per_sec",
        "amps.queue.backlog",
        "amps.view.conflation_ratio",
    } <= names
    assert c.service_check.call_args.args[1] == c.OK


@pytest.mark.parametrize("document", [None, [], {}, {"amps": {}}, {"amps": {"instance": []}}])
def test_invalid_envelope(check, document):
    c, config = check
    c.http.get.return_value.json.return_value = document
    c.check(config)
    c.gauge.assert_not_called()
    assert c.service_check.call_args.args[1] == c.CRITICAL


def test_http_error(check):
    c, config = check
    c.http.get.return_value.raise_for_status.side_effect = HTTPError("401 secret")
    c.check(config)
    c.http.get.return_value.json.assert_not_called()
    assert c.service_check.call_args.args[1] == c.CRITICAL


def test_invalid_json(check):
    c, config = check
    c.http.get.return_value.json.side_effect = ValueError("invalid")
    c.check(config)
    assert c.service_check.call_args.args[1] == c.CRITICAL


def test_no_config_mutation(check):
    c, config = check
    config["tags"] = ["env:test"]
    before = copy.deepcopy(config)
    payload(c, {"processors": [], "clients": []})
    c.check(config)
    assert config == before
    c.gauge.assert_any_call("amps.clients.total", 0, tags=["env:test", "amps_instance:test"])


def test_real_aggregator(monkeypatch):
    aggregator.reset()
    config = {"name": "one", "url": "https://host/amps/instance.json"}
    c = AMPSCheck("amps", {}, [config])
    response = Mock()
    response.json.return_value = {
        "amps": {"instance": {"memory": {"rss": 42}, "queues": [{"topic": "q", "transferred_in": 10}]}}
    }
    monkeypatch.setattr(type(c.http), "get", Mock(return_value=response))
    c.check(config)
    aggregator.assert_metric("amps.memory.rss", value=42, tags=["amps_instance:one"], metric_type=aggregator.GAUGE)
    aggregator.assert_metric(
        "amps.queue.transferred_in",
        value=10,
        tags=["amps_instance:one", "queue:q"],
        metric_type=aggregator.MONOTONIC_COUNT,
    )
    aggregator.assert_service_check("amps.admin.check", status=c.OK, tags=["amps_instance:one"])
    aggregator.reset()
