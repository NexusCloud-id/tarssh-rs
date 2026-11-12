#!/usr/bin/env python3
"""Static contract check for importable deployment artifacts."""
import json
import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
dashboard = json.loads((ROOT / "observability/grafana/tarssh-rs.json").read_text())
assert dashboard["__requires"][0]["version"] == "11.6.0"
assert {item["name"] for item in dashboard["__inputs"]} == {"PROMETHEUS", "LOKI"}
assert dashboard["uid"] == "tarssh-rs"
assert len(dashboard["panels"]) == 11
allowed = {"stat", "timeseries", "logs", "table"}
allowed_metrics = {
    "tarssh_connections_active", "tarssh_connections_accepted_total",
    "tarssh_connections_rejected_total", "tarssh_connections_closed_total",
    "tarssh_connection_duration_seconds_bucket", "tarssh_bytes_sent_total",
    "tarssh_lines_sent_total", "tarssh_accept_errors_total",
    "tarssh_log_queue_events", "tarssh_log_events_sent_total",
    "tarssh_log_events_dropped_total", "tarssh_log_push_failures_total",
}
metrics = " ".join(json.dumps(panel) for panel in dashboard["panels"])
for name in ("tarssh_connections_active", "tarssh_connections_accepted_total",
             "tarssh_connections_rejected_total", "tarssh_bytes_sent_total",
             "tarssh_lines_sent_total", "tarssh_connection_duration_seconds",
             "tarssh_connections_closed_total", "tarssh_accept_errors_total",
             "tarssh_log_queue_events", "tarssh_log_events_sent_total",
             "tarssh_log_events_dropped_total", "tarssh_log_push_failures_total"):
    assert name in metrics, name
for panel in dashboard["panels"]:
    assert panel["type"] in allowed
    assert panel.get("datasource", {}).get("uid") in ("${PROMETHEUS}", "${LOKI}")
    assert panel.get("targets"), panel["id"]
    for target in panel["targets"]:
        query = target["expr"]
        assert 'service_name="tarssh-rs"' in query
        assert 'deployment=${deployment:json}' in query
        if panel["datasource"]["type"] == "loki":
            assert '| __error__=""' in query
            assert ':regex}' not in query and ':raw}' not in query
        else:
            assert "peer_ip" not in query and "connection_id" not in query
            assert set(re.findall(r"tarssh_[a-z_]+", query)) <= allowed_metrics
            links = panel["fieldConfig"]["defaults"]["links"]
            assert any("${deployment:percentencode}" in link["url"] and
                       "from=${__from}&to=${__to}" in link["url"] and
                       "viewPanel=8" in link["url"] for link in links)
vars_ = {item["name"]: item for item in dashboard["templating"]["list"]}
assert {"deployment", "peer_ip", "connection_id"} <= vars_.keys()
assert vars_["deployment"]["datasource"]["uid"] == "${PROMETHEUS}"
assert not vars_["deployment"]["multi"] and not vars_["deployment"]["includeAll"]
logs = dashboard["panels"][7]["targets"][0]["expr"]
assert '|= ${peer_ip:json}' in logs and '|= ${connection_id:json}' in logs
assert '| line_format "{{.original_line}}"' in logs
history = dashboard["panels"][9]["targets"][0]["expr"]
assert '| connection_id=${connection_id:json}' in history
assert "topk(10" in metrics and "[15m]" in metrics
alloy = (ROOT / "observability/alloy/tarssh.alloy").read_text()
assert 'use_incoming_timestamp = true' in alloy
assert 'listen_address = "127.0.0.1"' in alloy
compose = (ROOT / "compose.yaml").read_text()
for required in ("read_only: true", "no-new-privileges", "cap_drop: [ALL]",
                 "logging:", "driver: none", "stop_grace_period: 15s"):
    assert required in compose, required
print("asset contract: OK")
env = dict(os.environ, TARSSH_DEPLOYMENT="asset-check",
           TARSSH_ALLOY_URL="http://host.docker.internal:9999/loki/api/v1/push")
result = subprocess.run(
    ["docker", "compose", "--env-file", "/dev/null", "config", "--format", "json"],
    cwd=ROOT, env=env, check=True, capture_output=True, text=True)
services = json.loads(result.stdout)["services"]
assert set(services) == {"tarssh"}
service = services["tarssh"]
assert service["user"] == "65532:65532" and service["read_only"]
assert service["cap_drop"] == ["ALL"]
assert "no-new-privileges:true" in service["security_opt"]
assert service["logging"]["driver"] == "none"
assert service["pids_limit"] > 0 and service["mem_limit"]
assert service["stop_grace_period"] == "15s"
assert service["ulimits"]["nofile"]["soft"] >= 4096 + 1024
assert not service.get("volumes") and not service.get("privileged")
assert any(p["target"] == 9000 and p["host_ip"] == "127.0.0.1"
           for p in service["ports"])
print("Compose schema and hardening: OK")
