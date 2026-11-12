# tarssh-rs operator assets

Container, Compose, Prometheus scrape, Alloy receiver, and Grafana dashboard assets for the `tarssh-rs` SSH tarpit. The stack is intentionally additive: it does not deploy Grafana, Loki, Alloy, or Prometheus, and it does not retain application logs in Docker.

## Compose

Requires Docker Engine and Compose v2. Set the required values in the shell, not a committed `.env` file:

```sh
export TARSSH_ALLOY_URL=http://host.docker.internal:9999/loki/api/v1/push
export TARSSH_DEPLOYMENT=edge-1
# Optional: TARSSH_HOST_PORT=2222 TARSSH_METRICS_HOST_PORT=9000

docker compose --env-file /dev/null config --quiet
docker compose --env-file /dev/null up --build
```

Compose runs one numeric non-root service with a read-only root filesystem, no capabilities, `no-new-privileges`, PID/memory/file-descriptor limits, a 15-second stop grace period, and Docker logging driver `none`. Metrics bind to host loopback by default. The tarpit port is the only public mapping; do not map host port 22 without first preserving administrative SSH on another port or address.

The image is a Rust 1.91.0 multi-stage build with Debian CA certificates. Docker build is deferred until the service source is present in the coordinator's integrated tree.

## Configuration and local run

| Environment variable | Default / validation |
|---|---|
| `TARSSH_LISTEN_ADDR` | `0.0.0.0:2222`, explicit IP:port |
| `TARSSH_METRICS_ADDR` | `0.0.0.0:9000`, explicit IP:port |
| `TARSSH_DELAY_MS` | `10000`, integer 1..=86400000 |
| `TARSSH_MAX_LINE_LENGTH` | `32`, wire bytes including CRLF, 3..=255 |
| `TARSSH_MAX_CONNECTIONS` | `4096`, integer 1..=65536 |
| `TARSSH_WRITE_TIMEOUT_MS` | `5000`, integer 1..=60000 |
| `TARSSH_ALLOY_URL` | Required full HTTP(S) push URL; host required; no userinfo, query, fragment |
| `TARSSH_DEPLOYMENT` | Required 1..=64 ASCII letters, digits, `_`, `.`, `-`; unique per concurrent process |

Compose fixes internal addresses to the published ports. For direct local execution, keep metrics private and point Alloy to a reachable host listener:

```sh
cargo build --release --locked
TARSSH_LISTEN_ADDR=127.0.0.1:2222 \
TARSSH_METRICS_ADDR=127.0.0.1:9000 \
TARSSH_ALLOY_URL=http://127.0.0.1:9999/loki/api/v1/push \
TARSSH_DEPLOYMENT=local-1 ./target/release/tarssh-rs
```

The first random ASCII CRLF line follows the full delay; each subsequent
line follows another full delay. This is not a real SSH server. It never
captures credentials or client payloads; peers can disconnect themselves.
Increasing the connection cap requires reviewing the memory and descriptor
limits (the shipped descriptor limit supports the default cap, not 65536).

### Delivery limits

The in-memory queue holds 4096 events and drops the newest when full. A
single worker flushes at one second or 128 events, with at most 256 KiB of
event JSON per batch; events over 2 KiB are dropped. No durable spool or
application log file exists. Only `service_name` and `deployment` are Loki
stream labels; peer IP and connection ID remain JSON fields.

Each request has a one-second connect timeout and two-second attempt
timeout. Transport failures, 408, 429, and 5xx retry up to three total
attempts, with 250 ms then one-second backoff within a hard batch deadline.
Other responses fail immediately; only 200/204 mean Alloy accepted the
batch, not Loki durability. Redirects, system proxies, and hidden HTTP
retries are disabled. Queue/batch drops and failures are metrics, not
recursive logs. Retries after an accepted timeout can duplicate entries.
Crashes lose the queue. Shutdown drains within ten seconds; final losses
may disappear before Prometheus scrapes them.

## Alloy integration

Append `observability/alloy/tarssh.alloy` to an existing Alloy configuration. Replace `PRIVATE_BRIDGE_IP` with the private host bridge address reachable from the container, and `EXISTING_WRITER` with the existing `loki.write` component name. The HTTP receiver must be firewall-restricted to the intended container/network; never expose unauthenticated ingestion publicly. The auxiliary gRPC listener is explicitly bound to `127.0.0.1` on port `0`.

A host-installed Alloy listener bound only to `127.0.0.1` cannot receive bridge traffic. If Alloy runs in a container, use its private shared-network address/service name instead. Plain HTTP is suitable only for the same-host private link; use verified TLS across a trust boundary. `use_incoming_timestamp = true` preserves application event timestamps. Receiver outages lose best-effort queued events; retries can duplicate after an accepted request timeout.

## Prometheus and Grafana

Merge `observability/prometheus/tarssh.yaml` into the existing `scrape_configs` list. It assumes Prometheus runs on the host and scrapes `127.0.0.1:9000`; use a private reachable address for another deployment path. Do not add application labels to the scrape job.

Use Grafana **Dashboards > New > Import**, upload
`observability/grafana/tarssh-rs.json`, select existing Prometheus and Loki
data sources, then import. Target: classic dashboard JSON on Grafana 11.6+;
live minimum/current-version imports remain an integration check.

The single deployment selector comes from `tarssh_connections_active`.
Queries use exact JSON-quoted scalar literals rather than interpolated
regular expressions. Optional peer-IP/connection-ID fields are literal
substring filters on general connection logs; blank matches all. Selected
connection history instead requires an exact ID; blank returns no
connection events. IP filtering also applies there. Metrics data links
preserve deployment/time and open the connection log panel. Text filters
never affect metrics or the top-IP summary.

Logs are best effort and can differ from Prometheus counters. Source IP
reflects the socket peer; NAT, proxies, or rootless networking can change
it. No forwarded address is trusted. Top peer IPs count opened events in
the 15 minutes ending at the selected range end, extracting only peer_ip
and applying `topk(10)` at query time. This limits output, not intermediate
series: Loki's `max_query_series` can still reject many-IP inputs. Narrow
deployment/time or use raw logs if that happens; do not index peer IPs.

## Checks and rollback

Run `python3 scripts/check-assets.py` and
`docker compose --env-file /dev/null config --quiet` with required variables
set. The Python check uses only the standard library plus the Compose CLI;
it checks JSON/query contracts and normalized Compose hardening, not live
PromQL/LogQL execution. Alloy/Grafana/promtool are not bundled. After
substitution, validate the complete existing Alloy config with `alloy
validate` and Prometheus config with `promtool check config` before reload.

Docker logging `none` means `docker logs` cannot diagnose startup. Run the
binary locally in the foreground using the shell example to see its single
redacted stderr diagnostic; runtime logging remains Alloy-only. An Alloy
outage alone must not prevent startup. Check `/metrics` and exporter
failure/drop rates, not Docker logs.

Stop only this service with `docker compose --env-file /dev/null down`.
Restore the previous image tag, then start without rebuilding. Restore the
previous dashboard and remove only the additive scrape job/receiver if
needed; validate the existing configs before reloading. No data migration
exists. Do not alter daemon-wide logging or operator stack retention.
