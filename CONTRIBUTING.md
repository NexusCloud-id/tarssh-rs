# Contributing

## Before opening an issue

- Search existing issues and pull requests.
- Do not post credentials, client payloads, private URLs, or personal data.
- Use the security process for vulnerabilities; do not use a public issue.

## Development

Requirements: Rust 1.91.0, Docker Engine, and Docker Compose v2.

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo build --release --locked
python3 scripts/check-assets.py
TARSSH_ALLOY_URL=http://host.docker.internal:9999/loki/api/v1/push \
TARSSH_DEPLOYMENT=asset-check \
  docker compose --env-file /dev/null config --quiet
```

Keep changes bounded. Do not add authentication, payload capture, durable
queues, outbound scanning, application log files, or unbounded labels.

## Pull requests

Explain the behavior change and its operational impact. Include tests and
validation commands. Update the README or observability assets when their
contracts change. Keep commits focused and avoid unrelated formatting.

A pull request should pass formatting, clippy, tests, release build, asset
checks, and Compose validation before review.
