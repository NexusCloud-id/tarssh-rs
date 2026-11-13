## Change

<!-- What changed? Why? -->

## Validation

- [ ] `cargo fmt --check`
- [ ] `cargo clippy --all-targets --locked -- -D warnings`
- [ ] `cargo test --locked`
- [ ] `cargo build --release --locked`
- [ ] `python3 scripts/check-assets.py`
- [ ] Compose validation passed

## Safety

- [ ] No credentials, client payloads, or private data added
- [ ] No unbounded connection, queue, retry, batch, label, or shutdown path added
- [ ] Documentation and dashboard contracts updated when needed
