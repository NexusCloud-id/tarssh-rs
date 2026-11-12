FROM rust:1.91.0-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/tarssh-rs /usr/local/bin/tarssh-rs
USER 65532:65532
EXPOSE 2222 9000
ENTRYPOINT ["/usr/local/bin/tarssh-rs"]
