FROM rust:1.91.0-bookworm@sha256:e187887ec511b3d93e45c0231d2f0fd59f1347526c58aa86343aa83c74f3e1a9 AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/tarssh-rs /usr/local/bin/tarssh-rs
USER 65532:65532
EXPOSE 2222 9000
ENTRYPOINT ["/usr/local/bin/tarssh-rs"]
