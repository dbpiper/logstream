ARG CARGO_REGISTRY_DIR=/usr/local/cargo/registry
ARG CARGO_GIT_DIR=/usr/local/cargo/git

FROM rust:1.88-slim AS chef
WORKDIR /app
# Build deps (cached)
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef
# Capture dependency graph
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY config.toml ./config.toml
RUN cargo chef prepare --recipe-path recipe.json

FROM rust:1.88-slim AS builder
ARG CARGO_REGISTRY_DIR
ARG CARGO_GIT_DIR
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=chef /usr/local/cargo/bin/cargo-chef /usr/local/cargo/bin/cargo-chef
COPY --from=chef /app/recipe.json recipe.json
# Build deps layer (uses mounted cargo cache if provided)
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release

FROM debian:12-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/logstream /usr/local/bin/logstream
COPY config.toml /app/config.toml
RUN mkdir -p /state && chown -R 1000:1000 /state
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/logstream", "/app/config.toml"]

