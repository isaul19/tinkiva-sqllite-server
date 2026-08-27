FROM rust:1.97-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /var/lib/tinkivadb tinkiva \
    && install -d -o tinkiva -g tinkiva /var/lib/tinkivadb/databases
COPY --from=builder /build/target/release/tinkiva-database /usr/local/bin/tinkiva-database
USER tinkiva
ENV TINKIVA_BIND=0.0.0.0:7000 \
    TINKIVA_DATABASE_DIR=/var/lib/tinkivadb/databases
EXPOSE 7000
VOLUME ["/var/lib/tinkivadb/databases"]
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD ["curl", "--fail", "--silent", "http://127.0.0.1:7000/health"]
ENTRYPOINT ["tinkiva-database"]
