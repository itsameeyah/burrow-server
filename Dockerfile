FROM rust:1.99-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /usr/sbin/nologin burrow \
    && mkdir -p /data \
    && chown -R burrow:burrow /data
COPY --from=build /src/target/release/burrow-server /usr/local/bin/burrow-server
ENV BURROW_BIND=0.0.0.0:8787 BURROW_DB=/data/burrow.db
VOLUME /data
EXPOSE 8787
USER burrow
CMD ["burrow-server"]
