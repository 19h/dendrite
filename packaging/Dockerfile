FROM rust:1.94.0-slim-bookworm AS builder
ENV RUSTUP_TOOLCHAIN=1.94.0-x86_64-unknown-linux-gnu
WORKDIR /source
COPY . .
RUN cargo build --locked --release -p dendrite-daemon -p dendrite-cli

FROM debian:bookworm-slim
LABEL org.opencontainers.image.title="Dendrite" \
      org.opencontainers.image.description="Memory-safe BitTorrent daemon" \
      org.opencontainers.image.authors="Kenan Sulayman"
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 dendrite \
    && install -d -o dendrite -g dendrite \
        /home/dendrite/dendrite-data /home/dendrite/downloads
COPY --from=builder /source/target/release/dendrite /usr/local/bin/dendrite
COPY --from=builder /source/target/release/dendritectl /usr/local/bin/dendritectl
USER dendrite
WORKDIR /home/dendrite
VOLUME ["/home/dendrite/dendrite-data", "/home/dendrite/downloads"]
EXPOSE 8412/tcp 16493/tcp 16493/udp 16309/udp
ENTRYPOINT ["dendrite"]
