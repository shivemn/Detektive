# One image with both binaries: detektive-consumer (the default) and
# detektive-replay (built with Redpanda support).
#
#   docker build -t detektive:dev .
#
# Build stage: compiles librdkafka from source, so it needs cmake and the
# curl headers (curl itself is not used at run time).
FROM rust:1-bookworm AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake libcurl4-openssl-dev \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# the cache mounts keep downloaded crates and compiled dependencies between
# builds, so changing one source file does not rebuild librdkafka
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
        -p detektive-consumer -p detektive-replay --features detektive-replay/kafka \
 && cp target/release/detektive-consumer target/release/detektive-replay /usr/local/bin/

# Run stage: the binaries only need libc, libm and libgcc_s, which the slim
# image already has.
FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --no-create-home detektive
COPY --from=build /usr/local/bin/detektive-consumer /usr/local/bin/detektive-replay /usr/local/bin/
USER detektive
EXPOSE 9898
ENTRYPOINT ["detektive-consumer"]
