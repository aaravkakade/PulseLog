# Build and runtime image for a PulseLog broker.
#
# Two stages: a builder with the toolchain, and a slim runtime that carries
# only the binaries and the shared libraries they actually need. The runtime
# runs as a non-root user and owns its data directory.
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# Warnings are errors in the image build: a container that ships is a container
# that compiled cleanly.
RUN cmake -S . -B build -G Ninja \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DPULSELOG_BUILD_TESTS=OFF \
        -DPULSELOG_BUILD_BENCHMARKS=OFF \
        -DPULSELOG_WERROR=ON \
    && cmake --build build -j"$(nproc)"

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        libstdc++6 \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 pulselog

COPY --from=builder /src/build/bin/pulselog-broker /usr/local/bin/
COPY --from=builder /src/build/bin/pulselog-cli /usr/local/bin/
COPY --from=builder /src/build/bin/pulselog-bench /usr/local/bin/

RUN mkdir -p /var/lib/pulselog && chown pulselog:pulselog /var/lib/pulselog
USER pulselog
WORKDIR /var/lib/pulselog

EXPOSE 9092 9644
VOLUME ["/var/lib/pulselog"]

# The metrics endpoint answers even when the data plane is saturated, which is
# exactly when a health check needs to work.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:9644/health || exit 1

ENTRYPOINT ["pulselog-broker"]
CMD ["--broker.data.dir=/var/lib/pulselog", "--net.listen=0.0.0.0:9092", "--metrics.host=0.0.0.0"]
