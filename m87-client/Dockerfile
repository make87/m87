# ===========================
# Build Stage (cross-compile to musl for static binaries)
# ===========================
FROM rust:slim-bookworm AS builder

ARG BUILD_PROFILE=release
ARG TARGETARCH

# Install build dependencies and musl toolchain
RUN apt-get update && apt-get install -y \
    musl-tools \
    musl-dev \
    build-essential \
    cmake \
    clang \
    perl \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Add musl targets
RUN rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl

# Install aarch64 cross-compiler if needed
RUN if [ "$TARGETARCH" = "arm64" ]; then \
      apt-get update && apt-get install -y gcc-aarch64-linux-gnu && rm -rf /var/lib/apt/lists/*; \
    fi

WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY m87-client ./m87-client
COPY m87-server ./m87-server
COPY m87-shared ./m87-shared

# Set up cross-compilation environment
ENV RUSTFLAGS="-C target-feature=+crt-static"

RUN if [ "$TARGETARCH" = "arm64" ]; then \
      export TARGET="aarch64-unknown-linux-musl"; \
      export CC_aarch64_unknown_linux_musl=aarch64-linux-gnu-gcc; \
      export AR_aarch64_unknown_linux_musl=aarch64-linux-gnu-ar; \
    else \
      export TARGET="x86_64-unknown-linux-musl"; \
    fi && \
    if [ "$BUILD_PROFILE" = "release" ]; then \
      cargo build --release --target $TARGET --package m87-client --features runtime,cli; \
    else \
      cargo build --target $TARGET --package m87-client --features runtime,cli; \
    fi && \
    mkdir -p /workspace/out && \
    cp /workspace/target/$TARGET/${BUILD_PROFILE}/m87 /workspace/out/m87

# ===========================
# Runtime Stage (for e2e tests and container deployments)
# ===========================
FROM alpine:3.21

ARG BUILD_PROFILE=release

RUN apk add --no-cache \
    ca-certificates \
    curl \
    netcat-openbsd

WORKDIR /app

COPY --from=builder /workspace/out/m87 /app/m87

ENV PATH="/app:${PATH}"

ENTRYPOINT ["/app/m87"]
