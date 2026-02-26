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
COPY m87-privileged ./m87-privileged
COPY m87-shared ./m87-shared
# Stub out other workspace members so Cargo.toml resolves
COPY m87-client/Cargo.toml ./m87-client/Cargo.toml
RUN mkdir -p m87-client/src && echo "" > m87-client/src/lib.rs && echo "fn main() {}" > m87-client/src/main.rs
COPY m87-server/Cargo.toml ./m87-server/Cargo.toml
RUN mkdir -p m87-server/src && echo "" > m87-server/src/lib.rs && echo "fn main() {}" > m87-server/src/main.rs

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
      cargo build --release --target $TARGET --package m87-privileged; \
    else \
      cargo build --target $TARGET --package m87-privileged; \
    fi && \
    mkdir -p /workspace/out && \
    cp /workspace/target/$TARGET/${BUILD_PROFILE}/m87-privileged /workspace/out/m87-privileged && \
    cp /workspace/target/$TARGET/${BUILD_PROFILE}/m87-sudo /workspace/out/m87-sudo

# ===========================
# Runtime Stage
# ===========================
FROM alpine:3.21

COPY --from=builder /workspace/out/m87-privileged /app/m87-privileged
COPY --from=builder /workspace/out/m87-sudo /app/m87-sudo

ENTRYPOINT ["/app/m87-privileged"]
