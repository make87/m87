# ===========================
# Build Stage (manylinux2014 for glibc 2.17 compatibility)
# ===========================

FROM quay.io/pypa/manylinux2014 AS builder

# Build profile: "release" or "debug"
ARG BUILD_PROFILE=release

# Install Rust
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal

WORKDIR /workspace

# Copy workspace files
COPY Cargo.toml Cargo.lock ./

# Copy all workspace members (required by workspace manifest)
COPY m87-client ./m87-client
COPY m87-server ./m87-server
COPY m87-shared ./m87-shared

# Build the m87 binary with both runtime and command line features
RUN if [ "$BUILD_PROFILE" = "release" ]; then \
      cargo build --release --package m87-client --features runtime,cli; \
    else \
      cargo build --package m87-client --features runtime,cli; \
    fi

# ===========================
# Runtime Stage (for e2e tests and container deployments)
# ===========================
FROM debian:bookworm-slim

# Build profile: "release" or "debug"
ARG BUILD_PROFILE=release

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy compiled binary from manylinux builder
# Binary compiled on glibc 2.17 is forward-compatible with glibc 2.36 (bookworm)
COPY --from=builder /workspace/target/${BUILD_PROFILE}/m87 /app/m87

# Create config directory
RUN mkdir -p /root/.config/m87

# Add to PATH
ENV PATH="/app:${PATH}"

# No default entrypoint - commands passed via docker compose
