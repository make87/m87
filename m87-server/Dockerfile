# ===========================
# Build Stage
# ===========================
FROM rust:1.90-slim-bookworm AS builder

# Install build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Copy workspace files
COPY Cargo.toml Cargo.lock ./

# Copy both workspace members (required by workspace manifest)
COPY m87-client ./m87-client
COPY m87-server ./m87-server

# Build the m87-server binary
RUN cargo build --release --package m87-server

# ===========================
# Runtime Stage
# ===========================
FROM debian:bookworm-slim

# Install only runtime libs actually required
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libgcc-s1 \
    libstdc++6 \
    libc6 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy compiled binary from builder
COPY --from=builder /workspace/target/release/m87-server /app/m87-server


# Default entrypoint
ENTRYPOINT ["/app/m87-server"]
