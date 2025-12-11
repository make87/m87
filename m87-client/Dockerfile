# ===========================
# Build Stage
# ===========================
FROM rust:1.90-slim-bookworm AS builder

# Build profile: "release" or "dev"
ARG BUILD_PROFILE=release

WORKDIR /workspace

# Install build dependencies
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./

# Copy all workspace members (required by workspace manifest)
COPY m87-client ./m87-client
COPY m87-server ./m87-server
COPY m87-shared ./m87-shared

# Build the m87 client binary with both agent and manager features
RUN if [ "$BUILD_PROFILE" = "release" ]; then \
      cargo build --release --package m87-client --features agent,manager; \
    else \
      cargo build --package m87-client --features agent,manager; \
    fi

# ===========================
# Runtime Stage
# ===========================
FROM debian:bookworm-slim

# Build profile: "release" or "dev" -> maps to "release" or "debug" directory
ARG BUILD_PROFILE=release

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy compiled binary from builder
# Note: dev profile outputs to "debug" directory, release to "release"
COPY --from=builder /workspace/target/${BUILD_PROFILE}/m87 /app/m87

# Create config directory
RUN mkdir -p /root/.config/m87

# Add to PATH
ENV PATH="/app:${PATH}"

# No default entrypoint - commands passed via docker compose
