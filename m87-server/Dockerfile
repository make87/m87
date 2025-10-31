# ===========================
# Build Stage
# ===========================
FROM rust:1.90-slim-bookworm AS builder

# Install build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# Copy real source
COPY src ./src
RUN cargo build --release

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
COPY --from=builder /app/target/release/nexus /app/nexus

# Expose ports from AppConfig
EXPOSE 443
EXPOSE 8081

# Default entrypoint
ENTRYPOINT ["/app/nexus"]
