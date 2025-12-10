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

# Build the m87-server binary
RUN if [ "$BUILD_PROFILE" = "release" ]; then \
      cargo build --release --package m87-server; \
    else \
      cargo build --package m87-server; \
    fi

# ===========================
# Runtime Stage
# ===========================
FROM gcr.io/distroless/cc-debian12

# Build profile: "release" or "dev" -> maps to "release" or "debug" directory
ARG BUILD_PROFILE=release

WORKDIR /app

# Copy compiled binary from builder
# Note: dev profile outputs to "debug" directory, release to "release"
COPY --from=builder /workspace/target/${BUILD_PROFILE}/m87-server /app/m87-server
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Default entrypoint
ENTRYPOINT ["/app/m87-server"]
