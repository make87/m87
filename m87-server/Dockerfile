# ===========================
# Build Stage
# ===========================
FROM rust:1.90-slim-bookworm AS builder

# Build argument to control release vs debug build
ARG BUILD_MODE=release

WORKDIR /workspace

# Copy workspace files
COPY Cargo.toml Cargo.lock ./

# Copy all workspace members (required by workspace manifest)
COPY m87-client ./m87-client
COPY m87-server ./m87-server
COPY m87-shared ./m87-shared

# Build the m87-server binary
RUN if [ "$BUILD_MODE" = "release" ]; then \
      cargo build --release --package m87-server; \
    else \
      cargo build --package m87-server; \
    fi

# ===========================
# Runtime Stage
# ===========================
FROM gcr.io/distroless/cc-debian12

ARG BUILD_MODE=release

WORKDIR /app

# Copy compiled binary from builder
COPY --from=builder /workspace/target/${BUILD_MODE}/m87-server /app/m87-server

# Default entrypoint
ENTRYPOINT ["/app/m87-server"]
