# ===========================
# Build Stage
# ===========================
FROM rust:1.90-slim-bookworm AS builder

WORKDIR /workspace

# Copy workspace files
COPY Cargo.toml Cargo.lock ./

# Copy all workspace members (required by workspace manifest)
COPY m87-client ./m87-client
COPY m87-server ./m87-server
COPY m87-shared ./m87-shared

# Build the m87-server binary
RUN cargo build --release --package m87-server

# ===========================
# Runtime Stage
# ===========================
FROM gcr.io/distroless/cc-debian12

WORKDIR /app

# Copy compiled binary from builder
COPY --from=builder /workspace/target/release/m87-server /app/m87-server

# Default entrypoint
ENTRYPOINT ["/app/m87-server"]
