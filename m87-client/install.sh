#!/bin/sh
set -e

# make87 installer script
#
# This script installs the m87 client binary to /usr/local/bin
#
# Usage:
#   curl -fsSL https://github.com/make87/make87/releases/latest/download/install-client.sh | sh
#   curl -fsSL get.make87.com | sh
#
# What it does:
#   - Detects OS (Linux) and architecture (x86_64/aarch64)
#   - Downloads specific version of m87 binary from GitHub releases
#   - Verifies SHA256 checksum
#   - Installs to /usr/local/bin/m87

# Color codes for pretty output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Installation configuration
INSTALL_DIR="/usr/local/bin"
BINARY_NAME="m87"
GITHUB_REPO="make87/make87"
# This version is set during release - do not change manually
VERSION="__VERSION__"

# Helper functions
info() {
    printf "${BLUE}==>${NC} %s\n" "$1"
}

success() {
    printf "${GREEN}✓${NC} %s\n" "$1"
}

error() {
    printf "${RED}✗ Error:${NC} %s\n" "$1" >&2
}

warning() {
    printf "${YELLOW}⚠ Warning:${NC} %s\n" "$1"
}

# Detect OS
detect_os() {
    case "$(uname -s)" in
        Linux*)
            OS="linux"
            ;;
        Darwin*)
            error "macOS is not yet supported. Please build from source or check back later."
            exit 1
            ;;
        *)
            error "Unsupported operating system: $(uname -s)"
            exit 1
            ;;
    esac
}

# Detect architecture
detect_arch() {
    ARCH="$(uname -m)"
    case "$ARCH" in
        x86_64|amd64)
            ARCH="x86_64"
            TARGET="x86_64-unknown-linux-gnu"
            ;;
        aarch64|arm64)
            ARCH="aarch64"
            TARGET="aarch64-unknown-linux-gnu"
            ;;
        *)
            error "Unsupported architecture: $ARCH"
            error "Supported architectures: x86_64 (AMD64), aarch64 (ARM64)"
            exit 1
            ;;
    esac
}

# Check for required tools
check_dependencies() {
    missing_deps=""

    # Check for download tool
    if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
        missing_deps="${missing_deps} curl or wget"
    fi

    # Check for checksum tool
    if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
        missing_deps="${missing_deps} sha256sum or shasum"
    fi

    if [ -n "$missing_deps" ]; then
        error "Missing required dependencies:$missing_deps"
        error "Please install them and try again"
        exit 1
    fi
}

# Download file with progress
download() {
    url="$1"
    output="$2"

    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --progress-bar "$url" -o "$output"
    elif command -v wget >/dev/null 2>&1; then
        wget -q --show-progress "$url" -O "$output"
    else
        error "No download tool available"
        exit 1
    fi
}

# Verify SHA256 checksum
verify_checksum() {
    file="$1"
    expected_checksum="$2"
    actual_checksum=""

    if command -v sha256sum >/dev/null 2>&1; then
        actual_checksum=$(sha256sum "$file" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
        actual_checksum=$(shasum -a 256 "$file" | awk '{print $1}')
    else
        warning "Could not verify checksum (no sha256sum or shasum found)"
        return 0
    fi

    if [ "$actual_checksum" != "$expected_checksum" ]; then
        error "Checksum verification failed!"
        error "Expected: $expected_checksum"
        error "Got:      $actual_checksum"
        exit 1
    fi

    success "Checksum verified"
}

# Check if sudo is needed for installation
needs_sudo() {
    if [ -w "$INSTALL_DIR" ]; then
        return 1  # Don't need sudo
    else
        return 0  # Need sudo
    fi
}

# Install binary
install_binary() {
    src="$1"
    dest="$INSTALL_DIR/$BINARY_NAME"

    if needs_sudo; then
        info "Installing to $dest (requires sudo)"
        sudo install -m 755 "$src" "$dest"
    else
        info "Installing to $dest"
        install -m 755 "$src" "$dest"
    fi

    success "Installed $BINARY_NAME to $dest"
}

# Main installation flow
main() {
    echo ""
    info "make87 installer"
    echo ""

    # Step 1: Detection
    info "Detecting system..."
    detect_os
    detect_arch
    success "Detected: $OS ($ARCH)"

    check_dependencies

    # Step 2: Verify version is set
    if [ "$VERSION" = "__VERSION__" ]; then
        error "This install script has not been properly configured with a version."
        error "Please download the install.sh from a specific release:"
        error "  https://github.com/$GITHUB_REPO/releases"
        exit 1
    fi

    info "Installing version: v$VERSION"

    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' EXIT

    # Step 3: Download binary
    binary_name="${BINARY_NAME}-${TARGET}"
    download_url="https://github.com/$GITHUB_REPO/releases/download/v${VERSION}/${binary_name}"
    binary_path="$tmp_dir/$binary_name"

    info "Downloading $binary_name..."
    download "$download_url" "$binary_path"
    success "Downloaded binary"

    # Step 4: Download and verify checksum
    info "Verifying checksum..."
    checksums_url="https://github.com/$GITHUB_REPO/releases/download/v${VERSION}/SHA256SUMS"
    checksums_file="$tmp_dir/SHA256SUMS"

    if download "$checksums_url" "$checksums_file" 2>/dev/null; then
        expected_checksum=$(grep "$binary_name" "$checksums_file" | awk '{print $1}')

        if [ -n "$expected_checksum" ]; then
            verify_checksum "$binary_path" "$expected_checksum"
        else
            warning "Checksum not found in SHA256SUMS, skipping verification"
        fi
    else
        warning "Could not download SHA256SUMS, skipping verification"
    fi

    # Step 5: Install
    install_binary "$binary_path"

    # Step 6: Verify installation
    info "Verifying installation..."
    if command -v "$BINARY_NAME" >/dev/null 2>&1; then
        installed_version=$("$BINARY_NAME" --version 2>/dev/null || echo "unknown")
        success "Installation verified: $installed_version"
    else
        warning "$BINARY_NAME not found in PATH. You may need to restart your shell."
    fi

    # Success message
    echo ""
    success "make87 has been installed successfully!"
    echo ""
    echo "Get started with:"
    echo "  $BINARY_NAME --help          # Show all commands"
    echo "  $BINARY_NAME login           # Authenticate with make87"
    echo "  $BINARY_NAME device install   # Install as system service"
    echo ""
}

# Run main installation
main
