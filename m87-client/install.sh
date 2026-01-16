#!/bin/sh
set -e

# make87 installer script
#
# This script installs the m87 client binary to ~/.local/bin (no sudo required)
#
# Usage:
#   curl -fsSL https://github.com/make87/m87/releases/latest/download/install-client.sh | sh
#   curl -fsSL get.make87.com | sh
#
# What it does:
#   - Detects OS (Linux, macOS) and architecture (x86_64/aarch64)
#   - Downloads specific version of m87 binary from GitHub releases
#   - Verifies SHA256 checksum
#   - Installs to ~/.local/bin/m87
#   - Prints PATH instructions if needed
#
# Supported platforms:
#   - Linux x86_64 (AMD64)
#   - Linux aarch64 (ARM64)
#   - macOS aarch64 (Apple Silicon)
#   - Windows (via WSL)

# Color codes for pretty output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Installation configuration
INSTALL_DIR="$HOME/.local/bin"
BINARY_NAME="m87"
GITHUB_REPO="make87/m87"
# This version is set during release - do not change manually
VERSION="${M87_VERSION:-__VERSION__}"
# Allow custom download URL for testing (e.g., M87_DOWNLOAD_URL=http://localhost:8000)
DOWNLOAD_BASE_URL="${M87_DOWNLOAD_URL:-https://github.com/$GITHUB_REPO/releases/download}"

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
            OS="macos"
            ;;
        *)
            error "Unsupported operating system: $(uname -s)"
            exit 1
            ;;
    esac
}

# Detect architecture (must be called after detect_os)
detect_arch() {
    ARCH="$(uname -m)"
    case "$ARCH" in
        x86_64|amd64)
            ARCH="x86_64"
            if [ "$OS" = "macos" ]; then
                error "macOS x86_64 (Intel) is not supported. Only Apple Silicon (ARM64) is supported."
                exit 1
            fi
            TARGET="x86_64-unknown-linux-musl"
            ;;
        aarch64|arm64)
            ARCH="aarch64"
            if [ "$OS" = "macos" ]; then
                TARGET="aarch64-apple-darwin"
            else
                TARGET="aarch64-unknown-linux-musl"
            fi
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

# Install binary
install_binary() {
    src="$1"
    dest="$INSTALL_DIR/$BINARY_NAME"

    # Create install directory if it doesn't exist
    if [ ! -d "$INSTALL_DIR" ]; then
        info "Creating $INSTALL_DIR..."
        mkdir -p "$INSTALL_DIR"
    fi

    info "Installing to $dest"
    install -m 755 "$src" "$dest"

    success "Installed $BINARY_NAME to $dest"
}

# Check if install directory is in PATH and print instructions if not
check_path() {
    # First check current PATH
    case ":$PATH:" in
        *":$INSTALL_DIR:"*)
            return 0  # Already in PATH
            ;;
    esac

    # Check if a new login shell would have it in PATH
    # Use $SHELL to respect user's preferred shell (zsh, bash, fish, etc.)
    # This handles .profile/.zshrc/.bashrc conditionals like: if [ -d "$HOME/.local/bin" ]; then PATH=...
    # We get PATH from user's shell, then check it with POSIX tools (portable across bash/zsh/fish)
    new_path=$("$SHELL" -l -c 'echo $PATH' 2>/dev/null) || true
    if echo ":${new_path}:" | grep -q ":${INSTALL_DIR}:"; then
        echo ""
        success "$INSTALL_DIR will be in PATH after shell restart"
        echo ""
        echo "Run one of:"
        echo "  exec \$SHELL -l    # Restart current shell"
        echo "  source ~/.profile  # Reload profile"
        return 0
    fi

    # Not in PATH and won't be automatically (or check failed) - show manual instructions
    echo ""
    warning "$INSTALL_DIR is not in your PATH"
    echo ""
    echo "Add this line to your shell configuration file:"
    echo ""
    echo "  For bash (~/.bashrc or ~/.bash_profile):"
    echo "    export PATH=\"\$HOME/.local/bin:\$PATH\""
    echo ""
    echo "  For zsh (~/.zshrc):"
    echo "    export PATH=\"\$HOME/.local/bin:\$PATH\""
    echo ""
    echo "  For fish (~/.config/fish/config.fish):"
    echo "    set -gx PATH \$HOME/.local/bin \$PATH"
    echo ""
    echo "Then restart your shell or run: source <config-file>"
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
    download_url="${DOWNLOAD_BASE_URL}/v${VERSION}/${binary_name}"
    binary_path="$tmp_dir/$binary_name"

    info "Downloading $binary_name..."
    download "$download_url" "$binary_path"
    success "Downloaded binary"

    # Step 4: Download and verify checksum
    info "Verifying checksum..."
    checksums_url="${DOWNLOAD_BASE_URL}/v${VERSION}/SHA256SUMS"
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

    # Step 5: Remove macOS quarantine attribute (if on macOS)
    if [ "$OS" = "macos" ]; then
        info "Removing macOS quarantine attribute..."
        xattr -d com.apple.quarantine "$binary_path" 2>/dev/null || true
        success "Quarantine attribute removed"
    fi

    # Step 6: Install
    install_binary "$binary_path"

    # Step 7: Check if PATH includes install directory
    check_path

    # Step 8: Verify installation
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
    echo "  $BINARY_NAME --help               # Show all commands"
    echo "  $BINARY_NAME login                # Authenticate with make87"
    if [ "$OS" = "linux" ]; then
        echo ""
        echo "Runtime mode (Linux only):"
        echo "  $BINARY_NAME runtime login          # Authenticate runtime"
        echo "  $BINARY_NAME runtime enable --now   # Install as system service"
    fi
    echo ""
}

# Run main installation
main
