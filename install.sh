#!/bin/bash
# Orbit local CLI (`orbit`) installation script.
# Supports macOS (darwin), Linux, and Windows (Git Bash) on x86_64 and aarch64.
# Run with --help for usage examples.

set -euo pipefail

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

OPTIONS:
    --version VERSION    Install a specific version (e.g., v0.51.0). Defaults to the latest release.
    --force              Reinstall even if 'orbit' already exists in the install directory.
    --help               Show this help message.

EXAMPLES:
    # Linux / macOS
    curl -fsSL https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh | bash
    curl -fsSL https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh | bash -s -- --version v0.51.0
    curl -fsSL https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh | bash -s -- --force

    # Windows (CMD or PowerShell, no Git Bash required)
    C:\Progra~1\Git\bin\sh.exe -c 'curl -fsSL https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh -o $TMP/install.sh ; bash $TMP/install.sh'
    C:\Progra~1\Git\bin\sh.exe -c 'curl -fsSL https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh -o $TMP/install.sh ; bash $TMP/install.sh --version v0.51.0'
    C:\Progra~1\Git\bin\sh.exe -c 'curl -fsSL https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh -o $TMP/install.sh ; bash $TMP/install.sh --force'

    # After downloading:
    bash install.sh
    bash install.sh --version v0.51.0
    bash install.sh --force
EOF
}

case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*)
        INSTALL_DIR="${LOCALAPPDATA:-$USERPROFILE/AppData/Local}/Programs/orbit"
        ;;
    *)
        INSTALL_DIR="${HOME}/.local/bin"
        ;;
esac
TEMP_DIR=$(mktemp -d)
VERSION=""
FORCE_INSTALL=false

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

cleanup() {
    if [ -d "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR"
    fi
}
trap cleanup EXIT

error() {
    echo -e "${RED}Error: $1${NC}" >&2
    exit 1
}

success() {
    echo -e "${GREEN}$1${NC}"
}

warning() {
    echo -e "${YELLOW}$1${NC}"
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            if [[ -z "${2:-}" || "$2" == --* ]]; then
                error "Missing value for --version"
            fi
            VERSION="$2"
            shift 2
            ;;
        --force)
            FORCE_INSTALL=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            ;;
    esac
done

if [ -n "$VERSION" ]; then
    case "$VERSION" in
        v*) : ;;
        *) VERSION="v$VERSION" ;;
    esac
    if ! echo "$VERSION" | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$'; then
        error "Invalid version format: $VERSION (expected: vX.Y.Z or vX.Y.Z-suffix)"
    fi
fi

detect_os() {
    case "$(uname -s)" in
        Linux*)              echo "linux" ;;
        Darwin*)             echo "darwin" ;;
        MINGW*|MSYS*|CYGWIN*) echo "windows" ;;
        *)                   error "Unsupported operating system: $(uname -s)" ;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)  echo "x86_64" ;;
        arm64|aarch64) echo "aarch64" ;;
        *)             error "Unsupported architecture: $(uname -m)" ;;
    esac
}

command_exists() {
    command -v "$1" >/dev/null 2>&1
}

download_file() {
    local url="$1"
    local output="$2"

    if command_exists curl; then
        curl -fsSL --progress-bar "$url" -o "$output" || return 1
    elif command_exists wget; then
        wget -q --show-progress "$url" -O "$output" || return 1
    else
        error "Neither curl nor wget found. Please install one of them."
    fi
}

verify_checksum() {
    local file="$1"
    local checksum_url="$2"
    local checksum_file="${TEMP_DIR}/checksum.sha256"

    echo "Downloading checksum..."
    if ! download_file "$checksum_url" "$checksum_file"; then
        error "Checksum file not found at $checksum_url. Installation aborted for security reasons."
    fi

    echo "Verifying checksum..."
    local expected_checksum
    expected_checksum=$(awk '{print $1}' "$checksum_file")
    local actual_checksum

    if command_exists sha256sum; then
        actual_checksum=$(sha256sum "$file" | awk '{print $1}')
    elif command_exists shasum; then
        actual_checksum=$(shasum -a 256 "$file" | awk '{print $1}')
    else
        error "No SHA256 tool found (sha256sum or shasum). Cannot verify download integrity."
    fi

    if [ "$expected_checksum" != "$actual_checksum" ]; then
        error "Checksum verification failed for $file using $checksum_url
Expected: $expected_checksum
Actual:   $actual_checksum"
    fi

    success "Checksum verified successfully."
}

install_vcredist() {
    local installed=false
    if reg query "HKLM\\SOFTWARE\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\X64" /v Installed 2>/dev/null | grep -q "0x1"; then
        installed=true
    elif reg query "HKLM\\SOFTWARE\\WOW6432Node\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\X64" /v Installed 2>/dev/null | grep -q "0x1"; then
        installed=true
    fi
    if [ "$installed" = true ]; then
        return
    fi

    echo "Installing Visual C++ Redistributable (required by orbit on Windows)..."
    if command_exists winget; then
        winget install --id Microsoft.VCRedist.2015+.x64 \
            --accept-package-agreements --accept-source-agreements --silent || true
    else
        local vcredist="${TEMP_DIR}/vc_redist.x64.exe"
        download_file "https://aka.ms/vs/17/release/vc_redist.x64.exe" "$vcredist"
        cmd //c "$vcredist" /install /quiet /norestart || true
    fi
}

install_orbit() {
    local platform="$1"
    local arch="$2"

    local binary_name="orbit"
    [ "$platform" = "windows" ] && binary_name="orbit.exe"

    if [ "$FORCE_INSTALL" = false ] && [ -f "$INSTALL_DIR/$binary_name" ]; then
        warning "Orbit local CLI is already installed at $INSTALL_DIR/$binary_name"
        echo "To upgrade or reinstall:"
        echo "  - Reinstall same/latest: run with --force"
        echo "  - Install specific:      run with --version vX.Y.Z [and optionally --force]"
        exit 0
    fi

    mkdir -p "$INSTALL_DIR"

    local project_id="77960826"
    local artifact_name="orbit-local-${platform}-${arch}.tar.gz"
    local resolved_tag

    if [ -z "$VERSION" ]; then
        echo "Resolving the latest Orbit release..."
        local permalink="https://gitlab.com/api/v4/projects/${project_id}/releases/permalink/latest"
        if command_exists curl; then
            resolved_tag=$(curl -fsSL "$permalink" | sed -nE 's/.*"tag_name":[[:space:]]*"([^"\\]+)".*/\1/p' | head -n1)
        else
            resolved_tag=$(wget -qO- "$permalink" | sed -nE 's/.*"tag_name":[[:space:]]*"([^"\\]+)".*/\1/p' | head -n1)
        fi
        if [ -z "$resolved_tag" ]; then
            error "Failed to resolve the latest release tag from $permalink"
        fi
        VERSION="$resolved_tag"
    fi
    echo "Installing the Orbit local CLI ${VERSION}..."

    # Tarballs are published to the project's Generic Package Registry under
    # '<package>/<version>/<artifact>'. The version segment omits the leading 'v'.
    # PACKAGE_NAME defaults to 'orbit-local'; override to test against a staging
    # package path (e.g. PACKAGE_NAME=orbit-local-dev) without touching production.
    local pkg_name="${PACKAGE_NAME:-orbit-local}"
    local pkg_version="${VERSION#v}"
    local pkg_base="https://gitlab.com/api/v4/projects/${project_id}/packages/generic/${pkg_name}/${pkg_version}"
    local download_url="${pkg_base}/${artifact_name}"
    local checksum_url="${pkg_base}/${artifact_name}.sha256"

    local tarball="${TEMP_DIR}/${artifact_name}"
    echo "Downloading the Orbit local CLI for ${platform}-${arch}..."
    if ! download_file "$download_url" "$tarball"; then
        error "Failed to download the Orbit local CLI from $download_url. Check your internet connection and the version number."
    fi

    verify_checksum "$tarball" "$checksum_url"

    echo "Extracting orbit..."
    if ! tar -xzf "$tarball" -C "$TEMP_DIR"; then
        error "Failed to extract the tarball."
    fi

    local orbit_binary="${TEMP_DIR}/${binary_name}"
    if [ ! -f "$orbit_binary" ]; then
        if [ "$platform" = "windows" ]; then
            orbit_binary=$(find "$TEMP_DIR" -maxdepth 2 -name "$binary_name" -type f | head -n 1)
        else
            orbit_binary=$(find "$TEMP_DIR" -maxdepth 2 -name "$binary_name" -type f -perm -u+x | head -n 1)
        fi
        if [ -z "$orbit_binary" ]; then
            error "orbit binary not found in the extracted files."
        fi
    fi

    local real_binary real_temp
    real_binary=$(realpath "$orbit_binary" 2>/dev/null || readlink -f "$orbit_binary" 2>/dev/null || echo "$orbit_binary")
    real_temp=$(realpath "$TEMP_DIR" 2>/dev/null || readlink -f "$TEMP_DIR" 2>/dev/null || echo "$TEMP_DIR")
    case "$real_binary" in
        "${real_temp}"/*) : ;;
        *) error "Extracted binary resolved outside temp directory: $real_binary" ;;
    esac

    echo "Installing orbit to $INSTALL_DIR..."
    if [ "$platform" = "windows" ]; then
        cp "$orbit_binary" "$INSTALL_DIR/$binary_name"
    elif command_exists install; then
        install -m 0755 "$orbit_binary" "$INSTALL_DIR/$binary_name"
    else
        chmod +x "$orbit_binary"
        mv "$orbit_binary" "$INSTALL_DIR/$binary_name"
    fi

    success "Orbit local CLI has been installed to $INSTALL_DIR/$binary_name"
}

update_path() {
    local platform="$1"

    if [ "$platform" = "windows" ]; then
        # Update Windows user PATH via registry (persistent for CMD, PowerShell, and new terminals)
        local win_dir
        win_dir=$(cygpath -w "$INSTALL_DIR" 2>/dev/null || echo "$INSTALL_DIR")
        local current_path
        current_path=$(reg query "HKCU\Environment" /v PATH 2>/dev/null \
            | grep -i "PATH" | sed 's/.*REG_[^ ]* *//' || echo "")
        if echo "$current_path" | grep -qiF "$win_dir"; then
            echo "Windows PATH already contains $win_dir"
        else
            reg add "HKCU\Environment" /v PATH /t REG_EXPAND_SZ \
                /d "${win_dir};${current_path}" /f >/dev/null
            success "Windows user PATH updated. Open a new terminal to apply."
        fi

        # Also add to ~/.bashrc so Git Bash sessions pick it up immediately
        local bashrc="${HOME}/.bashrc"
        touch "$bashrc"
        if ! grep -Fq "# Added by Orbit local CLI installer" "$bashrc"; then
            printf '\n# Added by Orbit local CLI installer\nexport PATH="%s:$PATH"\n' \
                "$INSTALL_DIR" >> "$bashrc"
            success "Git Bash PATH updated in $bashrc"
        fi
        return
    fi

    local targets=()
    local os_name
    os_name="$(uname -s)"

    if command_exists zsh || [ -x "/bin/zsh" ]; then
        targets+=("${HOME}/.zshrc")
        if [ "$os_name" = "Darwin" ]; then
            targets+=("${HOME}/.zprofile")
        fi
    fi

    if command_exists bash || [ -x "/bin/bash" ]; then
        if [ "$os_name" = "Darwin" ]; then
            targets+=("${HOME}/.bash_profile")
        else
            targets+=("${HOME}/.bashrc")
        fi
    fi

    targets+=("${HOME}/.profile")

    for shell_rc in "${targets[@]}"; do
        echo "Updating PATH in $shell_rc..."
        touch "$shell_rc"

        local home_path="${HOME}/.local/bin"

        if grep -Fq "# Added by Orbit local CLI installer" "$shell_rc"; then
            echo "PATH export already exists in $shell_rc"
            continue
        fi

        if grep -Eq '^[[:space:]]*(export[[:space:]]+)?PATH=.*((\$HOME|\${HOME}|~)/\.local/bin)' "$shell_rc" || \
           grep -Fq "$home_path" "$shell_rc"; then
            echo "PATH export already exists in $shell_rc"
        else
            echo "" >> "$shell_rc"
            echo "# Added by Orbit local CLI installer" >> "$shell_rc"
            echo "export PATH=\"\$HOME/.local/bin:\$PATH\"" >> "$shell_rc"
            success "PATH has been updated in $shell_rc"
        fi
    done
}

ensure_dependencies() {
    if ! command_exists tar; then
        error "Required dependency 'tar' not found. Please install it and re-run the installer."
    fi
    if ! command_exists curl && ! command_exists wget; then
        error "Neither 'curl' nor 'wget' found. Please install one of them and re-run the installer."
    fi
    if ! command_exists awk; then
        error "Required dependency 'awk' not found. Please install it and re-run the installer."
    fi
}

main() {
    echo "=== Orbit local CLI installation ==="
    echo

    local platform
    local arch
    platform=$(detect_os)
    arch=$(detect_arch)

    echo "Detected system: ${platform}-${arch}"
    echo

    ensure_dependencies
    install_orbit "$platform" "$arch"
    [ "$platform" = "windows" ] && install_vcredist
    update_path "$platform"

    echo
    success "Installation complete."
    echo
    echo "To start using orbit in your terminal, run:"
    if [ "$platform" = "windows" ]; then
        echo "  - Git Bash: 'source ~/.bashrc' or open a new Git Bash window."
        echo "  - CMD/PowerShell: open a new terminal (Windows PATH was updated)."
    elif [ "$platform" = "darwin" ]; then
        echo "  - zsh:  'source ~/.zshrc' (login shells: 'source ~/.zprofile')"
        echo "  - bash: 'source ~/.bash_profile'"
    else
        echo "  - zsh:  'source ~/.zshrc'"
        echo "  - bash: 'source ~/.bashrc'"
    fi
    echo "  - Or open a new terminal."
    echo
    if [ "$platform" != "windows" ]; then
        echo "If you use a different shell, add \$HOME/.local/bin to PATH manually."
        echo
    fi
    echo "Then verify the installation with: orbit --version"
}

main
