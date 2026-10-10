#!/usr/bin/env bash
# install_tooling.sh -- one-shot host tooling installer for the nilgiri range.
# Installs, rather than merely checks for, everything `make tooling-check`
# used to assert:
#   * packer + terraform pinned into $BIN_DIR (default ~/bin), checksum-verified
#   * the apt packages the range drives from the host (libvirt/virsh, qemu-img,
#     virt-customize/guestfish, swtpm, jq, sshpass, unzip, curl, python3-venv)
#   * the libvirt storage pool `vm-storage` pointed at /mnt/vm-storage
# The Ansible venv is NOT handled here -- `make venv` owns that.
#
# Idempotent: every step is a no-op when already satisfied, so it is safe to
# re-run. Needs sudo for the apt + libvirt-pool steps only, and only when
# something is actually missing. Pass --check to verify without installing
# (the old tooling-check behaviour).
#
# Version pinning -- both default to the latest OSS release; override to pin:
#   PACKER_VERSION=1.11.2 TERRAFORM_VERSION=1.9.8 make tooling-install

set -euo pipefail

BIN_DIR="${BIN_DIR:-$HOME/bin}"
PACKER_VERSION="${PACKER_VERSION:-latest}"
TERRAFORM_VERSION="${TERRAFORM_VERSION:-latest}"
POOL_NAME="${POOL_NAME:-vm-storage}"
POOL_PATH="${POOL_PATH:-/mnt/vm-storage}"
VENV="${VENV:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.venv}"

# Host packages. qemu-utils -> qemu-img, libguestfs-tools -> virt-customize +
# guestfish (the bake scripts), swtpm* -> the Win11 packer build's TPM 2.0.
APT_PACKAGES=(
    libvirt-clients libvirt-daemon-system virtinst
    qemu-system-x86 qemu-utils
    libguestfs-tools
    swtpm swtpm-tools
    jq sshpass unzip curl
    python3-venv
)

CHECK_ONLY=0
case "${1:-}" in
    --check) CHECK_ONLY=1 ;;
    "") ;;
    *) echo "usage: $0 [--check]" >&2; exit 2 ;;
esac

# Single scratch dir for the downloads, torn down on exit. (A per-function
# RETURN trap would leak: bash applies it globally, not just to that function.)
WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

ok()   { printf '  \033[32mok\033[0m      %s\n' "$*"; }
info() { printf '  ..      %s\n' "$*"; }
fail() { printf '  \033[31mMISSING\033[0m %s\n' "$*" >&2; }

FAILED=0
note_missing() { fail "$1"; FAILED=1; }

# ---- host packages --------------------------------------------------
# The commands the range actually invokes. If every one of these resolves, a
# fully-provisioned host never touches apt at all.
REQUIRED_COMMANDS=(virsh qemu-img virt-customize guestfish swtpm jq sshpass unzip curl)

install_packages() {
    local missing=() cmd
    for cmd in "${REQUIRED_COMMANDS[@]}"; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    # python3-venv ships no binary of its own; probe the module instead.
    python3 -c 'import venv' >/dev/null 2>&1 || missing+=(python3-venv)

    if [ ${#missing[@]} -eq 0 ]; then
        ok "host packages (virsh, qemu-img, virt-customize, swtpm, jq, ...)"
        return 0
    fi
    if [ "$CHECK_ONLY" = 1 ]; then
        note_missing "host packages: ${missing[*]}"
        return 0
    fi

    info "installing host packages (missing: ${missing[*]})"
    sudo apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${APT_PACKAGES[@]}"
    ok "host packages"

    # virsh against qemu:///system needs group membership; without it every
    # later virsh call in the Makefile fails with a permission error.
    local grp
    for grp in libvirt kvm; do
        if getent group "$grp" >/dev/null && ! id -nG | tr ' ' '\n' | grep -qx "$grp"; then
            sudo usermod -aG "$grp" "$USER"
            info "added $USER to group $grp -- log out and back in for it to take effect"
        fi
    done
}

# ---- packer / terraform ---------------------------------------------
hashicorp_arch() {
    case "$(uname -m)" in
        x86_64|amd64)  echo amd64 ;;
        aarch64|arm64) echo arm64 ;;
        *) echo "unsupported architecture: $(uname -m)" >&2; return 1 ;;
    esac
}

# Latest OSS version for a product, straight from the releases API.
latest_version() {
    local product=$1 json
    json=$(curl -fsSL "https://api.releases.hashicorp.com/v1/releases/${product}/latest?license_class=oss") \
        || { echo "could not reach the HashiCorp releases API; pin a version instead" >&2; return 1; }
    python3 -c 'import json,sys; print(json.load(sys.stdin)["version"])' <<<"$json"
}

installed_version() {
    local bin=$1
    [ -x "$bin" ] || return 1
    "$bin" --version 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1
}

# Download <product> <version> and drop the binary into $BIN_DIR, verifying it
# against the release's SHA256SUMS first.
install_hashicorp() {
    local product=$1 version=$2
    local arch url zip tmp
    arch=$(hashicorp_arch)
    url="https://releases.hashicorp.com/${product}/${version}"
    zip="${product}_${version}_linux_${arch}.zip"
    tmp="$WORKDIR/$product"
    rm -rf "$tmp" && mkdir -p "$tmp"

    info "downloading ${product} ${version} (linux_${arch})"
    curl -fsSL -o "$tmp/$zip" "$url/$zip"
    curl -fsSL -o "$tmp/SHA256SUMS" "$url/${product}_${version}_SHA256SUMS"
    (cd "$tmp" && sha256sum --ignore-missing --quiet -c SHA256SUMS) \
        || { echo "checksum mismatch for $zip" >&2; return 1; }

    mkdir -p "$BIN_DIR"
    # python3's zipfile keeps this dependency-free -- unzip may itself be one
    # of the packages we are still installing.
    python3 -c 'import sys,zipfile; zipfile.ZipFile(sys.argv[1]).extract(sys.argv[2], sys.argv[3])' \
        "$tmp/$zip" "$product" "$tmp"
    install -m 0755 "$tmp/$product" "$BIN_DIR/$product"
    ok "${product} ${version} -> ${BIN_DIR}/${product}"
}

ensure_hashicorp() {
    local product=$1 want=$2
    local bin="$BIN_DIR/$product"
    local have
    have=$(installed_version "$bin" || true)

    if [ "$want" = latest ]; then
        # Already present and no pin asked for: leave it alone rather than
        # chasing upstream on every run.
        if [ -n "$have" ]; then ok "${product} ${have} (${bin})"; return 0; fi
        if [ "$CHECK_ONLY" = 1 ]; then note_missing "$product ($bin)"; return 0; fi
        want=$(latest_version "$product")
    else
        if [ "$have" = "$want" ]; then ok "${product} ${have} (${bin})"; return 0; fi
        if [ "$CHECK_ONLY" = 1 ]; then
            note_missing "$product ${want} ($bin has ${have:-nothing})"
            return 0
        fi
    fi

    install_hashicorp "$product" "$want"
}

# ---- ansible venv ----------------------------------------------------
check_venv() {
    if [ -x "$VENV/bin/ansible-playbook" ]; then
        ok "ansible venv ($VENV)"
    else
        note_missing "ansible venv -- run 'make venv'"
    fi
}

# ---- libvirt storage pool -------------------------------------------
ensure_pool() {
    if virsh pool-info "$POOL_NAME" >/dev/null 2>&1; then
        # Defined but inactive after a host reboot is the common case.
        if [ "$(virsh pool-info "$POOL_NAME" | awk '/^State:/{print $2}')" != running ]; then
            [ "$CHECK_ONLY" = 1 ] && { note_missing "pool $POOL_NAME defined but not running"; return 0; }
            virsh pool-start "$POOL_NAME" >/dev/null
        fi
        ok "libvirt pool $POOL_NAME"
        return 0
    fi

    if [ "$CHECK_ONLY" = 1 ]; then
        note_missing "libvirt pool $POOL_NAME"
        return 0
    fi
    if [ ! -d "$POOL_PATH" ]; then
        echo "  $POOL_PATH does not exist -- create or mount the VM storage" >&2
        echo "  filesystem there first, or re-run with POOL_PATH=<dir>." >&2
        FAILED=1
        return 0
    fi

    info "defining libvirt pool $POOL_NAME at $POOL_PATH"
    virsh pool-define-as "$POOL_NAME" dir --target "$POOL_PATH" >/dev/null
    virsh pool-build "$POOL_NAME" >/dev/null 2>&1 || true
    virsh pool-start "$POOL_NAME" >/dev/null
    virsh pool-autostart "$POOL_NAME" >/dev/null
    ok "libvirt pool $POOL_NAME -> $POOL_PATH"
}

# ---- run -------------------------------------------------------------
if [ "$CHECK_ONLY" = 1 ]; then
    echo "Checking host tooling:"
else
    echo "Installing host tooling:"
fi

install_packages
ensure_hashicorp packer "$PACKER_VERSION"
ensure_hashicorp terraform "$TERRAFORM_VERSION"
check_venv
ensure_pool

if [ "$FAILED" != 0 ]; then
    echo
    if [ "$CHECK_ONLY" = 1 ]; then
        echo "Tooling incomplete -- run 'make tooling-install'." >&2
    else
        echo "Tooling incomplete -- see the notes above." >&2
    fi
    exit 1
fi

case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *) echo; echo "Note: $BIN_DIR is not on your PATH. The Makefile calls packer/terraform"
       echo "      by absolute path, so this is only a convenience:"
       echo "          export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac

echo
echo "Tooling ready."
