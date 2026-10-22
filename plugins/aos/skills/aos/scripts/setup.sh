#!/bin/sh
# Prepare a private interpreter, then run the shared cross-platform installer.
set -eu
umask 077
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
prefix="${AOS_HOME:-${XDG_DATA_HOME:-$HOME/.local/share}/aos}"
previous=
for argument do
    if [ "$previous" = prefix ]; then prefix=$argument; previous=; continue; fi
    case "$argument" in
        --prefix) previous=prefix ;;
        --prefix=*) prefix=${argument#--prefix=} ;;
    esac
done
if [ "$previous" = prefix ]; then printf '%s\n' 'Missing --prefix value' >&2; exit 2; fi
mkdir -p "$prefix"
prefix=$(CDPATH= cd -- "$prefix" && pwd)
export AOS_HOME="$prefix"
uv_version=0.13.5
case "$(uname -s)/$(uname -m)" in
    Darwin/arm64)
        uv_target=aarch64-apple-darwin
        uv_digest=743f85035a1996ecb350659e01cd9faea9709829be09b1b4b03c73eaf3c9cd73 ;;
    Linux/x86_64)
        uv_target=x86_64-unknown-linux-gnu
        uv_digest=f68d291c75783122dad58ab77def0e13f3233f358731db5d6d7e8715603b928f ;;
    *)
        printf '%s\n' '{"ok":false,"error":{"code":"UNSUPPORTED_PLATFORM","message":"Supported: macOS arm64 and GNU/Linux x86_64; Windows uses setup.ps1."}}'
        exit 1 ;;
esac
checksum() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | awk '{print $1}'
    else printf '%s\n' 'SHA256 utility is required' >&2; exit 1
    fi
}
uv_dir="$prefix/bootstrap/uv/$uv_version"
uv="$uv_dir/uv"
mkdir -p "$uv_dir"
valid=false
if [ -f "$uv" ] && [ -f "$uv.sha256" ]; then
    if [ "$(checksum "$uv")" = "$(cat "$uv.sha256")" ]; then valid=true; fi
fi
if [ "$valid" != true ]; then
    temp=$(mktemp -d "$prefix/bootstrap/uv-download.XXXXXX")
    trap 'rm -rf "$temp"' EXIT HUP INT TERM
    asset="uv-$uv_target.tar.gz"
    printf '%s\n' "Preparing uv $uv_version" >&2
    if command -v curl >/dev/null 2>&1; then
        curl --fail --silent --show-error --location --retry 3 \
            "https://github.com/astral-sh/uv/releases/download/$uv_version/$asset" -o "$temp/uv.tar.gz"
    else
        printf '%s\n' 'curl is required to download uv' >&2; exit 1
    fi
    if [ "$(checksum "$temp/uv.tar.gz")" != "$uv_digest" ]; then
        printf '%s\n' 'uv archive checksum mismatch' >&2; exit 1
    fi
    tar -xzf "$temp/uv.tar.gz" -C "$temp"
    chmod 755 "$temp/uv-$uv_target/uv"
    checksum "$temp/uv-$uv_target/uv" > "$temp/uv.sha256"
    mv -f "$temp/uv-$uv_target/uv" "$uv"
    mv -f "$temp/uv.sha256" "$uv.sha256"
    rm -rf "$temp"
    trap - EXIT HUP INT TERM
fi
export UV_PYTHON_INSTALL_DIR="$prefix/bootstrap/python"
export UV_CACHE_DIR="$prefix/cache/uv"
export UV_PYTHON_BIN_DIR="$prefix/bootstrap/bin"
"$uv" python install --no-config --no-bin --no-registry 3.11.15 >&2
python=$("$uv" python find --no-config --no-project --managed-python 3.11.15)
exec "$python" -I "$script_dir/aos_manager.py" install --uv "$uv" "$@"
