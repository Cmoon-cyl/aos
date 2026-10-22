#!/bin/sh
# Install the released AOS agent distribution. No Python/Rust/Node.js required.
set -eu
umask 077
version="${AOS_AGENT_VERSION:-0.1.1}"
if ! printf '%s\n' "$version" | awk '/^[0-9]+\.[0-9]+\.[0-9]+$/ {ok=1} END {exit !ok}'; then
    printf '%s\n' 'Invalid AOS_AGENT_VERSION' >&2; exit 2
fi
if [ "$#" -gt 0 ] && [ "$1" = --help ]; then
    printf '%s\n' 'Install AOS: install.sh [--prefix PATH] [--agent codex|claude|trae] [--no-path] [--non-interactive] [--json]'
    exit 0
fi
temp=$(mktemp -d)
trap 'rm -rf "$temp"' EXIT HUP INT TERM
asset="aos-agent-$version.tar.gz"
tag="aos-agent-v$version"
if command -v curl >/dev/null 2>&1; then
    for name in "$asset" "$asset.sha256"; do
        curl --fail --silent --show-error --location --retry 3 \
            "https://github.com/Cmoon-cyl/aos/releases/download/$tag/$name" -o "$temp/$name"
    done
else
    printf '%s\n' 'curl is required to download AOS.' >&2; exit 1
fi
expected=$(awk '{print $1}' "$temp/$asset.sha256")
if command -v sha256sum >/dev/null 2>&1; then actual=$(sha256sum "$temp/$asset" | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then actual=$(shasum -a 256 "$temp/$asset" | awk '{print $1}')
else printf '%s\n' 'SHA256 utility is required.' >&2; exit 1
fi
if [ "$actual" != "$expected" ]; then printf '%s\n' 'AOS download checksum mismatch.' >&2; exit 1; fi
tar -xzf "$temp/$asset" -C "$temp"
sh "$temp/aos-agent/skills/aos/scripts/setup.sh" "$@"
