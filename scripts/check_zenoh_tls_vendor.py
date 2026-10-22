#!/usr/bin/env python3
"""Verify the pinned TLS crate, fixed edit set, and byte-exact patch replay."""

import argparse
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import tempfile

# Resolve this repository's checker explicitly so `python -I` works from any
# directory without accepting an ambient module with the same name.
_source_spec = importlib.util.spec_from_file_location(
    "_aos_zenoh_source_check", Path(__file__).with_name("check_zenoh_vendor.py"),
)
_source = importlib.util.module_from_spec(_source_spec)
_source_spec.loader.exec_module(_source)
digest, hashes, tree_files = _source.digest, _source.hashes, _source.tree_files
LICENSE_HASHES = _source.LICENSE_HASHES

CRATE = "zenoh-link-tls"
VERSION = "1.10.1"
COMMIT = "1211779c3647f5a96713dade452c546a07823580"
ARCHIVE_SHA256 = "cce65fd71b4631821a85058542e28faac0151af6c53cd61f2dfafc7e16c4d6c4"
PATCH_PATH = "upstream/patches/zenoh-link-tls-1.10.1-private-roots.patch"
MODIFIED_FILES = frozenset({"src/lib.rs", "src/utils.rs", "src/unicast.rs"})
ADDED_FILES = frozenset({"AOS-PATCH.md", "LICENSE", "NOTICE.md"})
MANIFEST_PATH = "upstream/zenoh-link-tls-1.10.1-vendor-manifest.json"


def find_archive(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit.resolve(strict=True)
    configured = os.environ.get("AOS_ZENOH_TLS_CRATE_ARCHIVE")
    if configured:
        return Path(configured).resolve(strict=True)
    cargo = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo")))
    for path in sorted((cargo / "registry/cache").glob(f"*/{CRATE}-{VERSION}.crate")):
        if digest(path.read_bytes()) == ARCHIVE_SHA256:
            return path.resolve()
    raise ValueError("Pinned original zenoh-link-tls-1.10.1.crate is required; pass --archive or AOS_ZENOH_TLS_CRATE_ARCHIVE. No download is performed.")


def original_files(path: Path) -> dict[str, bytes]:
    if digest(path.read_bytes()) != ARCHIVE_SHA256:
        raise ValueError("TLS original archive hash mismatch")
    files = {}
    prefix = f"{CRATE}-{VERSION}/"
    with tarfile.open(path) as archive:
        for member in archive.getmembers():
            if member.isdir():
                continue
            if not member.isfile() or not member.name.startswith(prefix):
                raise ValueError(f"Unexpected TLS archive member: {member.name}")
            name = member.name[len(prefix):]
            relative = PurePosixPath(name)
            if relative.is_absolute() or ".." in relative.parts or "\\" in name or ":" in name or name in files:
                raise ValueError(f"Unsafe or duplicate TLS archive member: {name}")
            files[name] = archive.extractfile(member).read()
    return files


def check_change_set(manifest: dict, final: dict[str, str]) -> None:
    for field, expected in {
        "crate": CRATE, "version": VERSION, "upstream_commit": COMMIT,
        "archive_sha256": ARCHIVE_SHA256, "license_source_commit": COMMIT,
        "repository": "https://github.com/eclipse-zenoh/zenoh",
        "selected_license": "Apache-2.0", "upstream_license": "EPL-2.0 OR Apache-2.0",
    }.items():
        if manifest.get(field) != expected:
            raise ValueError(f"TLS fixed source field mismatch: {field}")
    if manifest["changed_original_files"] != sorted(MODIFIED_FILES) or manifest["added_files"] != sorted(ADDED_FILES):
        raise ValueError("TLS declared change set mismatch")
    original = manifest["original_files"]
    if original.keys() - final.keys() or final.keys() - original.keys() != ADDED_FILES:
        raise ValueError("TLS actual source file set mismatch")
    changed = {name for name in original if original[name] != final[name]}
    if changed != MODIFIED_FILES:
        raise ValueError(f"TLS actual modified original files mismatch: {sorted(changed)}")
    if any(final[name] != expected for name, expected in LICENSE_HASHES.items()):
        raise ValueError("TLS fixed upstream license hash mismatch")
    if final != manifest["final_files"]:
        raise ValueError("TLS final source hashes mismatch")


def verify(root: Path, archive: Path) -> dict:
    manifest = json.loads((root / MANIFEST_PATH).read_text(encoding="utf-8"))
    final = hashes(tree_files(root / "vendor" / CRATE))
    check_change_set(manifest, final)
    original = original_files(archive)
    if hashes(original) != manifest["original_files"]:
        raise ValueError("TLS original archive contents mismatch")
    patch = root / PATCH_PATH
    if manifest["source_patch"]["path"] != PATCH_PATH or digest(patch.read_bytes()) != manifest["source_patch"]["sha256"]:
        raise ValueError("TLS source patch identity mismatch")
    with tempfile.TemporaryDirectory(prefix="aos-tls-source-check-") as temporary:
        reproduction = Path(temporary)
        for name, data in original.items():
            destination = reproduction / "vendor" / CRATE / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
        environment = os.environ.copy()
        for variable in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"):
            environment.pop(variable, None)
        for check in (["--check"], []):
            result = subprocess.run(["git", "-c", "core.autocrlf=false", "apply", "--whitespace=nowarn", *check, str(patch.resolve())], cwd=reproduction, env=environment, capture_output=True, text=True)
            if result.returncode:
                raise ValueError(f"TLS patch failed to apply: {result.stderr.strip()}")
        expected = {f"vendor/{CRATE}/{name}": sha for name, sha in final.items()}
        if hashes(tree_files(reproduction)) != expected:
            raise ValueError("TLS patch does not reproduce the complete final tree")
    return {"crate": CRATE, "version": VERSION, "upstream_commit": COMMIT, "archive_sha256": ARCHIVE_SHA256, "source_patch_sha256": digest(patch.read_bytes()), "original_files": len(original), "final_files": len(final), "changed_original_files": sorted(MODIFIED_FILES), "added_files": sorted(ADDED_FILES), "patch_reproduces_final_tree": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        result = verify(Path(__file__).resolve().parents[1], find_archive(args.archive))
    except (OSError, ValueError, KeyError, tarfile.TarError) as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(result, indent=2) if args.json else "Zenoh TLS: fixed change set and complete patch replay verified")


if __name__ == "__main__":
    main()
