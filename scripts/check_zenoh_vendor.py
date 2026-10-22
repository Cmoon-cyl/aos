#!/usr/bin/env python3
"""Verify the pinned Zenoh archive, exact change set and complete patch replay."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import tempfile


VERSION = "1.10.1"
UPSTREAM_COMMIT = "1211779c3647f5a96713dade452c546a07823580"
ARCHIVE_SHA256 = "453ba5d28a1197653aae4bb024fd74a9d5c7051d5a19a77f3a43d83f40c22584"
PATCH_PATH = "upstream/patches/zenoh-1.10.1-liveliness-history.patch"
MODIFIED_FILES = frozenset({
    "src/api/mod.rs", "src/api/session.rs", "src/api/subscriber.rs",
    "src/net/primitives/demux.rs",
    "src/net/routing/dispatcher/face.rs",
    "src/net/routing/dispatcher/interests.rs",
    "src/net/routing/dispatcher/mod.rs",
    "src/net/routing/dispatcher/tables.rs",
    "src/net/routing/gateway.rs",
    "src/net/routing/hat/router/mod.rs",
    "src/net/routing/hat/router/pubsub.rs",
    "src/net/routing/hat/router/queries.rs",
    "src/net/routing/hat/router/token.rs",
})
ADDED_FILES = frozenset({
    "AOS-PATCH.md", "LICENSE", "NOTICE.md", "src/api/liveliness_history.rs",
    "src/net/routing/dispatcher/declare_outbox.rs",
})
# These two files are copied from the repository root at UPSTREAM_COMMIT.
LICENSE_HASHES = {
    "LICENSE": "01a44774f7b1a453595c7c6d7f7308284ba6a1059dc49e14dad6647e1d44a338",
    "NOTICE.md": "02e4bf9ebd5cd19d726e8158659b60caf2c0e5077a2d330d41e506f50570f3b1",
}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hashes(files: dict[str, bytes]) -> dict[str, str]:
    return {name: digest(data) for name, data in files.items()}


def tree_files(root: Path) -> dict[str, bytes]:
    files = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"Zenoh tree contains a symlink: {path}")
        if path.is_file():
            files[path.relative_to(root).as_posix()] = path.read_bytes()
    return files


def find_archive(explicit: Path | None = None) -> Path:
    if explicit is not None:
        return explicit.resolve(strict=True)
    configured = os.environ.get("AOS_ZENOH_CRATE_ARCHIVE")
    if configured:
        return Path(configured).resolve(strict=True)
    cargo = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo")))
    for path in sorted((cargo / "registry/cache").glob(f"*/zenoh-{VERSION}.crate")):
        if digest(path.read_bytes()) == ARCHIVE_SHA256:
            return path.resolve()
    raise ValueError(
        "The pinned original zenoh-1.10.1.crate is required. Pass --archive or set "
        "AOS_ZENOH_CRATE_ARCHIVE; this verifier does not download source."
    )


def original_files(archive_path: Path) -> dict[str, bytes]:
    if digest(archive_path.read_bytes()) != ARCHIVE_SHA256:
        raise ValueError("Zenoh original archive hash mismatch")
    files = {}
    prefix = f"zenoh-{VERSION}/"
    with tarfile.open(archive_path) as archive:
        for member in archive.getmembers():
            if member.isdir():
                continue
            if not member.isfile() or not member.name.startswith(prefix):
                raise ValueError(f"Unexpected Zenoh archive member: {member.name}")
            name = member.name[len(prefix):]
            relative = PurePosixPath(name)
            if (relative.is_absolute() or ".." in relative.parts
                    or "\\" in name or ":" in name or name in files):
                raise ValueError(f"Unsafe or duplicate Zenoh archive member: {name}")
            files[name] = archive.extractfile(member).read()
    return files


def check_change_set(manifest: dict, final: dict[str, str]) -> None:
    for key, expected in {
        "crate": "zenoh", "version": VERSION, "upstream_commit": UPSTREAM_COMMIT,
        "archive_sha256": ARCHIVE_SHA256, "license_source_commit": UPSTREAM_COMMIT,
        "repository": "https://github.com/eclipse-zenoh/zenoh",
        "selected_license": "Apache-2.0", "upstream_license": "EPL-2.0 OR Apache-2.0",
    }.items():
        if manifest.get(key) != expected:
            raise ValueError(f"Zenoh fixed source field mismatch: {key}")
    if manifest["changed_original_files"] != sorted(MODIFIED_FILES):
        raise ValueError("Zenoh declared modified original file set mismatch")
    if manifest["added_files"] != sorted(ADDED_FILES):
        raise ValueError("Zenoh declared added file set mismatch")
    original = manifest["original_files"]
    if original.keys() - final.keys():
        raise ValueError("Zenoh original files were removed")
    if final.keys() - original.keys() != ADDED_FILES:
        raise ValueError("Zenoh actual added file set mismatch")
    changed = {name for name in original if original[name] != final[name]}
    if changed != MODIFIED_FILES:
        raise ValueError(f"Zenoh actual modified original file set mismatch: {sorted(changed)}")
    for name, expected in LICENSE_HASHES.items():
        if final[name] != expected:
            raise ValueError(f"Zenoh fixed upstream license hash mismatch: {name}")
    if final != manifest["final_files"]:
        changed = sorted(
            name for name in final.keys() | manifest["final_files"].keys()
            if final.get(name) != manifest["final_files"].get(name)
        )
        raise ValueError(f"Zenoh vendor hash/set mismatch: {changed}")


def verify(root: Path, archive_path: Path) -> dict:
    manifest = json.loads(
        (root / "upstream/zenoh-1.10.1-vendor-manifest.json").read_text(encoding="utf-8")
    )
    final = hashes(tree_files(root / "vendor/zenoh"))
    check_change_set(manifest, final)
    original = original_files(archive_path)
    if hashes(original) != manifest["original_files"]:
        raise ValueError("Zenoh original archive contents mismatch")
    patch = root / PATCH_PATH
    if manifest["source_patch"]["path"] != PATCH_PATH:
        raise ValueError("Zenoh source patch path mismatch")
    patch_digest = digest(patch.read_bytes())
    if patch_digest != manifest["source_patch"]["sha256"]:
        raise ValueError("Zenoh source patch hash mismatch")
    with tempfile.TemporaryDirectory(prefix="aos-zenoh-source-check-") as temporary:
        reproduction = Path(temporary)
        for name, data in original.items():
            destination = reproduction / "vendor/zenoh" / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)
        environment = os.environ.copy()
        for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"):
            environment.pop(name, None)
        command = ["git", "-c", "core.autocrlf=false", "apply", "--whitespace=nowarn"]
        for check in (["--check"], []):
            completed = subprocess.run(
                [*command, *check, str(patch.resolve())], cwd=reproduction,
                env=environment, text=True, capture_output=True,
            )
            if completed.returncode:
                raise ValueError(f"Zenoh source patch does not apply: {completed.stderr.strip()}")
        # Include the whole temporary root so changes outside vendor/zenoh also
        # fail; the patch must reproduce every source/license/notice byte.
        reproduced = hashes(tree_files(reproduction))
        expected = {f"vendor/zenoh/{name}": value for name, value in final.items()}
        if reproduced != expected:
            raise ValueError("Zenoh source patch does not reproduce the complete final tree")
    return {
        "version": VERSION, "upstream_commit": UPSTREAM_COMMIT,
        "archive_sha256": ARCHIVE_SHA256, "source_patch_sha256": patch_digest,
        "original_files": len(original), "final_files": len(final),
        "changed_original_files": sorted(MODIFIED_FILES), "added_files": sorted(ADDED_FILES),
        "patch_reproduces_final_tree": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--archive", type=Path,
        help="original zenoh-1.10.1.crate; otherwise use AOS_ZENOH_CRATE_ARCHIVE or Cargo cache",
    )
    parser.add_argument("--json", action="store_true", help="print the verified source record")
    args = parser.parse_args()
    try:
        result = verify(Path(__file__).resolve().parents[1], find_archive(args.archive))
    except (OSError, ValueError, KeyError, tarfile.TarError) as error:
        raise SystemExit(str(error)) from error
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(
            f"Zenoh {VERSION}: {result['original_files']} original / "
            f"{result['final_files']} final files; fixed change set and complete patch replay verified"
        )


if __name__ == "__main__":
    main()
