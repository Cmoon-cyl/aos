#!/usr/bin/env python3
"""Assemble a local AOS platform bundle from recorded native artifacts.

Requires Python 3.11+, its standard library, and Git for source revision
metadata. Does not build, test, download, publish, or rewrite acceptance status.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import tomllib
import zipfile


PLATFORMS = {
    "macos-arm64": ("aarch64-apple-darwin", "aos", r"macosx_\d+_\d+_arm64"),
    "linux-x86_64": ("x86_64-unknown-linux-gnu", "aos", r"linux_x86_64"),
    "windows-x86_64": ("x86_64-pc-windows-msvc", "aos.exe", r"win_amd64"),
}
# Official Zenoh 1.10.1 standalone assets used by native acceptance. Shipping
# a router is explicit; the archive must match the platform's fixed digest.
ROUTER_ASSETS = {
    "macos-arm64": ("zenoh-1.10.1-aarch64-apple-darwin-standalone.zip",
                    "a27a177014fa81bc1bdb9b824373a2a8bce0e0a33c40e6c189d6fc9414b57b8a"),
    "linux-x86_64": ("zenoh-1.10.1-x86_64-unknown-linux-gnu-standalone.zip",
                     "512ac81438fd995ca2db02f75dd83afa048b6a11630ee10e0cb4dc423c39a5e9"),
    "windows-x86_64": ("zenoh-1.10.1-x86_64-pc-windows-msvc-standalone.zip",
                       "2da28ac4e700de3c43db38417160b0ca971bd336861dd9d86781d042ae24fa40"),
}
TREES = ("config", "examples", "interfaces", "docs", "upstream")
VENDORED_LIBRARIES = ("zenoh", "zenoh-link-tls")
VENDOR_RECORDS = ("LICENSE", "NOTICE.md", "AOS-PATCH.md")
# Public installation flows, not the repository's build/test/administration tools.
# Both installers locate these inputs relative to their own scripts/ directory.
INSTALLERS = {
    "scripts/install_demo_package.py": (
        "examples/launch/provider.py", "examples/launch/consumer.py",
        "examples/launch/Label.json", "examples/launch/Status.json",
    ),
    "scripts/install_memory_package.py": (
        "examples/memory_server/provider.py", "examples/memory_server/consumer.py",
        "examples/memory_server/memory_store.py",
        "examples/memory_server/request_observer.py",
        "examples/memory_server/interfaces/Put.json",
        "examples/memory_server/interfaces/Get.json",
        "examples/memory_server/interfaces/List.json",
        "examples/memory_server/interfaces/Status.json",
        "examples/memory_server/interfaces/Export.json",
        "examples/memory_server/interfaces/RequestTelemetry.json",
    ),
}
def verify_binding_evidence(root: Path, provenance: dict) -> None:
    """Validate private build inputs without publishing their records."""
    bindings = provenance.get("installed_test_bindings")
    if bindings is None:
        return
    if not isinstance(bindings, dict):
        raise ValueError("installed_test_bindings must be an object")
    for name in ("record", "manifest"):
        recorded_artifact(root, {
            "path": bindings[name], "sha256": bindings[name + "_sha256"],
        }, root / "installed-test-bindings" / (name + ".json"))


def sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def copy_file(source: Path, destination: Path) -> None:
    if source.is_symlink() or not source.is_file():
        raise ValueError(f"Expected an ordinary file: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def copy_tree(source: Path, destination: Path, *, skip_generated: bool = True) -> None:
    if source.is_symlink() or not source.is_dir():
        raise ValueError(f"Expected an ordinary directory: {source}")
    for path in sorted(source.rglob("*")):
        if skip_generated and (
            {"__pycache__", "evidence", "installed-test-bindings", "logs"}
            .intersection(path.relative_to(source).parts)
            or path.name == ".DS_Store"
        ):
            continue
        if path.is_symlink():
            raise ValueError(f"Symlinks are not accepted in delivery inputs: {path}")
        if path.is_dir() or (skip_generated and path.suffix in {".pyc", ".pyo"}):
            continue
        copy_file(path, destination / path.relative_to(source))


def recorded_artifact(artifacts: Path, record: dict, expected: Path) -> Path:
    relative = PurePosixPath(record["path"])
    if (
        relative.is_absolute() or ".." in relative.parts
        or "\\" in str(relative) or ":" in str(relative)
    ):
        raise ValueError(f"Invalid provenance artifact path: {relative}")
    path = artifacts.joinpath(*relative.parts)
    if path.resolve() != expected.resolve() or path.is_symlink():
        raise ValueError(f"Unexpected provenance artifact path: {path}")
    if not path.is_file() or sha256(path) != record["sha256"]:
        raise ValueError(f"Artifact missing or SHA256 mismatch: {path}")
    return path


def git_output(source: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(source), *arguments],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def bundle_router(artifacts: Path, provenance: dict, platform: str, bundle: Path) -> dict:
    """Copy the pinned upstream distribution, including Windows DLLs and deps."""
    name, digest = ROUTER_ASSETS[platform]
    record = provenance["zenohd"]
    if record["version"] != "1.10.1" or any(
        record[key] != digest for key in ("expected_sha256", "actual_sha256")
    ):
        raise ValueError("Router provenance does not match the pinned platform archive")
    archive = recorded_artifact(artifacts, {
        "path": record["archive"], "sha256": digest,
    }, artifacts / "zenohd" / name)
    executable = "zenohd.exe" if platform == "windows-x86_64" else "zenohd"
    recorded_artifact(artifacts, {
        "path": record["executable"], "sha256": record["executable_sha256"],
    }, artifacts / "zenohd" / "unpacked" / executable)
    destination = bundle / "router"
    destination.mkdir()
    seen = set()
    with zipfile.ZipFile(archive) as zipped:
        for member in zipped.infolist():
            relative = PurePosixPath(member.filename)
            key = relative.as_posix().casefold()
            if (relative.is_absolute() or ".." in relative.parts
                    or not relative.parts or "\\" in member.filename
                    or ":" in member.filename or key in seen
                    or stat.S_ISLNK(member.external_attr >> 16)):
                raise ValueError(f"Unsafe or duplicate router archive member: {member.filename}")
            seen.add(key)
            path = destination.joinpath(*relative.parts)
            if member.is_dir():
                path.mkdir(parents=True, exist_ok=True)
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            with zipped.open(member) as source, path.open("xb") as target:
                shutil.copyfileobj(source, target)
    binary = destination / executable
    if not binary.is_file() or sha256(binary) != record["executable_sha256"]:
        raise ValueError("Bundled router differs from the native-tested executable")
    binary.chmod(0o755)
    return {
        "version": "1.10.1", "bundled": True,
        "executable": "router/" + executable,
        "executable_sha256": record["executable_sha256"],
        "upstream_archive": name, "upstream_archive_sha256": digest,
        "upstream_url": "https://github.com/eclipse-zenoh/zenoh/releases/download/1.10.1/" + name,
        "instructions": "docs/install.md",
    }


def package(args: argparse.Namespace) -> Path:
    source = args.source_repo.resolve()
    artifacts = args.artifacts.resolve()
    output = args.output.resolve()
    if any(output.is_relative_to(source / directory) for directory in TREES) or (
        output.is_relative_to(artifacts)
    ):
        raise ValueError("Output must be outside the copied source trees and artifact inputs")
    if not re.fullmatch(r"[0-9a-f]{40}", args.source_sha):
        raise ValueError("--source-sha must be a full lowercase 40-character Git SHA")
    target, cli_name, platform_tag = PLATFORMS[args.platform]
    provenance = json.loads((artifacts / "provenance.json").read_text(encoding="utf-8"))
    if provenance["source_sha"] != args.source_sha:
        raise ValueError("Source SHA does not match artifact provenance")
    artifact_build_source_sha = provenance.get("artifact_build_source_sha", args.source_sha)
    if not isinstance(artifact_build_source_sha, str) or not re.fullmatch(
        r"[0-9a-f]{40}", artifact_build_source_sha,
    ):
        raise ValueError("Artifact build source SHA must be a full lowercase 40-character Git SHA")
    if provenance["target"] != target:
        raise ValueError(f"Artifact provenance target must be {target}")
    if not isinstance(provenance.get("status"), str) or not provenance["status"]:
        raise ValueError("Artifact provenance must retain a nonempty acceptance status")
    packaging_revision = git_output(source, "rev-parse", "HEAD")
    tracked_changes = git_output(source, "status", "--porcelain", "--untracked-files=no")
    version = tomllib.loads((source / "Cargo.toml").read_text(encoding="utf-8"))[
        "workspace"
    ]["package"]["version"]
    if not re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z.+-]*", version):
        raise ValueError("Invalid workspace version")
    wheels = sorted((artifacts / "wheels").glob("aos_sdk-*.whl"))
    pattern = rf"aos_sdk-{re.escape(version)}-cp311-abi3-{platform_tag}\.whl"
    if len(wheels) != 1 or not re.fullmatch(pattern, wheels[0].name):
        raise ValueError(f"Expected exactly one matching cp311-abi3 SDK wheel: {wheels}")
    wheel = recorded_artifact(artifacts, provenance["wheel"], wheels[0])
    cli = recorded_artifact(artifacts, provenance["cli"], artifacts / "cli" / cli_name)
    verify_binding_evidence(artifacts, provenance)
    for required in (
        "README.md", "docs/install.md", "docs/known-limitations.md",
        "upstream/source-map.json", "config/zenoh-session.json5",
        "config/zenoh-router.json5", "config/zenoh-router-windows.json5",
    ):
        if not (source / required).is_file():
            raise ValueError(f"Missing required delivery file: {required}")
    licenses = source / "upstream" / "licenses"
    if not licenses.is_dir() or not any(licenses.iterdir()):
        raise ValueError("Missing upstream license records")
    for installer, dependencies in INSTALLERS.items():
        for relative in (installer, *dependencies):
            path = source / relative
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"Missing ordinary installer or required resource: {relative}")
    for library in VENDORED_LIBRARIES:
        for filename in VENDOR_RECORDS:
            path = source / "vendor" / library / filename
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"Missing vendored license or modification record: {path}")
    name = f"aos-{version}-{args.platform}-{args.source_sha[:12]}"
    suffix = ".zip" if args.platform == "windows-x86_64" else ".tar.gz"
    output.mkdir(parents=True, exist_ok=True)
    archive = output / (name + suffix)
    checksum = output / (archive.name + ".sha256")
    if archive.exists() or checksum.exists():
        raise ValueError(f"Refusing to overwrite output: {archive}")
    with tempfile.TemporaryDirectory(prefix=".aos-package-", dir=output) as temporary:
        staging = Path(temporary)
        bundle = staging / name
        bundle.mkdir()
        for directory in TREES:
            copy_tree(source / directory, bundle / directory)
        for installer in INSTALLERS:
            copy_file(source / installer, bundle / installer)
        for library in VENDORED_LIBRARIES:
            for filename in VENDOR_RECORDS:
                copy_file(
                    source / "vendor" / library / filename,
                    bundle / "upstream" / "licenses" / (library + "-1.10.1") / filename,
                )
        copy_file(source / "README.md", bundle / "README.md")
        for filename in ("CHANGELOG.md", "THIRD_PARTY_NOTICES.md", "CONTRIBUTING.md"):
            if (source / filename).is_file():
                copy_file(source / filename, bundle / filename)
        top_licenses = sorted({
            path for pattern in ("LICENSE*", "NOTICE*") for path in source.glob(pattern)
            if path.is_file()
        })
        for license_file in top_licenses:
            copy_file(license_file, bundle / license_file.name)
        copy_file(cli, bundle / "bin" / cli_name)
        (bundle / "bin" / cli_name).chmod(0o755)
        copy_file(wheel, bundle / "wheels" / wheel.name)
        router = {"version": "1.10.1", "bundled": False, "instructions": "docs/install.md"}
        if getattr(args, "bundle_router", False):
            router = bundle_router(artifacts, provenance, args.platform, bundle)
        for path, record in (
            (bundle / "bin" / cli_name, provenance["cli"]),
            (bundle / "wheels" / wheel.name, provenance["wheel"]),
        ):
            if sha256(path) != record["sha256"]:
                raise ValueError(f"Artifact changed while being packaged: {path.name}")
        verify_binding_evidence(artifacts, provenance)
        files = {
            path.relative_to(bundle).as_posix(): {
                "sha256": sha256(path), "bytes": path.stat().st_size,
            }
            for path in sorted(bundle.rglob("*")) if path.is_file()
        }
        manifest = {
            "format_version": 1,
            "name": "aos",
            "version": version,
            "platform": args.platform,
            "target": target,
            "python_acceptance_scope": "CPython 3.11",
            "source_sha": artifact_build_source_sha,
            "validation_source_sha": args.source_sha,
            "packaging_revision": packaging_revision,
            "packaging_tracked_changes": tracked_changes,
            "packaging_script_sha256": sha256(Path(__file__)),
            "installed_package_installers": list(INSTALLERS),
            "acceptance_status": provenance["status"],
            "packaging_runs_acceptance": False,
            "zenohd": router,
            "top_level_licenses": [path.name for path in top_licenses],
            "files_excludes": ["manifest.json"],
            "files": files,
        }
        (bundle / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
        )
        staged_archive = staging / archive.name
        if suffix == ".zip":
            with zipfile.ZipFile(staged_archive, "w", zipfile.ZIP_DEFLATED) as zipped:
                for path in sorted(bundle.rglob("*")):
                    if path.is_file():
                        zipped.write(path, path.relative_to(staging).as_posix())
        else:
            def executable_cli(member):
                # A Windows staging filesystem cannot retain POSIX execute bits.
                # The archive is for the selected target, not the packaging host.
                if member.name in {f"{name}/bin/{cli_name}", f"{name}/router/zenohd"}:
                    member.mode = 0o755
                return member

            with tarfile.open(staged_archive, "w:gz", format=tarfile.PAX_FORMAT) as tar:
                tar.add(bundle, arcname=name, filter=executable_cli)
        digest = sha256(staged_archive)
        # Exclusive creation prevents a concurrent packaging run overwriting a file.
        with archive.open("xb") as destination, staged_archive.open("rb") as packed:
            shutil.copyfileobj(packed, destination)
        with checksum.open("x", encoding="utf-8") as digest_file:
            digest_file.write(f"{digest}  {archive.name}\n")
    return archive


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-repo", required=True, type=Path)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--platform", required=True, choices=PLATFORMS)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--bundle-router", action="store_true",
                        help="Include the pinned, native-tested standalone router and its dependencies")
    args = parser.parse_args()
    try:
        archive = package(args)
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Packaging failed: {error}\n")
    print(archive)
    print(f"Archive SHA256: {sha256(archive)}")
    print("Packaging complete; original acceptance status is unchanged.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
