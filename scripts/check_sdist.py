#!/usr/bin/env python3
"""Verify a clean checkout's sdist, build it offline, and import its wheel.

Run with CPython 3.11, the pinned Rust toolchain and maturin already installed:
    python -I scripts/check_sdist.py --output /absolute/new/evidence-directory

Cargo dependencies must already be cached. The fresh build target defaults to
the output directory; --build-target-dir may select an exclusively owned cache.
This check does not publish artifacts or claim full SDK functional acceptance.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import platform
import shutil
import subprocess
import sys
import tarfile
import time
import tomllib
import zipfile

_spec = importlib.util.spec_from_file_location(
    "aos_native_acceptance", Path(__file__).with_name("native_acceptance.py")
)
_native = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_native)


def source_files(root: Path) -> dict[str, dict[str, object]]:
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).decode().split("\0")
    result = {}
    for relative in filter(None, paths):
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Source distribution requires ordinary tracked files: {relative}")
        result[relative] = {
            "sha256": _native.sha256(path),
            "executable": bool(path.stat().st_mode & 0o111),
        }
    if not result:
        raise ValueError("No tracked source files")
    return result


def unpack_verified(
    archive: Path, destination: Path | None, expected: dict[str, dict[str, object]],
    *, verify_modes: bool = True,
) -> Path | None:
    """Reject changed, missing, additional, linked, or ambiguous source entries."""
    entries = {}
    top = None
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle.getmembers():
            path = PurePosixPath(member.name)
            if (
                path.is_absolute() or ".." in path.parts or "\\" in member.name
                or ":" in member.name or not path.parts
            ):
                raise ValueError(f"Unsafe sdist path: {member.name!r}")
            if top is None:
                top = path.parts[0]
            if path.parts[0] != top:
                raise ValueError("Source distribution has multiple roots")
            if member.isdir():
                continue
            if not member.isfile() or len(path.parts) < 2:
                raise ValueError(f"Source distribution contains non-file: {member.name}")
            relative = "/".join(path.parts[1:])
            if relative in entries:
                raise ValueError(f"Duplicate sdist file: {relative}")
            entries[relative] = member
        if set(entries) != set(expected) | {"PKG-INFO"}:
            raise ValueError(
                "Source distribution file mismatch: "
                f"missing={sorted(set(expected) - set(entries))}; "
                f"unexpected={sorted(set(entries) - set(expected) - {'PKG-INFO'})}"
            )
        # Validate every input before extracting any files.
        for relative, description in expected.items():
            member = entries[relative]
            content = bundle.extractfile(member).read()
            if hashlib.sha256(content).hexdigest() != description["sha256"]:
                raise ValueError(f"Changed source file in sdist: {relative}")
            if verify_modes and bool(member.mode & 0o111) != description["executable"]:
                raise ValueError(f"Changed executable mode in sdist: {relative}")
        if destination is None:
            return None
        root = destination / top
        for relative, member in entries.items():
            output = root / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            with bundle.extractfile(member) as source, output.open("xb") as target:
                target.write(source.read())
            output.chmod(0o755 if member.mode & 0o111 else 0o644)
    return root


def restore_source_modes(
    archive: Path, expected: dict[str, dict[str, object]], original: Path,
) -> dict[str, object]:
    """Restore tracked execute bits lost by maturin's git sdist generator.

    First validate paths, membership and every source byte without extraction.
    Preserve the generator's original archive and record each metadata change;
    the caller still performs the strict mode check on the final archive.
    """
    unpack_verified(archive, None, expected, verify_modes=False)
    changes = []
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle.getmembers():
            relative = "/".join(PurePosixPath(member.name).parts[1:])
            if member.isfile() and relative in expected:
                executable = expected[relative]["executable"]
                if bool(member.mode & 0o111) != executable:
                    changes.append({
                        "path": relative, "from": member.mode,
                        "to": 0o755 if executable else 0o644,
                    })
    record = {"generator_sha256": _native.sha256(archive), "changes": changes}
    if changes:
        original.parent.mkdir(parents=True, exist_ok=True)
        if original.exists():
            raise FileExistsError(original)
        shutil.copy2(archive, original)
        modes = {change["path"]: change["to"] for change in changes}
        temporary = archive.with_name(archive.name + ".modes.tmp")
        try:
            with tarfile.open(original, "r:gz") as source, tarfile.open(temporary, "x:gz") as target:
                for member in source.getmembers():
                    relative = "/".join(PurePosixPath(member.name).parts[1:])
                    if relative in modes:
                        member.mode = modes[relative]
                    target.addfile(member, source.extractfile(member) if member.isfile() else None)
            # Do not replace the artifact unless the repaired archive passes
            # the same strict validation used for its subsequent extraction.
            unpack_verified(temporary, None, expected)
            temporary.replace(archive)
        finally:
            temporary.unlink(missing_ok=True)
        record["generator_archive"] = str(original)
    record["final_sha256"] = _native.sha256(archive)
    return record


IMPORT_PROBE = r"""
import hashlib, importlib.machinery, json, pathlib, sys, sysconfig
import aos_sdk
from aos_sdk import _native
native = pathlib.Path(_native.__file__).resolve()
site = pathlib.Path(sysconfig.get_paths()["platlib"]).resolve()
assert sys.flags.isolated
assert native.is_relative_to(site / "aos_sdk")
assert pathlib.Path(aos_sdk.__file__).resolve().parent == native.parent
assert isinstance(_native.__loader__, importlib.machinery.ExtensionFileLoader)
digest = hashlib.sha256(native.read_bytes()).hexdigest()
assert digest == sys.argv[1], (digest, sys.argv[1])
print(json.dumps({"isolated": True, "native_path": str(native), "native_sha256": digest}))
"""


class SdistAcceptance(_native.Acceptance):
    def execute_sdist(self, build_target: Path | None) -> None:
        if platform.python_implementation() != "CPython" or sys.version_info[:2] != (3, 11):
            raise ValueError("Source acceptance requires CPython 3.11")
        if platform.system() not in ("Darwin", "Linux"):
            raise ValueError("Source acceptance currently supports Mac and Linux")
        sha = self.run("source-sha", ["git", "rev-parse", "HEAD"]).strip()
        self.record["source_sha"] = sha
        if self.run("source-status", ["git", "status", "--porcelain", "--untracked-files=no"]).strip():
            raise ValueError("Source acceptance requires a clean tracked checkout")
        expected = source_files(self.root)
        _native.write_json(self.output / "source-files.json", expected)
        self.record["source_file_count"] = len(expected)
        self.record["source_manifest_sha256"] = _native.sha256(self.output / "source-files.json")
        version = self.run("maturin-version", [sys.executable, "-I", "-m", "maturin", "--version"]).strip()
        if version != f"maturin {_native.MATURIN_VERSION}":
            raise ValueError(f"Unexpected maturin: {version}")
        self.record["maturin"] = version
        self.record["rustc"] = self.run("rustc-version", ["rustc", "-Vv"]).strip()
        if not self.record["rustc"].startswith(f"rustc {_native.RUST_VERSION} "):
            raise ValueError("Source acceptance requires the pinned native Rust toolchain")
        self.verify_vendor_source()
        dist = self.output / "sdist"
        dist.mkdir()
        self.run("source-distribution", [sys.executable, "-I", "-m", "maturin", "sdist", "--out", dist])
        archives = list(dist.glob("*.tar.gz"))
        if len(archives) != 1:
            raise ValueError(f"Expected one source distribution, found {archives}")
        archive = archives[0]
        self.record["sdist_mode_restoration"] = restore_source_modes(
            archive, expected, self.output / "sdist-generator-original" / archive.name,
        )
        unpacked = unpack_verified(archive, self.output / "unpacked", expected)
        self.record["sdist"] = {
            "path": str(archive.relative_to(self.output)), "sha256": _native.sha256(archive),
            "all_tracked_files_byte_identical": True, "executable_modes_preserved": True,
            "modes_restored": bool(self.record["sdist_mode_restoration"]["changes"]),
            "source_files": len(expected), "unpacked": str(unpacked.relative_to(self.output)),
        }
        self.save()
        target = build_target or self.output / "build-target"
        self.env["CARGO_TARGET_DIR"] = str(target)
        self.env["CARGO_NET_OFFLINE"] = "true"
        self.record["cargo_target_dir"] = str(target)
        wheels = self.output / "wheels"
        wheels.mkdir()
        command = [
            sys.executable, "-I", "-m", "maturin", "build", "--release", "--locked", "--offline",
            "--interpreter", sys.executable, "--out", wheels,
        ]
        if platform.system() == "Linux":
            command += ["--compatibility", "linux"]
        self.run("unpacked-offline-wheel", command, cwd=unpacked)
        candidates = list(wheels.glob("aos_sdk-*.whl"))
        if len(candidates) != 1:
            raise ValueError(f"Expected one newly built wheel, found {candidates}")
        wheel = candidates[0]
        self.run("public-wheel-metadata", [sys.executable, "-I", unpacked / "scripts/public_wheel.py",
                                          "--wheel", wheel, "--source-repo", unpacked], cwd=unpacked)
        config = tomllib.loads((self.root / "pyproject.toml").read_text())
        with zipfile.ZipFile(wheel) as package:
            names = package.namelist()
            extensions = [name for name in names if name.startswith("aos_sdk/_native.") and name.endswith(".so")]
            if len(extensions) != 1:
                raise ValueError("Wheel must contain exactly one native SDK extension")
            native_hash = hashlib.sha256(package.read(extensions[0])).hexdigest()
            licenses = []
            for relative in config["project"]["license-files"]:
                matches = [name for name in names if name.endswith(".dist-info/licenses/" + relative)]
                if len(matches) != 1 or package.read(matches[0]) != (self.root / relative).read_bytes():
                    raise ValueError(f"Missing or changed wheel license: {relative}")
                licenses.append(matches[0])
        self.record["wheel"] = {
            "path": str(wheel.relative_to(self.output)), "sha256": _native.sha256(wheel),
            "native_sha256": native_hash, "verified_license_files": licenses,
        }
        environment = self.output / "installed-env"
        self.create_environment("fresh-environment", environment)
        python = _native.environment_python(environment)
        self.run("install-offline-wheel", [
            python, "-I", "-m", "pip", "install", "--no-index", "--no-deps", wheel,
        ], cwd=self.output)
        imported = self.run("isolated-native-import", [
            python, "-I", "-c", IMPORT_PROBE, native_hash,
        ], cwd=self.output)
        self.record["installed_import"] = json.loads(imported)
        if self.run("final-source-sha", ["git", "rev-parse", "HEAD"]).strip() != sha:
            raise ValueError("Source HEAD changed during acceptance")
        if source_files(self.root) != expected:
            raise ValueError("Tracked source contents changed during acceptance")
        if self.run("final-source-status", ["git", "status", "--porcelain", "--untracked-files=no"]).strip():
            raise ValueError("Tracked checkout changed during acceptance")
        self.record["status"] = "passed"
        self.record["scope"] = "source packaging, offline rebuild, license bytes, isolated native import"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--build-target-dir", type=Path)
    arguments = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = arguments.output.resolve()
    if output.is_relative_to(root):
        raise ValueError("Evidence output must be outside the source checkout")
    output.mkdir(parents=True, exist_ok=False)
    check = SdistAcceptance(root, output)
    try:
        check.execute_sdist(arguments.build_target_dir.resolve() if arguments.build_target_dir else None)
    except BaseException as error:
        check.record.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        check.record["finished_unix"] = time.time()
        check.save()
    print(json.dumps({"status": check.record["status"], "source_sha": check.record["source_sha"]}))


if __name__ == "__main__":
    main()
