#!/usr/bin/env python3
"""Build and verify native Linux/Windows x86_64 and macOS arm64 artifacts with CPython 3.11.

Run with Python 3.11 and rustup available:
    python -I scripts/native_acceptance.py --output /absolute/evidence/directory

No release or deployment is performed. The output directory must be new or
empty. Build environments live in a fresh temporary directory; the installed
wheel is tested from outside the checkout with Python isolated mode enabled.
Set AOS_ZENOHD_ARCHIVE to reuse a local archive; its bytes must match the
platform's pinned official release digest before extraction or execution.
Set AOS_ZENOH_CRATE_ARCHIVE to reuse the pinned original Zenoh source archive.
Set AOS_ZENOH_TLS_CRATE_ARCHIVE to reuse the pinned original TLS source archive.
The complete vendor patch is reproduced and checked before any native build.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
import traceback
import urllib.request
import zipfile

RUST_VERSION = "1.99.1"
MATURIN_VERSION = "1.15.0"
ZENOH_VERSION = "1.10.1"
RELEASE_API = "https://api.github.com/repos/eclipse-zenoh/zenoh/releases/tags/1.10.1"
RELEASE_BASE = "https://github.com/eclipse-zenoh/zenoh/releases/download/1.10.1/"
ZENOH_CRATE_URL = "https://static.crates.io/crates/zenoh/zenoh-1.10.1.crate"
ZENOH_CRATE_SHA256 = "453ba5d28a1197653aae4bb024fd74a9d5c7051d5a19a77f3a43d83f40c22584"
ZENOH_TLS_CRATE_URL = "https://static.crates.io/crates/zenoh-link-tls/zenoh-link-tls-1.10.1.crate"
ZENOH_TLS_CRATE_SHA256 = "cce65fd71b4631821a85058542e28faac0151af6c53cd61f2dfafc7e16c4d6c4"

# Read from the official GitHub release API on 2026-10-20. Pin both the exact
# asset and its API-provided digest; a changed upstream archive fails closed.
PLATFORMS = {
    "Linux": {
        "machines": ["x86_64", "amd64"],
        "target": "x86_64-unknown-linux-gnu",
        "asset": "zenoh-1.10.1-x86_64-unknown-linux-gnu-standalone.zip",
        "asset_id": 548712663,
        "sha256": "512ac81438fd995ca2db02f75dd83afa048b6a11630ee10e0cb4dc423c39a5e9",
        "exe": "",
    },
    "Darwin": {
        "machines": ["arm64", "aarch64"],
        "target": "aarch64-apple-darwin",
        "asset": "zenoh-1.10.1-aarch64-apple-darwin-standalone.zip",
        "asset_id": 548711983,
        "sha256": "a27a177014fa81bc1bdb9b824373a2a8bce0e0a33c40e6c189d6fc9414b57b8a",
        "exe": "",
    },
    "Windows": {
        "machines": ["x86_64", "amd64"],
        "target": "x86_64-pc-windows-msvc",
        "asset": "zenoh-1.10.1-x86_64-pc-windows-msvc-standalone.zip",
        "asset_id": 548711685,
        "sha256": "2da28ac4e700de3c43db38417160b0ca971bd336861dd9d86781d042ae24fa40",
        "exe": ".exe",
    },
}

# Known platform exclusions and uncovered Windows fixtures, with exact IDs/reasons:
# missing tests, changed declarations, and any other skip still fail acceptance.
WINDOWS_EXPECTED_SKIPS = {
    'test_platform_bag.BagCliTest.test_full_output_does_not_delay_signal_cleanup': 'Unix socket descriptor backpressure contract',
    'test_platform_launch_cli.LaunchCliTest.test_all_terminal_signals_clean_the_owned_process_group': 'Unix process group contract',
    'test_platform_launch_cli.LaunchCliTest.test_journal_like_stall_keeps_control_running_and_drains_after_children_are_gone': 'Unix journal stream backpressure contract',
    'test_platform_launch_cli.LaunchCliTest.test_spawn_failure_cleans_an_already_started_peer': 'Unix process contract',
    'test_platform_launch_cli.LaunchCliTest.test_terminal_errors_are_bounded_and_preserve_shared_descriptor_flags': 'Unix shared descriptor contract',
    'test_platform_management_cli.ManagementCliTest.test_dump_new_file_is_private_and_replacement_preserves_permissions': 'file mode checks use POSIX permissions',
    'test_platform_observation.PlatformObservationTest.test_doctor_reaps_children_when_parent_ignores_sigchld': 'requires inherited POSIX SIGCHLD disposition',
    'test_platform_telemetry.TelemetryTest.test_failed_stderr_counts_loss_and_executor_keeps_dispatching': 'file-descriptor failure injection requires POSIX',
}


def native_platform(system, machine):
    spec = PLATFORMS.get(system)
    if spec is None or machine.lower() not in spec["machines"]:
        raise ValueError("Native acceptance requires x86_64 Linux/Windows or arm64 macOS")
    return spec


def installed_test_scope(selected, skipped, system):
    expected = WINDOWS_EXPECTED_SKIPS if system == "Windows" else {}
    observed = {item["test"]: item["reason"] for item in skipped}
    missing_tests = sorted(set(expected) - set(selected))
    unexpected = [item for item in skipped if expected.get(item["test"]) != item["reason"]]
    missing_skips = sorted(name for name, reason in expected.items() if observed.get(name) != reason)
    return {
        "valid": not missing_tests and not unexpected and not missing_skips and len(observed) == len(skipped),
        "declared_not_applicable": [dict(test=name, reason=reason) for name, reason in sorted(expected.items())],
        "missing_tests": missing_tests,
        "unexpected_skips": unexpected,
        "missing_declared_skips": missing_skips,
    }


# Executed by the fresh environment's Python -I from outside the source tree.
# Only the test directory is added by unittest discovery; aos_sdk must resolve
# to the installed wheel, including its native extension.
# Embed the same scope checker exercised by the harness tests. It depends only
# on this pinned literal and builtins; no checkout path is added to sys.path.
INSTALLED_TESTS = (
    "WINDOWS_EXPECTED_SKIPS = " + repr(WINDOWS_EXPECTED_SKIPS) + "\n"
    + inspect.getsource(installed_test_scope)
    + r"""
import importlib.machinery, json, pathlib, platform, sys, sysconfig, unittest
import aos_sdk
from aos_sdk import _native
tests, report = map(pathlib.Path, sys.argv[1:])
native = pathlib.Path(_native.__file__).resolve()
package = pathlib.Path(aos_sdk.__file__).resolve()
site = pathlib.Path(sysconfig.get_paths()["platlib"]).resolve()
assert sys.flags.isolated, "acceptance requires Python -I"
assert sys.flags.utf8_mode, "acceptance requires explicit -X utf8; -I ignores PYTHONUTF8"
assert native.is_relative_to(site / "aos_sdk"), (native, site)
assert package.parent == native.parent, (package, native)
assert isinstance(_native.__loader__, importlib.machinery.ExtensionFileLoader)
suite = unittest.defaultTestLoader.discover(str(tests), pattern="test_*.py")
def ids(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from ids(item)
        else:
            yield item.id()
selected = list(ids(suite))
for required in ("test_native_import", "test_interface", "test_zenoh_session",
                 "test_process_echo", "test_typed_sdk", "test_stage1_cli",
                 "test_action_sdk"):
    assert any(name.startswith(required + ".") for name in selected), required
result = unittest.TextTestRunner(verbosity=2).run(suite)
skipped = [dict(test=test.id(), reason=reason) for test, reason in result.skipped]
scope = installed_test_scope(selected, skipped, platform.system())
data = dict(
    status="passed" if result.wasSuccessful() and scope["valid"] else "failed",
    python=sys.version, executable=sys.executable, platform=platform.platform(),
    native_path=str(native), package_path=str(package), isolated=bool(sys.flags.isolated),
    utf8_mode=bool(sys.flags.utf8_mode),
    selected=selected, tests_run=result.testsRun,
    executed_tests=result.testsRun - len(result.skipped), platform_scope=scope,
    failures=[test.id() for test, _ in result.failures],
    errors=[test.id() for test, _ in result.errors],
    skipped=skipped,
)
report.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
if not scope["valid"]:
    print("Native acceptance requires the exact declared platform skips and no others.", file=sys.stderr)
raise SystemExit(0 if data["status"] == "passed" else 1)
"""
)


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def environment_python(directory: Path) -> Path:
    return directory / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def extract_archive(archive: Path, destination: Path) -> None:
    """Extract only safe ordinary files from the verified standalone archive."""
    with zipfile.ZipFile(archive) as bundle:
        for member in bundle.infolist():
            relative = PurePosixPath(member.filename)
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or "\\" in member.filename
                or ":" in member.filename
                or ((member.external_attr >> 16) & 0o170000) == 0o120000
            ):
                raise ValueError(f"Unsafe archive member: {member.filename!r}")
            destination_path = destination.joinpath(*relative.parts)
            if member.is_dir():
                destination_path.mkdir(parents=True, exist_ok=True)
            else:
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                with bundle.open(member) as source, destination_path.open("wb") as target:
                    shutil.copyfileobj(source, target)


class Acceptance:
    def __init__(self, root: Path, output: Path):
        self.root = root
        self.output = output
        self.logs = output / "logs"
        self.logs.mkdir()
        self.env = os.environ.copy()
        self.env.update(
            RUSTUP_TOOLCHAIN=RUST_VERSION,
            PYO3_PYTHON=sys.executable,
            PYTHONIOENCODING="utf-8",
            PYTHONUTF8="1",
        )
        # Keep imports and interface discovery independent of a developer setup.
        for key in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "AOS_INTERFACE_PATH",
                    "AOS_ZENOH_CONFIG", "AOS_DOMAIN_ID"):
            self.env.pop(key, None)
        self.record = {
            "status": "running",
            "started_unix": time.time(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": sys.version,
            "python_executable": sys.executable,
            "github_run_id": os.environ.get("GITHUB_RUN_ID"),
            "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
            "commands": [],
        }
        self.save()

    def save(self) -> None:
        write_json(self.output / "provenance.json", self.record)

    def create_environment(self, name: str, directory: Path) -> None:
        if importlib.util.find_spec("ensurepip") is not None:
            command = [sys.executable, "-I", "-m", "venv", str(directory)]
        else:
            uv = shutil.which("uv")
            if uv is None:
                raise ValueError(
                    "This Python installation lacks ensurepip. Install its venv support "
                    "or put uv on PATH to create isolated environments without system changes."
                )
            command = [uv, "venv", "--seed", "--python", sys.executable, str(directory)]
            self.record["venv_backend"] = self.run(
                "uv-version", [uv, "--version"], timeout=30,
            ).strip()
        self.run(name, command)

    def run(self, name: str, command: list[str | Path], *, cwd: Path | None = None,
            timeout: int = 1800) -> str:
        command = [str(part) for part in command]
        log_path = self.logs / f"{len(self.record['commands']) + 1:02d}-{name}.log"
        entry = {
            "name": name, "command": command, "cwd": str(cwd or self.root),
            "log": log_path.relative_to(self.output).as_posix(), "started_unix": time.time(),
        }
        self.record["commands"].append(entry)
        self.save()
        print(f"Running {name}; log: {log_path}", flush=True)
        try:
            # Stream directly into the raw log rather than buffering a long build.
            with log_path.open("wb") as log:
                process = subprocess.Popen(
                    command, cwd=cwd or self.root, env=self.env,
                    stdout=log, stderr=subprocess.STDOUT,
                    start_new_session=os.name != "nt",
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
                )
                try:
                    returncode = process.wait(timeout=timeout)
                except BaseException:
                    # Timeout cleanup covers this command's router/worker children.
                    if os.name == "nt":
                        subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                                       stdout=log, stderr=subprocess.STDOUT, timeout=30, check=False)
                    else:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    process.wait(timeout=30)
                    raise
            entry["returncode"] = returncode
            text = log_path.read_text(encoding="utf-8", errors="replace")
            if returncode:
                print(text[-20000:], file=sys.stderr, flush=True)
                raise subprocess.CalledProcessError(returncode, command)
            return text
        except BaseException as error:
            entry["error"] = str(error)
            raise
        finally:
            entry["finished_unix"] = time.time()
            self.save()

    def verify_default_profile_readiness(self) -> None:
        name = "launch_probe::tests::real_default_profile_reaches_owned_node_readiness"
        output = self.run("default-profile-readiness", [
            "cargo", "test", "-p", "aos-cli", "--locked", name,
            "--", "--ignored", "--exact", "--nocapture",
        ])
        summaries = re.findall(
            r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;",
            output, re.MULTILINE,
        )
        proof = {
            "test": name,
            "matching_success_lines": output.splitlines().count(f"test {name} ... ok"),
            "passed": sum(int(passed) for _, passed, _, _ in summaries),
            "failed": sum(int(failed) for _, _, failed, _ in summaries),
            "ignored": sum(int(ignored) for _, _, _, ignored in summaries),
            "result_blocks": len(summaries),
        }
        valid = (
            proof["matching_success_lines"] == 1
            and proof["passed"] == 1
            and proof["failed"] == 0
            and proof["ignored"] == 0
            and all(status == "ok" for status, _, _, _ in summaries)
        )
        proof["status"] = "passed" if valid else "failed"
        self.record["default_profile_readiness"] = proof
        self.save()
        if not valid:
            raise ValueError(
                "default-profile readiness must run exactly the named test: "
                f"expected 1 passed, 0 failed, 0 ignored; got {proof}"
            )

    def verify_vendor_source(self) -> None:
        sources = self.output / "sources"
        sources.mkdir()
        archive = sources / f"zenoh-{ZENOH_VERSION}.crate"
        cached = self.env.get("AOS_ZENOH_CRATE_ARCHIVE")
        if cached:
            source = Path(cached).resolve(strict=True)
            shutil.copy2(source, archive)
            acquisition = {"kind": "local_archive", "source": str(source)}
        else:
            request = urllib.request.Request(
                ZENOH_CRATE_URL, headers={"User-Agent": "aos-native-acceptance"},
            )
            print(f"Downloading {ZENOH_CRATE_URL}", flush=True)
            with urllib.request.urlopen(request, timeout=120) as response, archive.open("wb") as target:
                shutil.copyfileobj(response, target)
            acquisition = {"kind": "official_crate_download", "source": ZENOH_CRATE_URL}
        actual = sha256(archive)
        self.record["zenoh_vendor"] = {
            "version": ZENOH_VERSION, "archive_url": ZENOH_CRATE_URL,
            "expected_sha256": ZENOH_CRATE_SHA256, "actual_sha256": actual,
            "archive": archive.relative_to(self.output).as_posix(), "acquisition": acquisition,
        }
        self.save()
        if actual != ZENOH_CRATE_SHA256:
            raise ValueError(f"Zenoh source archive checksum mismatch: {actual}")
        # Tests and source-check reruns use this same retained, verified input.
        self.env["AOS_ZENOH_CRATE_ARCHIVE"] = str(archive)
        proof = self.run("zenoh-vendor-source", [
            sys.executable, "-I", self.root / "scripts/check_zenoh_vendor.py",
            "--archive", archive, "--json",
        ], timeout=120)
        self.record["zenoh_vendor"]["verification"] = json.loads(proof)
        self.save()
        self.verify_tls_vendor_source()

    def verify_tls_vendor_source(self) -> None:
        sources = self.output / "sources"
        sources.mkdir(exist_ok=True)
        archive = sources / f"zenoh-link-tls-{ZENOH_VERSION}.crate"
        cached = self.env.get("AOS_ZENOH_TLS_CRATE_ARCHIVE")
        if cached:
            source = Path(cached).resolve(strict=True)
            shutil.copy2(source, archive)
            acquisition = {"kind": "local_archive", "source": str(source)}
        else:
            request = urllib.request.Request(
                ZENOH_TLS_CRATE_URL, headers={"User-Agent": "aos-native-acceptance"},
            )
            print(f"Downloading {ZENOH_TLS_CRATE_URL}", flush=True)
            with urllib.request.urlopen(request, timeout=120) as response, archive.open("wb") as target:
                shutil.copyfileobj(response, target)
            acquisition = {"kind": "official_crate_download", "source": ZENOH_TLS_CRATE_URL}
        actual = sha256(archive)
        self.record["zenoh_tls_vendor"] = {
            "version": ZENOH_VERSION, "archive_url": ZENOH_TLS_CRATE_URL,
            "expected_sha256": ZENOH_TLS_CRATE_SHA256, "actual_sha256": actual,
            "archive": archive.relative_to(self.output).as_posix(), "acquisition": acquisition,
        }
        self.save()
        if actual != ZENOH_TLS_CRATE_SHA256:
            raise ValueError(f"Zenoh TLS source archive checksum mismatch: {actual}")
        self.env["AOS_ZENOH_TLS_CRATE_ARCHIVE"] = str(archive)
        proof = self.run("zenoh-tls-vendor-source", [
            sys.executable, "-I", self.root / "scripts/check_zenoh_tls_vendor.py",
            "--archive", archive, "--json",
        ], timeout=120)
        self.record["zenoh_tls_vendor"]["verification"] = json.loads(proof)
        self.save()

    def download_router(self, spec: dict[str, str | int]) -> Path:
        vendor = self.output / "zenohd"
        vendor.mkdir()
        archive = vendor / str(spec["asset"])
        url = RELEASE_BASE + str(spec["asset"])
        cached_archive = self.env.get("AOS_ZENOHD_ARCHIVE")
        if cached_archive:
            source = Path(cached_archive).resolve(strict=True)
            shutil.copy2(source, archive)
            acquisition = {"kind": "local_archive", "source": str(source)}
        else:
            request = urllib.request.Request(url, headers={"User-Agent": "aos-native-acceptance"})
            print(f"Downloading {url}", flush=True)
            with urllib.request.urlopen(request, timeout=120) as response, archive.open("wb") as target:
                shutil.copyfileobj(response, target)
            acquisition = {"kind": "official_release_download", "source": url}
        digest = sha256(archive)
        self.record["zenohd"] = {
            "version": ZENOH_VERSION, "release_api": RELEASE_API, "asset_url": url,
            "asset_id": spec["asset_id"], "expected_sha256": spec["sha256"],
            "actual_sha256": digest, "archive": archive.relative_to(self.output).as_posix(),
            "acquisition": acquisition,
        }
        self.save()
        if digest != spec["sha256"]:
            raise ValueError(f"Zenoh archive checksum mismatch: {digest}")
        unpacked = vendor / "unpacked"
        extract_archive(archive, unpacked)
        # Both pinned archives expose the runnable binary at their root.
        # The Windows archive also has a deps/zenohd.exe copy; do not glob it.
        binary = unpacked / ("zenohd" + str(spec["exe"]))
        if not binary.is_file():
            raise ValueError(f"Expected the standalone root executable: {binary}")
        if os.name != "nt":
            binary.chmod(0o755)
        version = self.run("zenohd-version", [binary, "--version"], timeout=30).strip()
        if not re.search(r"\bzenohd v?1\.10\.1\b", version):
            raise ValueError(f"Unexpected router version: {version}")
        self.record["zenohd"].update(
            executable=binary.relative_to(self.output).as_posix(), executable_sha256=sha256(binary),
            version_output=version,
        )
        self.save()
        return binary

    @staticmethod
    def router_log_snapshot(target: Path) -> dict[str, tuple[int, str, int]]:
        return {
            log.relative_to(target).as_posix(): (log.stat().st_mtime_ns, sha256(log), log.stat().st_size)
            for log in target.rglob("zenohd-*.log")
        }

    def collect_router_logs(self, target: Path, before: dict[str, tuple[int, str, int]]) -> None:
        collected = []
        excluded = []
        for log in target.rglob("zenohd-*.log"):
            relative = log.relative_to(target).as_posix()
            current = (log.stat().st_mtime_ns, sha256(log), log.stat().st_size)
            previous = before.get(relative)
            if previous and previous[1] == current[1]:
                excluded.append(relative)
                continue
            offset = 0
            appended = False
            if previous and current[2] >= previous[2]:
                digest = hashlib.sha256()
                remaining = previous[2]
                with log.open("rb") as stream:
                    while remaining:
                        chunk = stream.read(min(remaining, 1024 * 1024))
                        if not chunk:
                            break
                        digest.update(chunk)
                        remaining -= len(chunk)
                if not remaining and digest.hexdigest() == previous[1]:
                    offset = previous[2]
                    appended = True
            # Preserve only this run's suffix when an existing log was appended.
            # Replaced/truncated files are copied in full and labeled explicitly.
            name = "router-" + relative.replace("\\", "--").replace("/", "--")
            destination = self.logs / name
            with log.open("rb") as source, destination.open("wb") as output:
                source.seek(offset)
                shutil.copyfileobj(source, output)
            collected.append({
                "source": relative, "log": "logs/" + name, "sha256": sha256(destination),
                "source_sha256": current[1], "source_size": current[2],
                "copied_from_offset": offset, "copied_bytes": destination.stat().st_size,
                "change": "appended" if appended else ("replaced" if previous else "new"),
            })
        self.record["router_logs"] = {
            "collected": collected, "excluded_unchanged_from_previous_run": sorted(excluded),
        }
        self.save()

    def execute(self) -> None:
        if platform.python_implementation() != "CPython" or sys.version_info[:2] != (3, 11):
            raise ValueError("Native acceptance requires CPython 3.11")
        spec = native_platform(platform.system(), platform.machine())
        self.record["target"] = spec["target"]
        self.record["source_sha"] = self.run("source-sha", ["git", "rev-parse", "HEAD"]).strip()
        dirty = self.run("source-status", ["git", "status", "--porcelain"]).strip()
        self.record["tracked_changes"] = dirty
        if dirty:
            raise ValueError("Native acceptance requires no uncommitted or untracked source files")
        config = tomllib.loads((self.root / "rust-toolchain.toml").read_text(encoding="utf-8"))
        if config["toolchain"]["channel"] != RUST_VERSION:
            raise ValueError("rust-toolchain.toml does not match the pinned acceptance toolchain")
        self.record["cargo_lock_sha256"] = sha256(self.root / "Cargo.lock")
        self.verify_vendor_source()
        self.run("rustup", ["rustup", "toolchain", "install", RUST_VERSION, "--no-self-update", "--no-update", "--profile", "minimal",
                            "--component", "rustfmt", "--component", "clippy"])
        rustc = self.run("rustc-version", ["rustc", "-Vv"]).strip()
        if f"rustc {RUST_VERSION} " not in rustc or f"host: {spec['target']}" not in rustc:
            raise ValueError(f"Compiler is not the expected native toolchain: {rustc}")
        self.record["rustc"] = rustc
        self.record["cargo"] = self.run("cargo-version", ["cargo", "--version"]).strip()
        router = self.download_router(spec)
        self.env["AOS_ZENOHD"] = str(router)
        self.env["AOS_ECHO_RECORD"] = str(self.output / "topic-processes.json")
        self.env["AOS_STAGE1_RECORD"] = str(self.output / "topic-service-cli.json")
        self.env["AOS_ACTION_CLI_RECORD"] = str(self.output / "action-cli.json")

        with tempfile.TemporaryDirectory(prefix="aos-native-build-") as temporary, ExitStack() as cleanup:
            work = Path(temporary).resolve()
            # An explicitly selected cache survives this run; installed environments
            # are still fresh and outside the source checkout. Cargo fingerprints
            # select the locked source inputs; no bit-reproducibility claim is made.
            target = Path(self.env.get("AOS_BUILD_TARGET_DIR", str(work / "target"))).resolve()
            self.record["cargo_target_dir"] = str(target)
            # Preserve router diagnostics before the build directory is removed,
            # including when the explicitly enabled process test fails.
            prior_router_logs = self.router_log_snapshot(target)
            cleanup.callback(self.collect_router_logs, target, prior_router_logs)
            self.env["CARGO_TARGET_DIR"] = str(target)
            self.env["CARGO_BUILD_TARGET"] = str(spec["target"])
            self.run("format", ["cargo", "fmt", "--all", "--", "--check"])
            self.run("clippy", ["cargo", "clippy", "--workspace", "--all-targets",
                                "--locked", "--", "-D", "warnings"])
            self.run("tls-vendor-tests", ["cargo", "test", "-p", "zenoh-link-tls",
                                          "--lib", "--locked"])
            self.run("cargo-tests", ["cargo", "test", "--workspace", "--all-targets", "--locked"])
            self.run("cargo-doc-tests", ["cargo", "test", "--workspace", "--doc", "--locked"])
            if platform.system() in ("Linux", "Darwin", "Windows"):
                # The real null-profile regression must use its actual default
                # endpoint. Ordinary cargo test must not claim a user's 7447.
                # This explicit acceptance invocation fails if the isolated
                # test environment has not kept that port available.
                self.verify_default_profile_readiness()
            self.run("evidence-harness-tests", [
                sys.executable, "-I", "-m", "unittest", "discover",
                "-s", self.root / "scripts", "-p", "test_*.py", "-v",
            ])
            self.run("native-cli", ["cargo", "build", "-p", "aos-cli", "--release", "--locked"])
            cli_dir = self.output / "cli"
            cli_dir.mkdir()
            cli = cli_dir / ("aos" + str(spec["exe"]))
            shutil.copy2(target / str(spec["target"]) / "release" / cli.name, cli)
            self.env["AOS_CLI"] = str(cli)
            self.record["cli"] = {
                "path": cli.relative_to(self.output).as_posix(), "sha256": sha256(cli),
            }

            self.run("native-process-fixture", ["cargo", "build", "-p", "aos-launch", "--example", "process_fixture", "--release", "--locked"])
            fixture = cli_dir / ("process_fixture" + str(spec["exe"]))
            shutil.copy2(target / str(spec["target"]) / "release/examples" / fixture.name, fixture)
            self.env["AOS_PROCESS_FIXTURE"] = str(fixture)
            self.record["process_fixture"] = {"path": fixture.relative_to(self.output).as_posix(), "sha256": sha256(fixture)}

            build_env = work / "build-env"
            self.create_environment("build-environment", build_env)
            build_python = environment_python(build_env)
            self.run("install-maturin", [build_python, "-I", "-m", "pip", "install",
                                        "--disable-pip-version-check", f"maturin=={MATURIN_VERSION}"])
            self.record["maturin"] = self.run(
                "maturin-version", [build_python, "-I", "-m", "maturin", "--version"],
            ).strip()
            wheels = self.output / "wheels"
            wheels.mkdir()
            command = [build_python, "-I", "-m", "maturin", "build", "--release", "--locked",
                       "--interpreter", sys.executable, "--target", spec["target"], "--out", wheels]
            if platform.system() == "Linux":
                # This acceptance wheel targets the native runner, not a manylinux baseline.
                command += ["--compatibility", "linux"]
            self.run("wheel-build", command)
            candidates = list(wheels.glob("aos_sdk-*.whl"))
            if len(candidates) != 1:
                raise ValueError(f"Expected exactly one newly built SDK wheel, found {candidates}")
            wheel = candidates[0]
            self.run("public-wheel-metadata", [sys.executable, "-I", self.root / "scripts/public_wheel.py",
                                              "--wheel", wheel, "--source-repo", self.root])
            self.record["wheel"] = {
                "path": wheel.relative_to(self.output).as_posix(), "sha256": sha256(wheel),
            }

            installed_env = work / "installed-env"
            self.create_environment("fresh-environment", installed_env)
            installed_python = environment_python(installed_env)
            self.run("install-wheel", [installed_python, "-I", "-m", "pip", "install",
                                      "--disable-pip-version-check", "--no-index", "--no-deps", wheel])
            self.run("installed-distributions", [installed_python, "-I", "-m", "pip", "freeze",
                                                "--all"], cwd=work)
            self.run("installed-topic-service-action",
                     [installed_python, "-I", "-X", "utf8", "-u", "-c", INSTALLED_TESTS,
                      self.root / "python" / "tests", self.output / "python-tests.json"],
                     cwd=work, timeout=1200)
            # Exercise the intentionally ignored Rust cross-process test explicitly.
            self.run("native-probe", ["cargo", "build", "-p", "aos-zenoh", "--example",
                                     "probe", "--locked"])
            self.env["AOS_ZENOH_PROBE"] = str(
                target / str(spec["target"]) / "debug" / "examples" / ("probe" + str(spec["exe"]))
            )
            self.run("rust-process-test", ["cargo", "test", "-p", "aos-zenoh", "--test",
                                          "real_zenohd", "--locked", "--", "--ignored", "--nocapture"],
                     timeout=120)
        self.record["final_source_sha"] = self.run(
            "final-source-sha", ["git", "rev-parse", "HEAD"],
        ).strip()
        self.record["final_source_status"] = self.run(
            "final-source-status", ["git", "status", "--porcelain"],
        ).strip()
        if self.record["source_sha"] != self.record["final_source_sha"]:
            raise ValueError("Candidate commit changed during native acceptance")
        if self.record["final_source_status"]:
            raise ValueError("Candidate was edited during native acceptance")
        self.record["status"] = "passed"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="New or empty evidence directory")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error(f"Output directory must be empty: {output}")
    acceptance = Acceptance(Path(__file__).resolve().parents[1], output)
    try:
        acceptance.execute()
        return 0
    except BaseException as error:
        acceptance.record.update(status="failed", error=str(error))
        (acceptance.logs / "failure.log").write_text(traceback.format_exc(), encoding="utf-8")
        traceback.print_exc()
        return 1
    finally:
        acceptance.record["finished_unix"] = time.time()
        acceptance.save()
        print(f"Native acceptance {acceptance.record['status']}: {output}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
