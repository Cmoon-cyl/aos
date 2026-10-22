#!/usr/bin/env python3
"""AOS installation, local Router ownership, and transactional CLI updates.

Only the Python standard library is needed. setup.sh/setup.ps1 obtain a pinned
uv binary and CPython before invoking this module. Runtime packages are never
compiled locally. Every activated installation has passed an actual Service call.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import secrets
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import uuid
import zipfile

HERE = Path(__file__).resolve().parent
PLUGIN = HERE.parents[2]
REPOSITORY = "Cmoon-cyl/aos"
RELEASE_PREFIX = "aos-agent-v"
MAX_ARCHIVE_BYTES = 2 * 1024**3


class Failure(Exception):
    def __init__(self, code: str, message: str, *, exit_code: int = 1):
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code


def emit(data, *, ok=True):
    print(json.dumps({"ok": ok, "data" if ok else "error": data},
                     ensure_ascii=True, allow_nan=False), flush=True)


def log(message):
    print(message, file=sys.stderr, flush=True)


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temp.open("x", encoding="utf-8") as stream:
            os.chmod(temp, 0o600)
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def write_text_if_changed(path, source, *, mode=0o644):
    path = Path(path)
    if path.is_file() and path.read_bytes() == source.encode("utf-8"):
        if os.name != "nt":
            path.chmod(mode)
        return
    temp = path.with_name(path.name+"."+uuid.uuid4().hex+".tmp")
    try:
        with temp.open("x", encoding="utf-8", newline="") as stream:
            os.chmod(temp, mode)
            stream.write(source)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def version(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d+\.\d+\.\d+", value):
        raise Failure("INVALID_VERSION", "Use a stable version such as 0.1.0.")
    return tuple(map(int, value.split(".")))


def default_prefix():
    if os.environ.get("AOS_HOME"):
        return Path(os.environ["AOS_HOME"]).expanduser().resolve()
    if os.name == "nt":
        return Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData/Local")) / "AOS"
    return Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local/share")) / "aos"


def host_platform():
    machine = platform.machine().lower()
    if sys.platform == "darwin" and machine == "arm64":
        return "macos-arm64"
    if sys.platform == "linux" and machine in ("x86_64", "amd64"):
        if platform.libc_ver()[0] != "glibc":
            raise Failure("UNSUPPORTED_PLATFORM", "This release requires GNU/Linux with glibc.")
        return "linux-x86_64"
    if os.name == "nt" and machine in ("amd64", "x86_64") and sys.maxsize > 2**32:
        return "windows-x86_64"
    raise Failure("UNSUPPORTED_PLATFORM",
                  f"No verified AOS package for {sys.platform}/{machine}.")


def clean_environment():
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("AOS_") and k not in
           ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "UV_PROJECT_ENVIRONMENT")}
    env.update(PYTHONDONTWRITEBYTECODE="1", GH_PROMPT_DISABLED="1")
    if os.name == "nt":
        env["PATH"] = str(Path(sys.base_prefix)) + os.pathsep + env.get("PATH", "")
    return env


def run(command, *, cwd=None, env=None, input=None, timeout=180):
    try:
        result = subprocess.run(list(map(str, command)), cwd=cwd, env=env,
                                input=input, text=True, encoding="utf-8", stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=timeout)
    except subprocess.TimeoutExpired as error:
        raise Failure("COMMAND_TIMEOUT", f"{Path(str(command[0])).name} timed out.") from error
    except OSError as error:
        raise Failure("COMMAND_UNAVAILABLE", str(error)) from error
    if result.returncode:
        raise Failure("COMMAND_FAILED",
                      f"{Path(str(command[0])).name} exited {result.returncode}: "
                      f"{result.stderr[-2000:] or result.stdout[-2000:]}")
    return result.stdout


@contextmanager
def lock(prefix, name="install", timeout=120):
    prefix.mkdir(parents=True, exist_ok=True)
    path = prefix / (name + ".lock")
    with path.open("a+b") as stream:
        if path.stat().st_size == 0:
            stream.write(b"\0")
            stream.flush()
        deadline = time.monotonic() + timeout
        while True:
            try:
                stream.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    raise Failure("INSTALL_BUSY", "Another AOS operation holds the lock.")
                time.sleep(0.1)
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_UN)


def public_request(url):
    return urllib.request.Request(url, headers={"User-Agent": "aos-installer",
                                                "Accept": "application/octet-stream"})


def download_release(tag, asset, destination, *, repository=REPOSITORY):
    destination = Path(destination)
    if Path(asset).name != asset or not re.fullmatch(r"[A-Za-z0-9._+-]+", asset):
        raise Failure("INVALID_ASSET", "Invalid release asset name.")
    with tempfile.TemporaryDirectory(prefix="aos-download-", dir=destination.parent) as temp:
        output = Path(temp) / asset
        url = f"https://github.com/{repository}/releases/download/{tag}/{asset}"
        try:
            with urllib.request.urlopen(public_request(url), timeout=60) as response, output.open("wb") as stream:
                shutil.copyfileobj(response, stream)
        except (urllib.error.URLError, TimeoutError) as error:
            raise Failure("DOWNLOAD_FAILED", f"Cannot download {asset}: {error}") from error
        os.replace(output, destination)


def checked_download(tag, record, cache):
    cache.mkdir(parents=True, exist_ok=True)
    expected = record["sha256"]
    if not re.fullmatch("[a-f0-9]{64}", expected):
        raise Failure("INVALID_MANIFEST", "Missing SHA256 in the distribution manifest.")
    target = cache / (expected[:12] + "-" + record["asset"])
    if target.exists() and digest(target) == expected:
        return target
    target.unlink(missing_ok=True)
    log(f"Downloading {record['asset']}")
    download_release(tag, record["asset"], target)
    if digest(target) != expected:
        target.unlink(missing_ok=True)
        raise Failure("CHECKSUM_MISMATCH", "Release download did not match its pinned SHA256.")
    return target


def safe_member(name):
    path = PurePosixPath(name)
    if (not path.parts or path.is_absolute() or ".." in path.parts
            or "\\" in name or ":" in name or "\0" in name):
        raise Failure("UNSAFE_ARCHIVE", f"Unsafe archive path: {name!r}")
    return path


def extract_archive(archive, destination):
    """Accept one directory of plain files, never links or special devices."""
    destination.mkdir(parents=True, exist_ok=False)
    entries = []
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as source:
            for item in source.infolist():
                path = safe_member(item.filename)
                mode = item.external_attr >> 16
                if stat.S_ISLNK(mode):
                    raise Failure("UNSAFE_ARCHIVE", "Archive symlinks are not supported.")
                entries.append((path, item.is_dir(), item.file_size, mode, item))
            validate_entries(entries)
            for path, directory, _, mode, item in entries:
                target = destination.joinpath(*path.parts)
                if directory:
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with source.open(item) as stream, target.open("xb") as output:
                        shutil.copyfileobj(stream, output)
                    target.chmod(0o755 if mode & 0o111 else 0o644)
    else:
        with tarfile.open(archive, "r:gz") as source:
            for item in source.getmembers():
                path = safe_member(item.name)
                if not (item.isdir() or item.isfile()):
                    raise Failure("UNSAFE_ARCHIVE", "Archive contains a link or special file.")
                entries.append((path, item.isdir(), item.size, item.mode, item))
            validate_entries(entries)
            for path, directory, _, mode, item in entries:
                target = destination.joinpath(*path.parts)
                if directory:
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with source.extractfile(item) as stream, target.open("xb") as output:
                        shutil.copyfileobj(stream, output)
                    target.chmod(0o755 if mode & 0o111 else 0o644)
    return destination / entries[0][0].parts[0]


def validate_entries(entries):
    roots = {entry[0].parts[0] for entry in entries}
    names = [str(entry[0]).casefold() for entry in entries]
    if (not entries or len(roots) != 1 or len(names) != len(set(names))
            or len(entries) > 50000 or sum(entry[2] for entry in entries) > MAX_ARCHIVE_BYTES):
        raise Failure("UNSAFE_ARCHIVE", "Archive inventory is invalid or exceeds limits.")


def verify_bundle(bundle, distribution, host):
    manifest = read_json(bundle / "manifest.json")
    if (manifest.get("version") != distribution["runtime_version"]
            or manifest.get("platform") != host
            or manifest.get("acceptance_status") != "passed"
            or manifest.get("packaging_tracked_changes")
            or manifest.get("zenohd", {}).get("bundled") is not True):
        raise Failure("UNVERIFIED_PACKAGE", "Runtime package is not the accepted platform bundle.")
    inventory = {p.relative_to(bundle).as_posix() for p in bundle.rglob("*")
                 if p.is_file() and p.suffix != ".pyc"}
    if inventory != set(manifest["files"]) | {"manifest.json"}:
        raise Failure("INVENTORY_MISMATCH", "Package files differ from the manifest.")
    for name, record in manifest["files"].items():
        path = bundle.joinpath(*safe_member(name).parts)
        if (path.is_symlink() or path.stat().st_size != record["bytes"]
                or digest(path) != record["sha256"]):
            raise Failure("CHECKSUM_MISMATCH", f"Package file verification failed: {name}")
    return manifest


def tree_digest(root):
    summary = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if (path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
                and path.name not in ("installed-prefix.json", ".aos-managed-skill.json")):
            summary.update(path.relative_to(root).as_posix().encode())
            summary.update(bytes.fromhex(digest(path)))
    return summary.hexdigest()


def state(prefix, *, required=True, check_components=True):
    path = prefix / "current.json"
    if not path.is_file():
        if required:
            raise Failure("NOT_INSTALLED", "Run the AOS setup skill or one-command installer.")
        return None
    current = read_json(path)
    release = Path(current["release"]).resolve()
    if not release.is_relative_to((prefix / "releases").resolve()):
        raise Failure("INVALID_STATE", "Current release points outside this installation.")
    record = read_json(release / "installation.json")
    if record != current:
        raise Failure("INVALID_STATE", "The active installation record is inconsistent.")
    for key in ("python", "native_cli", "router", "manager") if check_components else ():
        if not Path(current[key]).is_file():
            raise Failure("INSTALLATION_DAMAGED", f"Installed component is missing: {key}")
    return current


def available_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def configs(folder, port):
    endpoint = f"tcp/127.0.0.1:{port}"
    common = {"scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}}}
    router = folder / "router.json5"
    session = folder / "session.json5"
    write_json(router, dict(common, mode="router", listen={"endpoints": [endpoint]}))
    write_json(session, dict(common, mode="client", connect={"endpoints": [endpoint]},
                             listen={"endpoints": []}))
    return router, session


def stop_child(child):
    if child.poll() is not None:
        return
    if os.name == "nt":
        child.terminate()
    else:
        child.send_signal(signal.SIGINT)
    try:
        child.wait(timeout=15)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=5)


def wait_port(port, process, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise Failure("ROUTER_START_FAILED", f"Router exited {process.returncode}.")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise Failure("ROUTER_TIMEOUT", "Router did not open its loopback listener.")


def self_test(current, output):
    output.mkdir(parents=True, exist_ok=False)
    env = clean_environment()
    children = []
    files = []
    report = {"status": "running", "started": time.time(), "scope": "isolated-loopback-service"}
    try:
        run([current["python"], "-X", "utf8", "-I", "-c",
             "import aos_sdk,aos_sdk._native; print(aos_sdk.__version__)"], env=env)
        run([current["native_cli"], "--help"], env=env)
        run([current["router"], "--version"], env=env)
        port = available_port()
        router_config, session = configs(output, port)
        env.update(AOS_ZENOH_CONFIG=str(session), AOS_DOMAIN_ID=str(secrets.randbelow(20000)+20000))
        def start(name, command, cwd):
            stdout = (output / f"{name}.stdout").open("w")
            stderr = (output / f"{name}.stderr").open("w")
            files.extend([stdout, stderr])
            child = subprocess.Popen(list(map(str, command)), cwd=cwd, env=env,
                                     stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
            children.append(child)
            return child
        router = start("router", [current["router"], "--config", router_config],
                       Path(current["router"]).parent)
        wait_port(port, router)
        provider = start("provider", [current["python"], "-X", "utf8", "-u", "-m", "examples.add_two_ints_server",
                                     "--config", session], current["bundle"])
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            lines = (output / "provider.stdout").read_text(encoding="utf-8").splitlines()
            if any('"ready": true' in line for line in lines):
                break
            if provider.poll() is not None:
                raise Failure("PROVIDER_START_FAILED", "The verification provider could not start.")
            time.sleep(0.05)
        else:
            raise Failure("PROVIDER_TIMEOUT", "The verification provider did not report ready.")
        request = {"service_name": "/add_two_ints", "service_type": "aos_examples/srv/AddTwoInts",
                   "values": {"a": 17, "b": 25}, "timeout": 5}
        response = json.loads(run([current["native_cli"], "service", "call", "--request", "-"],
                                  env=env, input=json.dumps(request), timeout=20))
        if response.get("ok") is not True or response.get("data", {}).get("response") != {"sum": 42}:
            raise Failure("SELF_TEST_FAILED", "The real Service call did not return sum: 42.")
        report.update(status="passed", service_response={"sum": 42}, port=port)
    except BaseException as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        for child in reversed(children):
            stop_child(child)
        for stream in files:
            stream.close()
        report["finished"] = time.time()
        report["children_stopped"] = all(child.poll() is not None for child in children)
        write_json(output / "result.json", report)
    return report


def install_dispatcher(prefix, python):
    # The dispatcher never changes with a release. Only current.json is switched.
    legacy_source = (
        "import json,os,pathlib,sys\n"
        "root=pathlib.Path(__file__).resolve().parent\n"
        "state=json.loads((root/'current.json').read_text(encoding='utf-8'))\n"
        "release=pathlib.Path(state['release']).resolve()\n"
        "assert release.is_relative_to((root/'releases').resolve())\n"
        "os.execv(state['python'],[state['python'],'-I',state['manager'],"
        "'_cli','--prefix',str(root),'--',*sys.argv[1:]])\n"
    )
    source = (
        "import json,os,pathlib,subprocess,sys\n"
        "root=pathlib.Path(__file__).resolve().parent\n"
        "state=json.loads((root/'current.json').read_text(encoding='utf-8'))\n"
        "release=pathlib.Path(state['release']).resolve()\n"
        "assert release.is_relative_to((root/'releases').resolve())\n"
        "command=[state['python'],'-X','utf8','-I',state['manager'],'_cli','--prefix',"
        "str(root),'--',*sys.argv[1:]]\n"
        "if os.name=='nt':\n"
        "    raise SystemExit(subprocess.call(command))\n"
        "os.execv(command[0],command)\n"
    )
    path = prefix / "dispatch.py"
    if path.exists() and path.read_text(encoding="utf-8") not in (source, legacy_source):
        raise Failure("ENTRYPOINT_CONFLICT", "The existing AOS dispatcher was modified.")
    write_text_if_changed(path, source)
    binary = prefix / "bin"
    binary.mkdir(exist_ok=True)
    if os.name == "nt":
        # cmd expands percent signs even inside quoted arguments.
        if any(c in str(prefix) + str(python) for c in "%\r\n"):
            raise Failure("UNSUPPORTED_PATH", "Use an installation path without %, CR, or LF.")
        shim = binary / "aos.cmd"
        # Keep the launcher ASCII: cmd uses its console code page to read batch
        # files. %~dp0 expands the actual Unicode directory without decoding it
        # from the batch file. A managed interpreter always lives in the prefix.
        try:
            interpreter = "%~dp0"+os.path.relpath(python, binary)
        except ValueError:
            interpreter = str(python)  # Direct developer invocation across drives.
        if not interpreter.isascii():
            raise Failure("UNSUPPORTED_PATH", "Use setup.ps1 to prepare a managed interpreter.")
        shim_source = (f'@echo off\r\nsetlocal DisableDelayedExpansion\r\n'
                       f'"{interpreter}" -X utf8 -I "%~dp0..\\dispatch.py" %*\r\n')
        write_text_if_changed(shim, shim_source)
    else:
        shim = binary / "aos"
        write_text_if_changed(shim, "#!/bin/sh\nexec " + shlex.quote(str(python)) + " -X utf8 -I "
                              + shlex.quote(str(path)) + ' "$@"\n', mode=0o755)
    return str(shim)


def install_skill(current, agent):
    roots = {"codex": Path.home()/".agents/skills",
             "claude": Path.home()/".claude/skills",
             "trae": Path.home()/".trae/skills"}
    if agent not in roots:
        raise Failure("UNKNOWN_AGENT", "Supported skill destinations: codex, claude, trae.")
    destination = roots[agent] / "aos"
    marker = destination / ".aos-managed-skill.json"
    if destination.exists() and not marker.is_file():
        raise Failure("SKILL_CONFLICT", f"Existing skill was not installed by AOS: {destination}")
    source = Path(current["manager"]).parent.parent
    roots[agent].mkdir(parents=True, exist_ok=True)
    stage = roots[agent] / (".aos-" + uuid.uuid4().hex)
    shutil.copytree(source, stage, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    write_json(stage / marker.name, {"manager_version": current["manager_version"],
                                    "prefix": current["prefix"]})
    # An installed standalone skill needs to locate custom prefixes too.
    write_json(stage / "scripts/installed-prefix.json", {"prefix": current["prefix"]})
    previous = roots[agent] / (".aos-backup-" + uuid.uuid4().hex)
    if destination.exists():
        os.replace(destination, previous)
    try:
        os.replace(stage, destination)
    except BaseException:
        if previous.exists():
            os.replace(previous, destination)
        raise
    if previous.exists():
        shutil.rmtree(previous)
    return str(destination)


def skill_preflight(agents):
    roots = {"codex": Path.home()/".agents/skills",
             "claude": Path.home()/".claude/skills",
             "trae": Path.home()/".trae/skills"}
    for agent in agents:
        if agent not in roots:
            raise Failure("UNKNOWN_AGENT", "Supported skill destinations: codex, claude, trae.")
        destination = roots[agent]/"aos"
        if destination.exists() and not (destination/".aos-managed-skill.json").is_file():
            raise Failure("SKILL_CONFLICT", f"Existing skill was not installed by AOS: {destination}")


def install(prefix, uv, *, archive=None, agents=(), force=False, package_root=PLUGIN):
    prefix = prefix.expanduser().resolve()
    distribution_path = package_root / "skills/aos/scripts/distribution.json"
    distribution = read_json(distribution_path)
    if distribution.get("schema_version") != 1:
        raise Failure("INVALID_MANIFEST", "Unsupported distribution manifest.")
    version(distribution["manager_version"])
    version(distribution["runtime_version"])
    host = host_platform()
    record = distribution["platforms"][host]
    manager_hash = tree_digest(package_root/"skills/aos")
    with lock(prefix):
        skill_preflight(agents)
        old = state(prefix, required=False, check_components=False)
        if old and not force and version(old["manager_version"]) > version(distribution["manager_version"]):
            old = state(prefix)
            result = self_test(old, prefix/"logs"/("verify-"+uuid.uuid4().hex))
            return dict(old, status="already_newer", verification=result,
                        skills=[install_skill(old, a) for a in agents])
        if (old and old["manager_digest"] == manager_hash
                and old["runtime_sha256"] == record["sha256"]):
            try:
                state(prefix)
                verify_bundle(Path(old["bundle"]), distribution, host)
                result = self_test(old, prefix/"logs"/("verify-"+uuid.uuid4().hex))
            except (Failure, OSError):
                log("The existing installation needs repair; preparing a new verified candidate.")
            else:
                return dict(old, status="already_installed", verification=result,
                            skills=[install_skill(old, a) for a in agents])
        if not uv or not Path(uv).is_file():
            raise Failure("UV_REQUIRED", "Use setup.sh/setup.ps1 to prepare the pinned uv runtime.")
        package = Path(archive).resolve() if archive else checked_download(
            distribution["runtime_tag"], record, prefix/"cache")
        if digest(package) != record["sha256"]:
            raise Failure("CHECKSUM_MISMATCH", "Runtime archive does not match the distribution.")
        releases = prefix/"releases"
        releases.mkdir(exist_ok=True)
        release = releases / (distribution["runtime_version"] + "-agent-"
                              + distribution["manager_version"] + "-" + uuid.uuid4().hex[:12])
        release.mkdir()
        try:
            bundle = extract_archive(package, release/"unpacked")
            manifest = verify_bundle(bundle, distribution, host)
            shutil.copytree(package_root, release/"agent",
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            venv = release/"venv"
            env = clean_environment()
            run([uv, "venv", "--no-config", "--python", sys.executable, venv], env=env)
            python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            wheels = list((bundle/"wheels").glob("*.whl"))
            if len(wheels) != 1:
                raise Failure("INVALID_PACKAGE", "Expected exactly one SDK wheel.")
            run([uv, "pip", "install", "--no-config", "--no-index", "--no-deps",
                 "--python", python, wheels[0]], env=env)
            manager = release/"agent/skills/aos/scripts/aos_manager.py"
            current = {
                "schema_version": 1, "prefix": str(prefix), "release": str(release),
                "runtime_version": distribution["runtime_version"],
                "manager_version": distribution["manager_version"],
                "runtime_sha256": record["sha256"], "manager_digest": manager_hash,
                "source_sha": manifest["source_sha"], "platform": host,
                "python": str(python), "uv": str(Path(uv).resolve()), "manager": str(manager),
                "native_cli": str(bundle/("bin/aos.exe" if os.name == "nt" else "bin/aos")),
                "router": str(bundle/("router/zenohd.exe" if os.name == "nt" else "router/zenohd")),
                "bundle": str(bundle), "installed_at": time.time(),
                "config": str(prefix/"config/session.json5"),
                "runtime_library_path": str(Path(sys.base_prefix)),
            }
            result = self_test(current, release/"verification")
            current["verification"] = result
            config = prefix/"config"
            config.mkdir(exist_ok=True)
            present = [(config/name).exists() for name in ("router.json5", "session.json5")]
            if not any(present):
                configs(config, 7447)
            elif not all(present):
                raise Failure("INCOMPLETE_CONFIG",
                              "Keep both config/router.json5 and config/session.json5, or move "
                              "the incomplete configuration aside before retrying.")
            current["cli"] = install_dispatcher(prefix, sys.executable)
            write_json(release/"installation.json", current)
            if old:
                write_json(prefix/"previous.json", old)
            write_json(prefix/"current.json", current)
        except BaseException:
            # Keep the failed candidate and logs for diagnosis; never activate it.
            log(f"Candidate did not activate. Inspect {release}")
            raise
        return dict(current, status="installed", skills=[install_skill(current, a) for a in agents])


def control(prefix, command="status"):
    path = prefix/"run/router.json"
    if not path.exists():
        return None
    record = read_json(path)
    try:
        with socket.create_connection(("127.0.0.1", record["control_port"]), timeout=3) as stream:
            stream.settimeout(20 if command == "stop" else 3)
            stream.sendall(json.dumps({"token": record["token"], "command": command}).encode()+b"\n")
            with stream.makefile("rb") as reader:
                value = json.loads(reader.readline(65536))
            if value.get("instance") != record["instance"]:
                return None
            return value
    except (OSError, ValueError, KeyError):
        return None


def supervise(prefix, current, token, instance):
    """The owner only stops its own Popen child; no saved PID is ever killed."""
    folder = prefix/"run"
    folder.mkdir(exist_ok=True)
    env = clean_environment()
    router_config = prefix/"config/router.json5"
    endpoint = read_json(router_config)["listen"]["endpoints"][0]
    if not re.fullmatch(r"tcp/127\.0\.0\.1:\d+", endpoint):
        raise Failure("UNMANAGED_CONFIG", "Managed Router requires a loopback TCP listener.")
    port = int(endpoint.rsplit(":", 1)[1])
    try:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
    except OSError as error:
        raise Failure("PORT_IN_USE", f"Port {port} is in use; no existing process was stopped.") from error
    with socket.socket() as server, (folder/"router.stdout").open("a") as stdout, \
            (folder/"router.stderr").open("a") as stderr:
        server.bind(("127.0.0.1", 0))
        server.listen(4)
        server.settimeout(1)
        child = subprocess.Popen([current["router"], "--config", str(router_config)],
                                 cwd=Path(current["router"]).parent, env=env,
                                 stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
        try:
            wait_port(port, child)
            # A listening foreign process does not prove this child owns the port.
            time.sleep(0.25)
            if child.poll() is not None:
                raise Failure("ROUTER_START_FAILED", "Router could not bind its configured port.")
            record = {"instance": instance, "token": token,
                      "control_port": server.getsockname()[1], "router_port": port,
                      "pid": child.pid, "runtime_version": current["runtime_version"],
                      "release": current["release"], "started": time.time(),
                      "config_sha256": digest(router_config)}
            write_json(folder/"router.json", record)
            while child.poll() is None:
                try:
                    stream, _ = server.accept()
                except socket.timeout:
                    continue
                with stream:
                    stream.settimeout(2)
                    try:
                        with stream.makefile("rb") as reader:
                            request = json.loads(reader.readline(65536))
                        if not secrets.compare_digest(str(request.get("token", "")), token):
                            continue
                        command = request.get("command")
                        if command == "stop":
                            stop_child(child)
                        response = {k: v for k, v in record.items() if k != "token"}
                        response["status"] = "stopped" if child.poll() is not None else "running"
                        stream.sendall(json.dumps(response).encode()+b"\n")
                        if command == "stop":
                            return
                    except (OSError, ValueError):
                        continue
        finally:
            stop_child(child)
            marker = folder/"router.json"
            if marker.exists() and read_json(marker).get("instance") == instance:
                marker.unlink()


def ensure_environment(prefix, current):
    with lock(prefix, "router"):
        running = control(prefix)
        if running and running["status"] == "running":
            if running["config_sha256"] != digest(prefix/"config/router.json5"):
                raise Failure("ROUTER_CONFIG_CHANGED",
                              "Run aos env stop, then aos env ensure to apply the changed config.")
            return dict(running, config=current["config"], reused=True)
        folder = prefix/"run"
        folder.mkdir(exist_ok=True)
        token, instance = secrets.token_hex(32), uuid.uuid4().hex
        request = folder/("start-"+instance+".json")
        write_json(request, {"current": current, "token": token, "instance": instance})
        with (folder/"supervisor.log").open("a") as output:
            options = {"start_new_session": True} if os.name != "nt" else {
                "creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS}
            process = subprocess.Popen(
                [current["python"], "-X", "utf8", "-I", current["manager"], "_supervise",
                 "--prefix", str(prefix), "--request", str(request)],
                stdin=subprocess.DEVNULL, stdout=output, stderr=output, **options)
        deadline = time.monotonic()+20
        try:
            while time.monotonic() < deadline:
                running = control(prefix)
                if running and running["instance"] == instance:
                    return dict(running, config=current["config"], reused=False)
                if process.poll() is not None:
                    raise Failure("ROUTER_START_FAILED",
                                  f"Inspect {folder/'supervisor.log'} and {folder/'router.stderr'}.")
                time.sleep(0.1)
            stop_child(process)
            raise Failure("ROUTER_START_TIMEOUT", "Owned Router did not become ready.")
        finally:
            request.unlink(missing_ok=True)


def find_release(requested=None):
    # A small public release catalog avoids GitHub API authentication and rate
    # limits. It is published with the source; the archives carry pinned hashes.
    ref = "main"
    if requested is not None:
        version(requested)
        ref = RELEASE_PREFIX + requested
    url = f"https://raw.githubusercontent.com/{REPOSITORY}/{ref}/distributions/stable.json"
    try:
        with urllib.request.urlopen(public_request(url), timeout=30) as response:
            candidate = json.load(response)
    except (urllib.error.URLError, TimeoutError) as error:
        raise Failure("UPDATE_CHECK_FAILED", f"Cannot read the public release catalog: {error}") from error
    if candidate.get("schema_version") != 1:
        raise Failure("INVALID_MANIFEST", "Unsupported release catalog.")
    target = candidate["manager_version"]
    version(target)
    version(candidate["runtime_version"])
    if requested is not None and target != requested:
        raise Failure("VERSION_MISMATCH", "Release catalog differs from the requested version.")
    tag = RELEASE_PREFIX + target
    return dict(candidate, tag_name=tag,
                html_url=f"https://github.com/{REPOSITORY}/releases/tag/{tag}")


def update_check(current, requested=None):
    release = find_release(requested)
    target = release["tag_name"][len(RELEASE_PREFIX):]
    return {"installed": current["manager_version"], "available": target,
            "update_available": version(target) > version(current["manager_version"]),
            "runtime_version": current["runtime_version"],
            "available_runtime_version": release["runtime_version"],
            "release_url": release["html_url"],
            "channel": "stable", "version_scope": "agent distribution and its pinned runtime"}


def update_apply(prefix, current, request):
    release = find_release(request.get("version"))
    target_version = release["tag_name"][len(RELEASE_PREFIX):]
    if version(target_version) < version(current["manager_version"]):
        raise Failure("DOWNGRADE_REQUIRES_ROLLBACK", "Use update rollback for the previous installation.")
    if target_version == current["manager_version"]:
        return {"status": "up_to_date", "manager_version": target_version}
    name = f"aos-agent-{target_version}.zip"
    asset = next((a for a in release["assets"] if a["name"] == name), None)
    if not asset:
        raise Failure("RELEASE_INCOMPLETE", "Agent distribution ZIP is missing.")
    asset_digest = asset.get("digest", "")
    if not re.fullmatch("sha256:[a-f0-9]{64}", asset_digest):
            raise Failure("RELEASE_INCOMPLETE", "Release catalog has no valid asset SHA256.")
    archive = checked_download(release["tag_name"],
                               {"asset": name, "sha256": asset_digest.split(":")[1]}, prefix/"cache")
    with tempfile.TemporaryDirectory(prefix="aos-update-", dir=prefix/"cache") as temp:
        plugin = extract_archive(archive, Path(temp)/"unpacked")
        manager = plugin/"skills/aos/scripts/aos_manager.py"
        manifest = read_json(manager.parent/"distribution.json")
        if manifest["manager_version"] != target_version:
            raise Failure("VERSION_MISMATCH", "Archive version differs from the selected release.")
        # The new installer owns its own migration rules. It keeps the previous
        # current.json until verification succeeds.
        setup = manager.parent/("setup.ps1" if os.name == "nt" else "setup.sh")
        command = (["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", setup]
                   if os.name == "nt" else ["sh", setup])
        output = run([*command, "--prefix", prefix, "--non-interactive", "--json"], timeout=900)
        result = json.loads(output)
        if result.get("ok") is not True:
            raise Failure("UPDATE_FAILED", "New installer did not report success.")
        return result["data"]


def rollback(prefix):
    with lock(prefix):
        previous = read_json(prefix/"previous.json") if (prefix/"previous.json").exists() else None
        if not previous:
            raise Failure("NO_PREVIOUS_VERSION", "No previous verified installation is available.")
        old = state(prefix)
        release = Path(previous["release"]).resolve()
        if (not release.is_relative_to((prefix/"releases").resolve())
                or read_json(release/"installation.json") != previous):
            raise Failure("INVALID_STATE", "Previous installation is inconsistent.")
        distribution = read_json(Path(previous["manager"]).parent/"distribution.json")
        verify_bundle(Path(previous["bundle"]), distribution, previous["platform"])
        result = self_test(previous, prefix/"logs"/("rollback-"+uuid.uuid4().hex))
        write_json(prefix/"previous.json", old)
        write_json(prefix/"current.json", previous)
        return {"status": "rolled_back", "manager_version": previous["manager_version"],
                "runtime_version": previous["runtime_version"], "verification": result}


MANAGED_COMMANDS = {
    "env": {"status": {}, "ensure": {}, "stop": {}, "test": {}},
    "update": {"check": {"version": {"type": "string"}},
               "apply": {"version": {"type": "string"}}, "rollback": {}},
}


def command_schema(group, verb):
    if group not in MANAGED_COMMANDS or verb not in MANAGED_COMMANDS[group]:
        raise Failure("UNKNOWN_COMMAND", "See aos --help.", exit_code=2)
    return {"command": f"aos {group} {verb}",
            "request_schema": {"type": "object", "properties": MANAGED_COMMANDS[group][verb],
                               "additionalProperties": False},
            "output_format": "json", "exit_codes": {"0": "success", "1": "operation failed",
                                                   "2": "invalid input"}}


def parse_request(args, group, verb):
    if not args:
        value = {}
    elif len(args) == 2 and args[0] == "--request":
        value = json.loads(sys.stdin.read() if args[1] == "-" else Path(args[1]).read_text())
    else:
        raise Failure("INVALID_INPUT", "Use --request PATH|- for JSON input.", exit_code=2)
    allowed = MANAGED_COMMANDS[group][verb]
    if (not isinstance(value, dict) or set(value)-set(allowed)
            or any(not isinstance(v, str) for v in value.values())):
        raise Failure("INVALID_INPUT", "Request does not match command schema.", exit_code=2)
    return value


def cli(prefix, args):
    current = state(prefix)
    if args in (["--version"], ["version"]):
        emit({"runtime_version": current["runtime_version"],
              "manager_version": current["manager_version"], "platform": current["platform"]})
        return
    if len(args) >= 2 and args[0] == "schema" and args[1] in MANAGED_COMMANDS:
        if len(args) != 3:
            raise Failure("INVALID_INPUT", "Use aos schema GROUP VERB.", exit_code=2)
        emit(command_schema(args[1], args[2]))
        return
    if args and args[0] in MANAGED_COMMANDS:
        group = args[0]
        if len(args) < 2 or "--help" in args:
            emit({"commands": [command_schema(group, verb) for verb in MANAGED_COMMANDS[group]]})
            return
        verb = args[1]
        command_schema(group, verb)
        request = parse_request(args[2:], group, verb)
        if (group, verb) == ("env", "status"):
            emit({"installation": current, "router": control(prefix) or {"status": "stopped"}})
        elif (group, verb) == ("env", "ensure"):
            emit(ensure_environment(prefix, current))
        elif (group, verb) == ("env", "stop"):
            with lock(prefix, "router"):
                emit(control(prefix, "stop") or {"status": "not_running"})
        elif (group, verb) == ("env", "test"):
            emit(self_test(current, prefix/"logs"/("self-test-"+uuid.uuid4().hex)))
        elif (group, verb) == ("update", "check"):
            emit(update_check(current, request.get("version")))
        elif (group, verb) == ("update", "apply"):
            emit(update_apply(prefix, current, request))
        elif (group, verb) == ("update", "rollback"):
            emit(rollback(prefix))
        return
    env = dict(os.environ)
    env.setdefault("AOS_ZENOH_CONFIG", current["config"])
    env.setdefault("AOS_DOMAIN_ID", "0")
    if os.name == "nt":
        env["PATH"] = current["runtime_library_path"] + os.pathsep + env.get("PATH", "")
    if not args or args in (["--help"], ["help"]):
        native = json.loads(run([current["native_cli"], "--help"], env=env))
        native["data"]["installation_commands"] = [
            command_schema(group, verb) for group, verbs in MANAGED_COMMANDS.items() for verb in verbs]
        native["data"]["versions"] = {k: current[k] for k in ("runtime_version", "manager_version")}
        print(json.dumps(native), flush=True)
        return
    # Preserve exit codes, streams, signals and JSONL shape of the native CLI.
    if os.name != "nt":
        os.execve(current["native_cli"], [current["native_cli"], *args], env)
    else:
        raise SystemExit(subprocess.call([current["native_cli"], *args], env=env))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["install", "_cli", "_supervise"])
    parser.add_argument("--prefix", type=Path, default=default_prefix())
    parser.add_argument("--uv", default=shutil.which("uv"))
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--agent", action="append", choices=["codex", "claude", "trae"], default=[])
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--non-interactive", action="store_true")
    parser.add_argument("--request", type=Path)
    arguments, tail = parser.parse_known_args()
    prefix = arguments.prefix.expanduser().resolve()
    try:
        if arguments.mode == "install":
            if tail:
                raise Failure("INVALID_INPUT", "Unknown installer arguments: " + " ".join(tail), exit_code=2)
            if (PLUGIN/"plugin.json").is_file() and read_json(PLUGIN/"plugin.json").get("name") == "aos":
                emit(install(prefix, arguments.uv, archive=arguments.archive, agents=arguments.agent))
            else:
                # Never copy unrelated user skills into the AOS installation.
                with tempfile.TemporaryDirectory(prefix="aos-skill-") as temp:
                    root = Path(temp)
                    shutil.copytree(HERE.parent, root/"skills/aos",
                                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc",
                                                                 "installed-prefix.json",
                                                                 ".aos-managed-skill.json"))
                    emit(install(prefix, arguments.uv, archive=arguments.archive,
                                 agents=arguments.agent, package_root=root))
        elif arguments.mode == "_supervise":
            request = read_json(arguments.request)
            supervise(prefix, request["current"], request["token"], request["instance"])
        else:
            cli(prefix, tail[1:] if tail[:1] == ["--"] else tail)
    except Failure as error:
        emit({"code": error.code, "message": str(error)}, ok=False)
        raise SystemExit(error.exit_code)
    except (OSError, ValueError, KeyError, TypeError, tarfile.TarError, zipfile.BadZipFile) as error:
        emit({"code": "OPERATION_FAILED", "message": str(error)}, ok=False)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
