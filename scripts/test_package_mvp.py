"""Archive checks using fake artifacts; never builds or executes AOS or a router."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("aos_package_mvp", ROOT / "scripts/package_mvp.py")
PACKAGER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PACKAGER)

# Keep this delivery contract independent of the implementation's allowlist.
PUBLIC_FILES = (
    "scripts/install_demo_package.py", "scripts/install_memory_package.py",
    "examples/launch/provider.py", "examples/launch/consumer.py",
    "examples/launch/Label.json", "examples/launch/Status.json",
    "examples/memory_server/provider.py", "examples/memory_server/consumer.py",
    "examples/memory_server/memory_store.py",
    "examples/memory_server/request_observer.py",
    "examples/memory_server/interfaces/Put.json",
    "examples/memory_server/interfaces/Get.json",
    "examples/memory_server/interfaces/List.json",
    "examples/memory_server/interfaces/Status.json",
    "examples/memory_server/interfaces/Export.json",
    "examples/memory_server/interfaces/RequestTelemetry.json",
)


def sha(data):
    return hashlib.sha256(data).hexdigest()


class PlatformBundleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="aos-package-fixture-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "fixture-source"
        self.artifacts = self.root / "fixture-artifacts"
        self.source.mkdir()
        self.artifacts.mkdir()
        for directory in ("config", "examples", "interfaces", "docs", "upstream"):
            (self.source / directory).mkdir()
        self.write(self.source / "Cargo.toml", '[workspace.package]\nversion = "0.2.0"\n')
        for relative in (
            "README.md", "docs/install.md", "docs/known-limitations.md",
            "upstream/source-map.json", "config/zenoh-session.json5",
            "config/zenoh-router.json5", "config/zenoh-router-windows.json5",
            "upstream/licenses/fixture-LICENSE",
        ):
            self.write(self.source / relative, "diagnostic fixture: " + relative + "\n")
        for relative in PUBLIC_FILES:
            destination = self.source / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, destination)
        self.write(self.source / "scripts/private-build.py", "raise RuntimeError('must not ship')\n")
        for library in ("zenoh", "zenoh-link-tls"):
            for filename in ("LICENSE", "NOTICE.md", "AOS-PATCH.md"):
                self.write(self.source / "vendor" / library / filename,
                           f"fixture {library}/{filename}\n")
        self.git("init", "--quiet")
        self.git("add", ".")
        self.git("-c", "user.name=Packaging Fixture", "-c", "user.email=fixture@example.invalid",
                 "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false",
                 "commit", "--quiet", "-m", "diagnostic fixture")
        self.revision = self.git("rev-parse", "HEAD").strip()
        self.args = argparse.Namespace(
            source_repo=self.source, artifacts=self.artifacts,
            output=self.root / "diagnostic-packages", platform="macos-arm64",
            source_sha="1" * 40,
        )
        self.proof = {
            "source_sha": self.args.source_sha, "target": "aarch64-apple-darwin",
            "status": "diagnostic_fixture_not_acceptance",
            "commands": [{"returncode": 1, "command": ["fixture-do-not-execute"]}],
        }
        self.prepare_artifacts()

    @staticmethod
    def write(path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.source), *args], check=True,
                              capture_output=True, text=True).stdout

    def save_proof(self):
        self.write(self.artifacts / "provenance.json", json.dumps(self.proof, indent=2) + "\n")

    def prepare_artifacts(self, platform="macos-arm64"):
        target, cli, tag = {
            "macos-arm64": ("aarch64-apple-darwin", "aos", "macosx_11_0_arm64"),
            "windows-x86_64": ("x86_64-pc-windows-msvc", "aos.exe", "win_amd64"),
        }[platform]
        self.args.platform = platform
        self.proof["target"] = target
        for previous in (self.artifacts / "wheels").glob("*.whl"):
            previous.unlink()
        for key, relative in (
            ("cli", "cli/" + cli),
            ("wheel", f"wheels/aos_sdk-0.2.0-cp311-abi3-{tag}.whl"),
        ):
            self.write(self.artifacts / relative, f"FAKE {key}; not runnable or installable.\n")
            self.proof[key] = {"path": relative, "sha256": sha((self.artifacts / relative).read_bytes())}
        self.save_proof()

    def binding_evidence(self):
        folder = self.artifacts / "installed-test-bindings"
        self.proof["installed_test_bindings"] = {}
        for name in ("record", "manifest"):
            path = folder / (name + ".json")
            self.write(path, json.dumps({"status": "fixture_only",
                                        "historical_absolute_path": "/original/host/path"}) + "\n")
            self.proof["installed_test_bindings"].update({
                name: path.relative_to(self.artifacts).as_posix(), name + "_sha256": sha(path.read_bytes()),
            })
        self.write(folder / "fixed-source/example.py", "# fixture proof snapshot\n")
        self.write(folder / "fixed-source/__pycache__/evidence.pyc", "fixture recorded bytes; never execute\n")
        self.write(self.artifacts / "logs/fixture.log", "fixture; no acceptance command ran\n")
        self.save_proof()

    def archive_files(self):
        archive = PACKAGER.package(self.args)
        expected, name = Path(str(archive) + ".sha256").read_text().split()
        self.assertEqual((expected, name), (sha(archive.read_bytes()), archive.name))
        if archive.suffix == ".zip":
            with zipfile.ZipFile(archive) as zipped:
                files = {str(PurePosixPath(name).relative_to(PurePosixPath(name).parts[0])): zipped.read(name)
                         for name in zipped.namelist()}
        else:
            with tarfile.open(archive, "r:gz") as tar:
                files = {str(PurePosixPath(member.name).relative_to(PurePosixPath(member.name).parts[0])):
                         tar.extractfile(member).read() for member in tar.getmembers() if member.isfile()}
                cli = next(member for member in tar.getmembers() if member.name.endswith("/bin/aos"))
                self.assertEqual(cli.mode & 0o777, 0o755)
        manifest = json.loads(files["manifest.json"])
        self.assertEqual(set(manifest["files"]), set(files) - {"manifest.json"})
        for relative, record in manifest["files"].items():
            self.assertEqual(record, {"sha256": sha(files[relative]), "bytes": len(files[relative])})
        return files, manifest

    def router_fixture(self, *, extra=None):
        self.args.bundle_router = True
        name, _ = PACKAGER.ROUTER_ASSETS[self.args.platform]
        executable = "zenohd.exe" if self.args.platform == "windows-x86_64" else "zenohd"
        members = {
            executable: b"FAKE native-tested router; not executable\n",
            "deps/runtime.dll": b"FAKE adjacent runtime; preserve exact bytes\n",
            "NOTICE": b"FAKE upstream license notice\n",
        }
        if extra:
            members.update(extra)
        archive = self.artifacts / "zenohd" / name
        archive.parent.mkdir(exist_ok=True)
        with zipfile.ZipFile(archive, "w") as zipped:
            for relative, data in members.items():
                zipped.writestr(relative, data)
        binary = self.artifacts / "zenohd/unpacked" / executable
        binary.parent.mkdir(exist_ok=True)
        binary.write_bytes(members[executable])
        digest = sha(archive.read_bytes())
        self.proof["zenohd"] = {
            "version": "1.10.1", "expected_sha256": digest, "actual_sha256": digest,
            "archive": archive.relative_to(self.artifacts).as_posix(),
            "executable": binary.relative_to(self.artifacts).as_posix(),
            "executable_sha256": sha(binary.read_bytes()),
        }
        self.save_proof()
        return name, digest, members

    def test_bundled_router_preserves_full_upstream_distribution_and_tested_bytes(self):
        for platform in ("macos-arm64", "windows-x86_64"):
            with self.subTest(platform=platform):
                self.prepare_artifacts(platform)
                self.args.output = self.root / platform
                name, digest, members = self.router_fixture()
                with patch.dict(PACKAGER.ROUTER_ASSETS, {platform: (name, digest)}):
                    files, manifest = self.archive_files()
                for relative, data in members.items():
                    self.assertEqual(files["router/" + relative], data)
                self.assertTrue(manifest["zenohd"]["bundled"])
                self.assertEqual(manifest["zenohd"]["upstream_archive_sha256"], digest)
                self.assertEqual(manifest["zenohd"]["executable_sha256"],
                                 self.proof["zenohd"]["executable_sha256"])
                self.assertEqual(manifest["acceptance_status"], "diagnostic_fixture_not_acceptance")

    def test_bundling_refuses_unpinned_archive_and_changed_native_router(self):
        name, digest, _ = self.router_fixture()
        with self.assertRaisesRegex(ValueError, "pinned platform"):
            PACKAGER.package(self.args)
        binary = self.artifacts / self.proof["zenohd"]["executable"]
        binary.write_bytes(b"different native router\n")
        with patch.dict(PACKAGER.ROUTER_ASSETS, {self.args.platform: (name, digest)}):
            with self.assertRaisesRegex(ValueError, "SHA256 mismatch"):
                PACKAGER.package(self.args)
        self.assertEqual(list(self.args.output.glob("*.tar.gz")), [])

    def test_bundling_refuses_archive_paths_outside_router_directory(self):
        name, digest, _ = self.router_fixture(extra={"../escaped.txt": b"must never be extracted"})
        with patch.dict(PACKAGER.ROUTER_ASSETS, {self.args.platform: (name, digest)}):
            with self.assertRaisesRegex(ValueError, "Unsafe or duplicate"):
                PACKAGER.package(self.args)
        self.assertFalse(any(self.args.output.rglob("escaped.txt")))
        self.assertEqual(list(self.args.output.glob("*.tar.gz")), [])

    def test_tar_and_zip_contain_public_installers_resources_and_both_vendor_records(self):
        for platform in ("macos-arm64", "windows-x86_64"):
            with self.subTest(platform=platform):
                self.prepare_artifacts(platform)
                self.args.output = self.root / platform
                files, manifest = self.archive_files()
                for relative in PUBLIC_FILES:
                    self.assertIn(relative, list(files))
                    self.assertEqual(files[relative], (self.source / relative).read_bytes())
                self.assertNotIn("scripts/private-build.py", files)
                for library in ("zenoh", "zenoh-link-tls"):
                    for filename in ("LICENSE", "NOTICE.md", "AOS-PATCH.md"):
                        self.assertEqual(files[f"upstream/licenses/{library}-1.10.1/{filename}"],
                                         (self.source / "vendor" / library / filename).read_bytes())
                self.assertEqual(manifest["source_sha"], "1" * 40)
                self.assertEqual(manifest["validation_source_sha"], "1" * 40)
                self.assertEqual(manifest["packaging_revision"], self.revision)
                self.assertNotEqual(manifest["source_sha"], manifest["packaging_revision"])
                self.assertEqual(manifest["packaging_script_sha256"], sha(Path(PACKAGER.__file__).read_bytes()))
                self.assertEqual(manifest["packaging_tracked_changes"], "")
                self.assertFalse(manifest["packaging_runs_acceptance"])
                self.assertFalse(any(name.startswith("evidence/") for name in files))

    def test_manifest_distinguishes_artifact_build_and_validation_sources(self):
        self.proof["artifact_build_source_sha"] = "2" * 40
        self.save_proof()
        files, manifest = self.archive_files()
        self.assertEqual(manifest["source_sha"], "2" * 40)
        self.assertEqual(manifest["validation_source_sha"], "1" * 40)
        self.assertEqual(manifest["packaging_revision"], self.revision)
        self.assertNotIn("acceptance_provenance", manifest)
        self.assertFalse(manifest["packaging_runs_acceptance"])
        self.assertEqual(manifest["acceptance_status"], self.proof["status"])

    def test_nonpassing_diagnostic_status_is_preserved_without_promoting_acceptance(self):
        for status in ("running", "failed", "not_validated"):
            with self.subTest(status=status):
                self.proof["status"] = status
                self.save_proof()
                self.args.output = self.root / status
                files, manifest = self.archive_files()
                self.assertEqual(manifest["acceptance_status"], status)
                self.assertNotIn("evidence/provenance.json", files)
                self.assertFalse(manifest["packaging_runs_acceptance"])

    def test_private_build_records_are_not_published(self):
        self.binding_evidence()
        self.proof["hostname"] = "private-build-host"
        self.save_proof()
        for relative in (
            "docs/evidence/review.md", "docs/platform/evidence/run/result.json",
            "docs/logs/build.log", "docs/installed-test-bindings/record.json",
        ):
            self.write(self.source / relative, "private-build-host /original/host/path\n")
        files, manifest = self.archive_files()
        self.assertFalse(any("evidence" in PurePosixPath(name).parts for name in files))
        self.assertFalse(any("logs" in PurePosixPath(name).parts for name in files))
        self.assertFalse(any("installed-test-bindings" in PurePosixPath(name).parts for name in files))
        self.assertNotIn("acceptance_provenance", manifest)
        for content in files.values():
            self.assertNotIn(b"private-build-host", content)
            self.assertNotIn(b"/original/host/path", content)

    def test_absent_optional_binding_evidence_does_not_block_historical_artifacts(self):
        files, manifest = self.archive_files()
        self.assertFalse(any(name.startswith("evidence/installed-test-bindings/") for name in files))
        self.assertEqual(manifest["acceptance_status"], "diagnostic_fixture_not_acceptance")

    def test_each_installer_and_its_required_resources_must_exist(self):
        for relative in PUBLIC_FILES:
            with self.subTest(relative=relative):
                path = self.source / relative
                saved = path.read_bytes()
                path.unlink()
                try:
                    with self.assertRaisesRegex(ValueError, "installer or required resource"):
                        PACKAGER.package(self.args)
                    self.assertFalse(self.args.output.exists())
                finally:
                    path.write_bytes(saved)

    def test_each_native_vendor_record_is_required(self):
        for library in ("zenoh", "zenoh-link-tls"):
            for name in ("LICENSE", "NOTICE.md", "AOS-PATCH.md"):
                path = self.source / "vendor" / library / name
                saved = path.read_bytes()
                path.unlink()
                try:
                    with self.subTest(library=library, name=name), self.assertRaisesRegex(ValueError, "license or modification"):
                        PACKAGER.package(self.args)
                finally:
                    path.write_bytes(saved)

    def test_declared_binding_proof_must_exist_and_match_hash(self):
        self.binding_evidence()
        path = self.artifacts / "installed-test-bindings/record.json"
        original = path.read_bytes()
        for mutation in (None, b"changed proof\n"):
            with self.subTest(mutation=mutation):
                path.unlink()
                if mutation is not None:
                    path.write_bytes(mutation)
                with self.assertRaisesRegex(ValueError, "missing or SHA256 mismatch"):
                    PACKAGER.package(self.args)
                path.write_bytes(original)
                self.assertFalse(self.args.output.exists())

    def test_declared_binding_proof_cannot_escape_its_fixed_location(self):
        self.binding_evidence()
        for path in ("../outside.json", "/absolute/record.json", "logs/record.json"):
            with self.subTest(path=path):
                self.proof["installed_test_bindings"]["record"] = path
                self.save_proof()
                with self.assertRaisesRegex(ValueError, "provenance artifact path"):
                    PACKAGER.package(self.args)

    def test_binding_inputs_are_rechecked_before_archive_creation(self):
        self.binding_evidence()
        copy_file = PACKAGER.copy_file

        def corrupt_copy(source, destination):
            copy_file(source, destination)
            if destination.suffix == ".whl":
                (self.artifacts / "installed-test-bindings/manifest.json").write_bytes(b"changed input\n")

        with patch.object(PACKAGER, "copy_file", side_effect=corrupt_copy):
            with self.assertRaisesRegex(ValueError, "SHA256 mismatch"):
                PACKAGER.package(self.args)
        self.assertEqual(list(self.args.output.iterdir()), [])

    def test_symlink_installer_is_rejected(self):
        path = self.source / "scripts/install_memory_package.py"
        if os.name == "nt":
            # Creating Windows symlinks needs a host privilege. Exercise the
            # rejection branch with an existing ordinary file and link metadata.
            is_symlink = Path.is_symlink
            with patch.object(Path, "is_symlink", lambda item: item == path or is_symlink(item)):
                with self.assertRaisesRegex(ValueError, "installer or required resource"):
                    PACKAGER.package(self.args)
            self.assertFalse(self.args.output.exists())
            return
        original = path.read_bytes()
        path.unlink()
        target = self.root / "outside-installer.py"
        target.write_bytes(original)
        path.symlink_to(target)
        with self.assertRaisesRegex(ValueError, "installer or required resource"):
            PACKAGER.package(self.args)


if __name__ == "__main__":
    unittest.main()
