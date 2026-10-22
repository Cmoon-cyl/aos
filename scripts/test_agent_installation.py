"""Installer transaction and archive tests; no network or native build needed."""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


manager = load("aos_manager_test", ROOT/"plugins/aos/skills/aos/scripts/aos_manager.py")
packager = load("aos_agent_packager_test", ROOT/"scripts/package_agent.py")
HOST = "windows-x86_64" if os.name == "nt" else "linux-x86_64"
EXE = ".exe" if os.name == "nt" else ""
PASSED = {"status": "passed", "service_response": {"sum": 42}, "children_stopped": True}


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.prefix = self.root/"install space"
        self.plugin = self.root/"plugin"
        scripts = self.plugin/"skills/aos/scripts"
        scripts.mkdir(parents=True)
        (scripts/"aos_manager.py").write_text("# test installer\n")
        (scripts/"setup.sh").write_text("#!/bin/sh\nexit 0\n")
        (scripts/"setup.ps1").write_text("exit 0\n")
        (self.plugin/"plugin.json").write_text('{"name":"aos","version":"0.1.0"}')
        self.archive = self.root/"bundle.tar.gz"
        files = {"bin/aos"+EXE: b"native-cli",
                 "router/zenohd"+EXE: b"native-router", "wheels/aos.whl": b"sdk-wheel"}
        manifest = {"version": "0.2.0", "platform": HOST, "acceptance_status": "passed",
                    "packaging_tracked_changes": [], "zenohd": {"bundled": True},
                    "source_sha": "a"*40,
                    "files": {name: {"bytes": len(value),
                                     "sha256": hashlib.sha256(value).hexdigest()}
                              for name, value in files.items()}}
        files["manifest.json"] = json.dumps(manifest).encode()
        with tarfile.open(self.archive, "w:gz") as output:
            for name, content in files.items():
                info = tarfile.TarInfo("bundle/"+name)
                info.size, info.mode = len(content), 0o755
                output.addfile(info, io.BytesIO(content))
        self.distribution = {
            "schema_version": 1, "manager_version": "0.1.0", "runtime_version": "0.2.0",
            "runtime_tag": "v0.2.0", "platforms": {
                HOST: {"asset": self.archive.name, "sha256": manager.digest(self.archive)}}}
        self.write_distribution()
        self.addCleanup(patch.stopall)
        patch.object(manager, "host_platform", return_value=HOST).start()
        self.self_test = patch.object(manager, "self_test", return_value=dict(PASSED)).start()
        self.run = patch.object(manager, "run", side_effect=self.fake_run).start()

    def write_distribution(self):
        (self.plugin/"skills/aos/scripts/distribution.json").write_text(
            json.dumps(self.distribution))

    def fake_run(self, command, **kwargs):
        if len(command) > 1 and command[1] == "venv":
            python = Path(command[-1])/("Scripts/python.exe" if os.name == "nt" else "bin/python")
            python.parent.mkdir(parents=True)
            python.write_text("test interpreter")
        return ""

    def install(self):
        return manager.install(self.prefix, sys.executable, archive=self.archive,
                               package_root=self.plugin)

    def test_first_install_activates_only_verified_candidate(self):
        result = self.install()
        self.assertEqual(result["status"], "installed")
        self.assertEqual(result["verification"]["service_response"], {"sum": 42})
        self.assertEqual(manager.state(self.prefix)["release"], result["release"])
        self.assertFalse((self.prefix/"previous.json").exists())

    def test_failed_first_install_never_creates_current(self):
        self.self_test.side_effect = manager.Failure("TEST_FAILED", "test failure")
        with self.assertRaises(manager.Failure):
            self.install()
        self.assertFalse((self.prefix/"current.json").exists())

    def test_failed_upgrade_preserves_current_and_config(self):
        before = self.install()
        original = (self.prefix/"current.json").read_bytes()
        config = self.prefix/"config/session.json5"
        config.write_text('{"user":"configuration"}')
        self.distribution["manager_version"] = "0.1.1"
        self.write_distribution()
        self.self_test.side_effect = manager.Failure("TEST_FAILED", "test failure")
        with self.assertRaises(manager.Failure):
            self.install()
        self.assertEqual((self.prefix/"current.json").read_bytes(), original)
        self.assertEqual(config.read_text(), '{"user":"configuration"}')
        self.assertEqual(manager.state(self.prefix)["release"], before["release"])

    def test_upgrade_and_rollback_keep_distinct_verified_versions(self):
        first = self.install()
        self.distribution["manager_version"] = "0.1.1"
        self.write_distribution()
        second = self.install()
        self.assertNotEqual(first["release"], second["release"])
        self.assertEqual(manager.read_json(self.prefix/"previous.json")["release"], first["release"])
        result = manager.rollback(self.prefix)
        self.assertEqual(result["manager_version"], "0.1.0")
        self.assertEqual(manager.state(self.prefix)["release"], first["release"])
        self.assertEqual(manager.read_json(self.prefix/"previous.json")["release"], second["release"])

    def test_failed_rollback_preserves_active_version(self):
        self.install()
        self.distribution["manager_version"] = "0.1.1"
        self.write_distribution()
        self.install()
        original = (self.prefix/"current.json").read_bytes()
        self.self_test.side_effect = manager.Failure("TEST_FAILED", "rollback test failure")
        with self.assertRaises(manager.Failure):
            manager.rollback(self.prefix)
        self.assertEqual((self.prefix/"current.json").read_bytes(), original)

    def test_repeat_install_rechecks_real_service_without_new_release(self):
        first = self.install()
        self.self_test.reset_mock()
        second = self.install()
        self.assertEqual(second["status"], "already_installed")
        self.assertEqual(first["release"], second["release"])
        self.self_test.assert_called_once()

    def test_missing_component_is_repaired_with_new_candidate(self):
        first = self.install()
        Path(first["native_cli"]).unlink()
        second = self.install()
        self.assertEqual(second["status"], "installed")
        self.assertNotEqual(first["release"], second["release"])
        self.assertTrue(Path(second["native_cli"]).is_file())

    def test_old_setup_does_not_downgrade_newer_installation(self):
        self.distribution["manager_version"] = "0.1.1"
        self.write_distribution()
        first = self.install()
        self.distribution["manager_version"] = "0.1.0"
        self.write_distribution()
        second = self.install()
        self.assertEqual(second["status"], "already_newer")
        self.assertEqual(first["release"], second["release"])

    def test_corrupt_archive_is_rejected_before_activation(self):
        self.archive.write_bytes(b"not the expected archive")
        with self.assertRaisesRegex(manager.Failure, "does not match"):
            self.install()
        self.assertFalse((self.prefix/"current.json").exists())

    def test_modified_bundle_payload_is_rejected(self):
        installed = self.install()
        Path(installed["native_cli"]).write_bytes(b"modified")
        with self.assertRaises(manager.Failure):
            manager.verify_bundle(Path(installed["bundle"]), self.distribution, HOST)

    def test_generated_python_cache_does_not_block_repeat_install_or_rollback(self):
        first = self.install()
        cache = Path(first["bundle"])/"examples/__pycache__"
        cache.mkdir(parents=True)
        (cache/"add_two_ints_server.cpython-311.pyc").write_bytes(b"generated bytecode")
        self.assertEqual(self.install()["status"], "already_installed")
        self.distribution["manager_version"] = "0.1.1"
        self.write_distribution()
        self.install()
        self.assertEqual(manager.rollback(self.prefix)["manager_version"], "0.1.0")

    def test_standalone_skill_reuses_runtime_and_preserves_rollback_target(self):
        first = self.install()
        self.distribution["manager_version"] = "0.1.1"
        self.write_distribution()
        current = self.install()
        standalone = self.root/"standalone"
        shutil.copytree(self.plugin/"skills/aos", standalone/"skills/aos")
        (standalone/"skills/aos/scripts/installed-prefix.json").write_text('{"local":"prefix"}')
        result = manager.install(self.prefix, sys.executable, archive=self.archive,
                                 package_root=standalone)
        self.assertEqual(result["status"], "already_installed")
        self.assertEqual(result["release"], current["release"])
        self.assertEqual(manager.read_json(self.prefix/"previous.json")["release"], first["release"])

    def test_config_pair_is_preserved_and_partial_pair_rejected(self):
        folder = self.prefix/"config"
        folder.mkdir(parents=True)
        (folder/"router.json5").write_text('{"user":"router"}')
        with self.assertRaisesRegex(manager.Failure, "Keep both"):
            self.install()
        self.assertFalse((self.prefix/"current.json").exists())
        (folder/"session.json5").write_text('{"user":"session"}')
        self.install()
        self.assertEqual((folder/"router.json5").read_text(), '{"user":"router"}')
        self.assertEqual((folder/"session.json5").read_text(), '{"user":"session"}')

    def test_install_lock_excludes_another_operation(self):
        with manager.lock(self.prefix):
            with self.assertRaisesRegex(manager.Failure, "holds the lock"):
                with manager.lock(self.prefix, timeout=0):
                    self.fail("second operation acquired install lock")

    def test_foreign_skill_conflict_precedes_installation(self):
        home = self.root/"home"
        (home/".agents/skills/aos").mkdir(parents=True)
        with patch.object(manager.Path, "home", return_value=home):
            with self.assertRaisesRegex(manager.Failure, "not installed by AOS"):
                manager.install(self.prefix, sys.executable, archive=self.archive,
                                agents=["codex"], package_root=self.plugin)
        self.assertFalse((self.prefix/"current.json").exists())


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_path_traversal_and_windows_drive_names_are_rejected(self):
        for path in ("../escape", "/escape", "root/../../escape", "C:/escape", "root\\escape"):
            with self.subTest(path=path), self.assertRaises(manager.Failure):
                manager.safe_member(path)

    def test_tar_symlink_is_rejected(self):
        path = self.root/"bad.tar.gz"
        with tarfile.open(path, "w:gz") as output:
            info = tarfile.TarInfo("root/link")
            info.type, info.linkname = tarfile.SYMTYPE, "../../outside"
            output.addfile(info)
        with self.assertRaisesRegex(manager.Failure, "link or special"):
            manager.extract_archive(path, self.root/"out")

    def test_zip_symlink_is_rejected(self):
        path = self.root/"bad.zip"
        with zipfile.ZipFile(path, "w") as output:
            info = zipfile.ZipInfo("root/link")
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            output.writestr(info, "../../outside")
        with self.assertRaisesRegex(manager.Failure, "symlinks"):
            manager.extract_archive(path, self.root/"out")

    def test_case_collisions_are_rejected_before_extraction(self):
        path = self.root/"bad.zip"
        with zipfile.ZipFile(path, "w") as output:
            output.writestr("root/FILE", "one")
            output.writestr("root/file", "two")
        with self.assertRaisesRegex(manager.Failure, "inventory"):
            manager.extract_archive(path, self.root/"out")
        self.assertEqual(list((self.root/"out").iterdir()), [])

    def test_multiple_roots_are_rejected(self):
        path = self.root/"bad.zip"
        with zipfile.ZipFile(path, "w") as output:
            output.writestr("one/file", "one")
            output.writestr("two/file", "two")
        with self.assertRaises(manager.Failure):
            manager.extract_archive(path, self.root/"out")

    def test_public_packaging_is_reproducible_and_excludes_local_records(self):
        plugin = self.root/"plugin"
        shutil.copytree(ROOT/"plugins/aos", plugin)
        (plugin/"debug.log").write_text("local process data")
        (plugin/"evidence").mkdir()
        (plugin/"evidence/report.json").write_text('{"private":"local"}')
        one = packager.package(plugin, self.root/"one")
        two = packager.package(plugin, self.root/"two")
        self.assertEqual(one, two)
        for asset in one["assets"]:
            self.assertEqual((self.root/"one"/asset["name"]).read_bytes(),
                             (self.root/"two"/asset["name"]).read_bytes())
        with zipfile.ZipFile(self.root/"one/aos.plugin") as archive:
            self.assertEqual(set(archive.namelist()), set(packager.FILES))
        with tarfile.open(self.root/"one/aos-agent-0.1.0.tar.gz") as archive:
            self.assertEqual(set(archive.getnames()), {"aos-agent/"+f for f in packager.FILES})


class UpdateTests(unittest.TestCase):
    def test_public_catalog_uses_anonymous_raw_endpoint(self):
        catalog = {"schema_version": 1, "manager_version": "0.1.1",
                   "runtime_version": "0.2.0", "assets": []}
        with patch.object(manager.urllib.request, "urlopen",
                          return_value=io.BytesIO(json.dumps(catalog).encode())) as request:
            result = manager.find_release()
        self.assertEqual(result["tag_name"], "aos-agent-v0.1.1")
        self.assertEqual(request.call_args.args[0].full_url,
                         "https://raw.githubusercontent.com/Cmoon-cyl/aos/main/distributions/stable.json")

    def test_requested_version_must_match_tag_catalog(self):
        catalog = {"schema_version": 1, "manager_version": "0.1.1", "runtime_version": "0.2.0"}
        with patch.object(manager.urllib.request, "urlopen",
                          return_value=io.BytesIO(json.dumps(catalog).encode())):
            with self.assertRaisesRegex(manager.Failure, "differs"):
                manager.find_release("0.1.0")

    def test_update_check_does_not_install(self):
        release = {"tag_name": "aos-agent-v0.1.1", "runtime_version": "0.2.1",
                   "html_url": "https://github.com/Cmoon-cyl/aos/releases/tag/aos-agent-v0.1.1"}
        with patch.object(manager, "find_release", return_value=release), \
                patch.object(manager, "install") as install:
            result = manager.update_check({"manager_version": "0.1.0", "runtime_version": "0.2.0"})
        self.assertTrue(result["update_available"])
        self.assertEqual(result["available_runtime_version"], "0.2.1")
        install.assert_not_called()

    def test_equal_version_is_noop_and_downgrade_is_rejected(self):
        current = {"manager_version": "0.1.1"}
        with patch.object(manager, "find_release", return_value={"tag_name": "aos-agent-v0.1.1"}), \
                patch.object(manager, "checked_download") as download:
            self.assertEqual(manager.update_apply(Path("unused"), current, {})["status"], "up_to_date")
            download.assert_not_called()
        with patch.object(manager, "find_release", return_value={"tag_name": "aos-agent-v0.1.0"}):
            with self.assertRaisesRegex(manager.Failure, "rollback"):
                manager.update_apply(Path("unused"), current, {})

    def test_update_launches_new_bootstrap_instead_of_reusing_old_python(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root/"cache").mkdir()
            archive = root/"next.zip"
            with zipfile.ZipFile(archive, "w") as output:
                output.writestr("aos-agent/skills/aos/scripts/aos_manager.py", "# manager")
                output.writestr("aos-agent/skills/aos/scripts/distribution.json",
                                json.dumps({"manager_version": "0.1.1"}))
                output.writestr("aos-agent/skills/aos/scripts/setup.sh", "#!/bin/sh")
                output.writestr("aos-agent/skills/aos/scripts/setup.ps1", "# setup")
            release = {"tag_name": "aos-agent-v0.1.1", "assets": [
                {"name": "aos-agent-0.1.1.zip", "digest": "sha256:"+manager.digest(archive)}]}
            with patch.object(manager, "find_release", return_value=release), \
                    patch.object(manager, "checked_download", return_value=archive), \
                    patch.object(manager, "run", return_value='{"ok":true,"data":{"status":"installed"}}') as run:
                result = manager.update_apply(root, {"manager_version": "0.1.0"}, {})
            command = list(map(str, run.call_args.args[0]))
            self.assertTrue(any(arg.endswith("setup.ps1" if os.name == "nt" else "setup.sh") for arg in command))
            self.assertIn("--non-interactive", command)
            self.assertEqual(result["status"], "installed")


class ProcessTests(unittest.TestCase):
    def test_subprocess_output_is_utf8_independent_of_console_codepage(self):
        result = manager.run([sys.executable, "-I", "-c",
                              "import sys; sys.stdout.buffer.write('路径中文'.encode('utf-8'))"])
        self.assertEqual(result, "路径中文")

    def test_dispatcher_waits_and_preserves_child_exit_code(self):
        with tempfile.TemporaryDirectory() as temp:
            prefix = Path(temp)/"AOS 中文 test"
            prefix.mkdir()
            release = prefix/"releases/test"
            release.mkdir(parents=True)
            child = release/"manager.py"
            child.write_text("import sys,time\n"
                             "time.sleep(0.15)\n"
                             "print('child finished',flush=True)\n"
                             "raise SystemExit(7)\n", encoding="utf-8")
            manager.write_json(prefix/"current.json",
                               {"release": str(release), "python": sys.executable,
                                "manager": str(child)})
            manager.install_dispatcher(prefix, str(prefix/"bootstrap/python.exe"))
            result = subprocess.run([sys.executable, "-I", str(prefix/"dispatch.py"), "--version"],
                                    capture_output=True, text=True, encoding="utf-8", timeout=10)
            self.assertEqual(result.returncode, 7)
            self.assertEqual(result.stdout.strip(), "child finished")


if __name__ == "__main__":
    unittest.main()
