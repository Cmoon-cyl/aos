"""Installed SDK resource lookup with no cwd, imports or entry execution."""

import json
import inspect
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import aos_sdk as aos
from process_support import assert_canonical_path, directory_alias


ROOT = Path(__file__).resolve().parents[2]


class PackageIndexTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name).resolve()
        self.prefix = self.directory / "installed package"
        self.prefix.mkdir()

    def install(self, prefix=None, version="1.0.0"):
        prefix = prefix or self.prefix
        index = prefix / "share/aos/resource_index/packages"
        share = prefix / "share/resource_probe"
        entries = prefix / "lib/resource_probe"
        index.mkdir(parents=True)
        share.mkdir(parents=True)
        entries.mkdir(parents=True)
        resource = share / "Message.json"
        resource.write_text(json.dumps({
            "name": "resource_probe/msg/Message", "kind": "msg",
            "message": {"type": "object", "properties": {"value": {"type": "integer"}},
                        "required": ["value"], "additionalProperties": False},
        }))
        marker = self.directory / "entry-executed"
        executable = entries / "probe"
        executable.write_text("#!/bin/sh\n: > " + shlex.quote(str(marker)) + "\nexit 99\n")
        executable.chmod(0o755)
        manifest = {
            "schema_version": 1, "name": "resource_probe", "version": version,
            "runtime_abi": "aos.process.v1",
            "entries": {"probe": {"kind": "aos_node", "executable": "lib/resource_probe/probe"}},
            "resources": {"message": {"kind": "interface", "path": "share/resource_probe/Message.json"}},
        }
        manifest_path = index / "resource_probe.json"
        manifest_path.write_text(json.dumps(manifest))
        return resource, manifest_path, marker

    def test_installed_interface_resolves_and_loads_from_an_unrelated_cwd(self):
        resource, _, marker = self.install()
        foreign = self.directory / "unrelated cwd"
        foreign.mkdir()
        completed = subprocess.run(
            [sys.executable, "-I", "-c", """
import json, pathlib, sys
import aos_sdk as aos
prefix = pathlib.Path(sys.argv[1])
resource = aos.get_package_resource("resource_probe", "message", prefixes=[prefix])
interface = aos.Interface.load(resource)
interface.validate({"value": 42})
print(json.dumps({
    "prefix": str(aos.get_package_prefix("resource_probe", prefixes=[prefix])),
    "share": str(aos.get_package_share_directory("resource_probe", prefixes=[prefix])),
    "resource": str(resource), "type": interface.type_name,
}))
""", str(self.prefix)], cwd=foreign, capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual(set(result), {"prefix", "share", "resource", "type"})
        self.assertEqual(result["type"], "resource_probe/msg/Message")
        for key, expected in (("prefix", self.prefix),
                              ("share", self.prefix / "share/resource_probe"),
                              ("resource", resource)):
            assert_canonical_path(result[key], expected)
        self.assertFalse(marker.exists(), "resource discovery executed a package entry")

    def test_default_environment_and_explicit_prefixes_do_not_use_cwd(self):
        self.install()
        empty = self.directory / "empty"
        empty.mkdir()
        with mock.patch.dict(os.environ, {"AOS_PREFIX_PATH": str(self.prefix)}):
            assert_canonical_path(aos.get_package_prefix("resource_probe"), self.prefix)
            with self.assertRaises(aos.AOSError) as caught:
                aos.PackageIndex([empty]).package("resource_probe")
            self.assertEqual(caught.exception.code, "package_not_found")
        completed = subprocess.run(
            [sys.executable, "-I", "-c", """
import os
import aos_sdk as aos
os.environ.pop("AOS_PREFIX_PATH", None)
try:
    aos.get_package_prefix("resource_probe")
except aos.AOSError as error:
    assert error.code == "package_not_found", error
else:
    raise AssertionError("cwd was searched as an implicit package prefix")
"""], cwd=self.prefix, capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_canonical_duplicate_prefix_and_ambiguous_packages(self):
        self.install()
        link = self.directory / "prefix-alias"
        directory_alias(link, self.prefix)
        index = aos.PackageIndex([self.prefix, link])
        self.assertEqual(len(index.packages()["resource_probe"]), 1)
        other = self.directory / "second-install"
        self.install(other, version="2.0.0")
        ambiguous = aos.PackageIndex([self.prefix, other])
        with self.assertRaises(aos.AOSError) as caught:
            ambiguous.package("resource_probe", version="1.0.0")
        self.assertEqual(caught.exception.code, "ambiguous_package")
        self.assertEqual(aos.PackageIndex([other]).package("resource_probe")["manifest"]["version"],
                         "2.0.0")

    def test_version_missing_resource_and_empty_path_errors_keep_native_codes(self):
        self.install()
        index = aos.PackageIndex([self.prefix])
        for operation, code in (
            (lambda: index.resource("resource_probe", "message", package_version="9.0"),
             "version_mismatch"),
            (lambda: index.resource("resource_probe", "absent"), "resource_not_found"),
            (lambda: aos.PackageIndex([""]), "invalid_argument"),
        ):
            with self.assertRaises(aos.AOSError) as caught:
                operation()
            self.assertEqual(caught.exception.code, code)
            self.assertIsInstance(caught.exception.stage, str)

    def test_deployed_default_uses_supervisor_prefixes_without_interpreter_fallback(self):
        self.install()
        interpreter_prefix = self.directory / "other-venv"
        self.install(interpreter_prefix, version="2.0.0")
        with mock.patch.object(sys, "prefix", str(interpreter_prefix)), \
             mock.patch.dict(os.environ, {"AOS_PREFIX_PATH": str(self.prefix)}):
            os.environ.pop("AOS_PROCESS_CONFIG", None)
            with self.assertRaises(aos.AOSError) as caught:
                aos.PackageIndex().package("resource_probe")
            self.assertEqual(caught.exception.code, "ambiguous_package")
            with mock.patch.dict(os.environ, {"AOS_PROCESS_CONFIG": "/owned/bootstrap.json"}):
                assert_canonical_path(aos.get_package_prefix("resource_probe"), self.prefix)

    def test_relative_and_empty_environment_prefixes_follow_the_native_resolver(self):
        self.install()
        result = subprocess.run(
            [sys.executable, "-I", "-c", inspect.getsource(assert_canonical_path) + "\n" + """
import os, pathlib, sys
import aos_sdk as aos
relative = sys.argv[1]
expected = pathlib.Path(relative).resolve()
assert_canonical_path(aos.get_package_prefix("resource_probe", prefixes=[relative]), expected)
os.environ["AOS_PREFIX_PATH"] = relative
assert_canonical_path(aos.get_package_prefix("resource_probe"), expected)
os.environ["AOS_PREFIX_PATH"] = relative + os.pathsep + ""
try:
    aos.PackageIndex()
except aos.AOSError as error:
    assert error.code == "invalid_argument"
else:
    raise AssertionError("empty path-list entry accepted")
os.environ["AOS_PREFIX_PATH"] = ""
aos.PackageIndex()
""", self.prefix.name],
            cwd=self.directory, capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_resource_replacement_cannot_escape_prefix_after_index_creation(self):
        resource, _, _ = self.install()
        index = aos.PackageIndex([self.prefix])
        external = self.directory / "outside.json"
        external.write_text("{}")
        if os.name == "nt":
            external_dir = self.directory / "outside-resources"
            external_dir.mkdir()
            (external_dir / resource.name).write_text("{}")
            resource.parent.rename(resource.parent.with_name("original-resources"))
            directory_alias(resource.parent, external_dir)
        else:
            resource.unlink()
            resource.symlink_to(external)
        with self.assertRaises(aos.AOSError) as caught:
            index.resource("resource_probe", "message")
        self.assertEqual(caught.exception.code, "invalid_argument")
        with self.assertRaises(aos.AOSError):
            aos.PackageIndex([self.prefix])

    def test_demo_installer_uses_candidate_python_and_preserves_existing_prefix(self):
        prefix = self.directory / "demo installed"
        config = self.directory / "peer.json5"
        config.write_text('{mode:"peer", scouting:{multicast:{enabled:false}}}')
        command = [
            sys.executable, "-I", str(ROOT / "scripts/install_demo_package.py"),
            "--prefix", str(prefix), "--config", str(config),
            "--domain-id", "73", "--namespace", "/installed",
        ]
        completed = subprocess.run(command, cwd=self.directory, capture_output=True,
                                   text=True, timeout=10)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        report = json.loads(completed.stdout)
        self.assertEqual(report["python"], sys.executable)
        self.assertEqual(report["sdk_version"], aos.__version__)
        index = aos.PackageIndex([prefix])
        interface = aos.Interface.load(index.resource("aos_demo", "label")["path"])
        interface.validate({"label": "installed"})
        plan_path = Path(index.resource("aos_demo", "pair")["path"])
        original = plan_path.read_bytes()
        plan = json.loads(original)
        self.assertEqual(plan["context"]["zenoh_config"], str(config))
        self.assertEqual(plan["processes"][0]["readiness"]["node"], "/installed/worker")
        rejected = subprocess.run(command, cwd=self.directory, capture_output=True,
                                  text=True, timeout=10)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertEqual(plan_path.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
