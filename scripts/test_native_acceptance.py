"""Evidence-harness regressions. Run with CPython 3.11 from scripts/."""

import ast
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from native_acceptance import Acceptance, INSTALLED_TESTS, WINDOWS_EXPECTED_SKIPS, installed_test_scope, native_platform


class NativeAcceptanceTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        output = self.root / "evidence"
        output.mkdir()
        self.acceptance = Acceptance(self.root, output)

    def test_native_platform_matches_only_supported_native_targets(self):
        for system, machine, target in [("Linux", "x86_64", "x86_64-unknown-linux-gnu"),
                ("Windows", "AMD64", "x86_64-pc-windows-msvc"), ("Darwin", "arm64", "aarch64-apple-darwin")]:
            self.assertEqual(native_platform(system, machine)["target"], target)
        for system, machine in [("Linux", "arm64"), ("Windows", "arm64"), ("Darwin", "x86_64"), ("FreeBSD", "x86_64")]:
            with self.subTest(system=system, machine=machine), self.assertRaises(ValueError):
                native_platform(system, machine)

    def test_installed_scope_requires_exact_declared_skips_and_rejects_drift(self):
        selected = [*WINDOWS_EXPECTED_SKIPS, "test_action_sdk.Example.test_success"]
        skipped = [dict(test=name, reason=reason) for name, reason in WINDOWS_EXPECTED_SKIPS.items()]
        self.assertTrue(installed_test_scope(selected, skipped, "Windows")["valid"])
        self.assertTrue(installed_test_scope(selected, [], "Linux")["valid"])
        self.assertFalse(installed_test_scope(selected, skipped, "Linux")["valid"])
        for changed in (
            skipped[:-1],
            skipped + [dict(test=selected[-1], reason="AOS_ZENOHD is not set")],
            skipped + [skipped[0]],
            [dict(skipped[0], reason="changed platform declaration"), *skipped[1:]],
        ):
            with self.subTest(skipped=changed):
                self.assertFalse(installed_test_scope(selected, changed, "Windows")["valid"])
        self.assertFalse(installed_test_scope(selected[1:], skipped, "Windows")["valid"])
        # The subprocess gets the same checker and literal, not a second policy.
        namespace = {}
        prefix = INSTALLED_TESTS.split("import importlib.machinery", 1)[0]
        exec(compile(prefix, "installed-scope-fixture", "exec"), namespace)
        self.assertEqual(namespace["installed_test_scope"](selected, skipped, "Windows"),
                         installed_test_scope(selected, skipped, "Windows"))

    def test_windows_exclusions_match_existing_platform_and_fixture_declarations(self):
        declared = {}
        test_root = Path(__file__).resolve().parents[1] / "python/tests"

        def posix_reason(decorators):
            for decorator in decorators:
                if (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                        and decorator.func.attr == "skipUnless" and len(decorator.args) == 2
                        and ast.unparse(decorator.args[0]) == "os.name == 'posix'"):
                    return ast.literal_eval(decorator.args[1])
            return None

        for path in test_root.glob("test_*.py"):
            for cls in ast.parse(path.read_text(encoding="utf-8")).body:
                if not isinstance(cls, ast.ClassDef):
                    continue
                for method in cls.body:
                    if isinstance(method, ast.FunctionDef) and method.name.startswith("test_"):
                        reason = posix_reason(cls.decorator_list) or posix_reason(method.decorator_list)
                        if reason:
                            declared[f"{path.stem}.{cls.name}.{method.name}"] = reason
        self.assertEqual(declared, WINDOWS_EXPECTED_SKIPS)

    def test_router_logs_exclude_old_bytes_and_keep_replacements(self):
        target = self.root / "target"
        target.mkdir()
        for name, value in {"same": b"old", "append": b"previous\n",
                            "replace": b"old log content\n", "empty": b""}.items():
            (target / f"zenohd-{name}.log").write_bytes(value)
        before = self.acceptance.router_log_snapshot(target)
        (target / "zenohd-append.log").write_bytes(b"previous\ncurrent\n")
        (target / "zenohd-replace.log").write_bytes(b"new\n")
        (target / "zenohd-new.log").write_bytes(b"fresh\n")
        (target / "zenohd-empty.log").write_bytes(b"first line\n")
        self.acceptance.collect_router_logs(target, before)
        logs = self.acceptance.record["router_logs"]
        self.assertEqual(logs["excluded_unchanged_from_previous_run"], ["zenohd-same.log"])
        entries = {entry["source"]: entry for entry in logs["collected"]}
        appended = entries["zenohd-append.log"]
        self.assertEqual(appended["change"], "appended")
        self.assertEqual(appended["copied_from_offset"], len(b"previous\n"))
        self.assertEqual((self.acceptance.output / appended["log"]).read_bytes(), b"current\n")
        self.assertEqual(entries["zenohd-replace.log"]["change"], "replaced")
        self.assertEqual(entries["zenohd-new.log"]["change"], "new")
        self.assertEqual(entries["zenohd-empty.log"]["change"], "appended")
        self.assertEqual(entries["zenohd-empty.log"]["copied_from_offset"], 0)

    def test_cached_archive_is_verified_before_extraction_or_execution(self):
        archive = self.root / "cached.zip"
        with zipfile.ZipFile(archive, "w") as bundle:
            bundle.writestr("zenohd", b"fixture only")
        self.acceptance.env["AOS_ZENOHD_ARCHIVE"] = str(archive)
        spec = {"asset": "router.zip", "asset_id": 1, "exe": "", "sha256": "0" * 64}
        with patch.object(self.acceptance, "run") as run:
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                self.acceptance.download_router(spec)
            run.assert_not_called()
        self.assertFalse((self.acceptance.output / "zenohd" / "unpacked").exists())
        evidence = json.loads((self.acceptance.output / "provenance.json").read_text())
        self.assertEqual(evidence["zenohd"]["actual_sha256"], hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertEqual(evidence["zenohd"]["acquisition"]["kind"], "local_archive")

    def test_vendor_source_archive_is_checked_before_the_source_verifier(self):
        archive = self.root / "corrupt.crate"
        archive.write_bytes(b"not the fixed source")
        self.acceptance.env["AOS_ZENOH_CRATE_ARCHIVE"] = str(archive)
        with patch.object(self.acceptance, "run") as run:
            with self.assertRaisesRegex(ValueError, "source archive checksum mismatch"):
                self.acceptance.verify_vendor_source()
            run.assert_not_called()
        proof = self.acceptance.record["zenoh_vendor"]
        self.assertEqual(proof["actual_sha256"], hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertNotIn("verification", proof)

    def test_tls_archive_is_checked_before_verifier_or_native_build(self):
        archive = self.root / "corrupt-tls.crate"
        archive.write_bytes(b"not the fixed TLS source")
        self.acceptance.env["AOS_ZENOH_TLS_CRATE_ARCHIVE"] = str(archive)
        with patch.object(self.acceptance, "run") as run:
            with self.assertRaisesRegex(ValueError, "TLS source archive checksum mismatch"):
                self.acceptance.verify_tls_vendor_source()
            run.assert_not_called()
        proof = self.acceptance.record["zenoh_tls_vendor"]
        self.assertEqual(proof["actual_sha256"], hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertNotIn("verification", proof)

    def test_default_profile_requires_one_named_test_not_only_cargo_success(self):
        name = "launch_probe::tests::real_default_profile_reaches_owned_node_readiness"
        success = (
            f"test {name} ... ok\n"
            "test result: ok. 1 passed; 0 failed; 0 ignored; 24 filtered out;\n"
            "test result: ok. 0 passed; 0 failed; 0 ignored; 19 filtered out;\n"
        )
        with patch.object(self.acceptance, "run", return_value=success) as run:
            self.acceptance.verify_default_profile_readiness()
        self.assertEqual(run.call_args.args[1][-4:], ["--", "--ignored", "--exact", "--nocapture"])
        self.assertEqual(self.acceptance.record["default_profile_readiness"]["passed"], 1)
        self.assertEqual(self.acceptance.record["default_profile_readiness"]["status"], "passed")
        rejected = [
            "test result: ok. 0 passed; 0 failed; 0 ignored; 25 filtered out;\n",
            success.replace(name, "renamed_test"),
            success.replace("1 passed", "2 passed"),
            success.replace("0 ignored", "1 ignored"),
            success.replace("0 failed", "1 failed"),
            f"test {name} ... ok\n",
        ]
        for output in rejected:
            with self.subTest(output=output), patch.object(self.acceptance, "run", return_value=output):
                with self.assertRaisesRegex(ValueError, "exactly the named test"):
                    self.acceptance.verify_default_profile_readiness()
                saved = json.loads((self.acceptance.output / "provenance.json").read_text())
                self.assertEqual(saved["default_profile_readiness"]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
