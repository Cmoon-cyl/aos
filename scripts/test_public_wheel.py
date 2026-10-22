"""Check that distribution cleanup preserves runtime bytes and wheel integrity."""

import base64
import csv
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

SPEC = importlib.util.spec_from_file_location("public_wheel", Path(__file__).with_name("public_wheel.py"))
PUBLIC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLIC)


class PublicWheelTest(unittest.TestCase):
    def test_metadata_cleanup_preserves_runtime_and_dependency_graph(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pyproject.toml").write_text('[project]\nlicense = "Apache-2.0"\nlicense-files = ["LICENSE"]\n')
            (root / "LICENSE").write_text("license fixture\n")
            (root / "README.md").write_text("public readme\n")
            wheel = root / "aos_sdk-0.2.0-cp311-abi3-macosx_11_0_arm64.whl"
            prefix = "aos_sdk-0.2.0.dist-info/"
            reference = "path+file:///private/build-host/crates/aos-core#0.2.0"
            sbom = {"components": [{"name": "aos-core", "version": "0.2.0", "bom-ref": reference}],
                    "dependencies": [{"ref": reference, "dependsOn": []}]}
            runtime = {"aos_sdk/_native.abi3.so": b"native binary fixture\x00\xff", "aos_sdk/node.py": b"class Node: pass\n"}
            with zipfile.ZipFile(wheel, "w") as archive:
                for name, data in runtime.items():
                    archive.writestr(name, data)
                archive.writestr(prefix + "METADATA", "Metadata-Version: 2.4\nName: aos-sdk\nVersion: 0.2.0\n\nold readme\n")
                archive.writestr(prefix + "sboms/aos-py.cyclonedx.json", json.dumps(sbom))
                archive.writestr(prefix + "RECORD", "old record")
            PUBLIC.prepare(wheel, root)
            with zipfile.ZipFile(wheel) as archive:
                for name, data in runtime.items():
                    self.assertEqual(archive.read(name), data)
                metadata = archive.read(prefix + "METADATA").decode()
                self.assertIn("License-Expression: Apache-2.0", metadata)
                self.assertIn("public readme", metadata)
                self.assertEqual(archive.read(prefix + "licenses/LICENSE"), b"license fixture\n")
                cleaned = json.loads(archive.read(prefix + "sboms/aos-py.cyclonedx.json"))
                self.assertEqual(cleaned["components"][0]["bom-ref"], cleaned["dependencies"][0]["ref"])
                self.assertNotIn("/private/build-host", json.dumps(cleaned))
                rows = list(csv.reader(io.StringIO(archive.read(prefix + "RECORD").decode())))
                self.assertEqual({row[0] for row in rows}, set(archive.namelist()))
                for name, digest, size in rows:
                    if name.endswith("/RECORD"):
                        self.assertEqual((digest, size), ("", ""))
                    else:
                        data = archive.read(name)
                        expected = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
                        self.assertEqual((digest, size), ("sha256=" + expected, str(len(data))))

    def test_windows_source_identifier_is_normalized(self):
        self.assertEqual(PUBLIC.normalize_sbom("path+file:///E:/private/build/vendor/zenoh#1.10.1"),
                         "aos-source:vendor/zenoh#1.10.1")

    def test_unsafe_wheel_is_unchanged_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pyproject.toml").write_text('[project]\nlicense = "Apache-2.0"\n')
            wheel = root / "unsafe.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("../outside", "do not extract")
            original = wheel.read_bytes()
            with self.assertRaisesRegex(ValueError, "Unsafe"):
                PUBLIC.prepare(wheel, root)
            self.assertEqual(wheel.read_bytes(), original)
