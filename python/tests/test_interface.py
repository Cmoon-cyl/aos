"""Installed-wheel typed interface validation; no router needed."""

import pathlib
import unittest

import aos_sdk as aos

INTERFACES = pathlib.Path(__file__).resolve().parents[2] / "interfaces"


class InterfaceTest(unittest.TestCase):
    def test_load_path_and_mapping_share_hash(self):
        loaded = aos.Interface.load(INTERFACES / "AddTwoInts.json")
        mapping = aos.Interface.load(loaded.definition)
        self.assertEqual(loaded.type_name, "aos_examples/srv/AddTwoInts")
        self.assertEqual(loaded.kind, "srv")
        self.assertEqual(loaded.type_hash, mapping.type_hash)
        self.assertRegex(loaded.type_hash, r"^[0-9a-f]{64}$")
        loaded.validate({"a": 1, "b": 2}, "request")

    def test_schema_errors_name_bad_field_and_reject_bool(self):
        interface = aos.Interface.load(INTERFACES / "AddTwoInts.json")
        for value in ("2", True):
            with self.subTest(value=value), self.assertRaises(
                aos.InvalidArgumentError
            ) as error:
                interface.validate({"a": value, "b": 2}, "request")
            self.assertIn("a", str(error.exception))
        with self.assertRaises(aos.InvalidArgumentError):
            interface.validate({"a": float("nan"), "b": 2}, "request")

    def test_qos_rejects_unsupported_policies(self):
        for kwargs in ({"depth": 0}, {"depth": True}, {"history": "keep_all"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(aos.InvalidArgumentError):
                aos.QoSProfile(**kwargs)


if __name__ == "__main__":
    unittest.main()
