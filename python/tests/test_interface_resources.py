"""Public Interface resource categories and compatibility; no router needed."""

import ast
import pathlib
import unittest

import aos_sdk as aos
from aos_sdk import _native, exceptions


def message(schema):
    return {"name": "test/msg/ResourceBudget", "kind": "msg", "message": schema}


class InterfaceResourcesTest(unittest.TestCase):
    def test_public_exception_exports_and_native_stub_agree(self):
        self.assertIs(aos.ResourceLimitError, exceptions.ResourceLimitError)
        self.assertIs(aos.ResourceLimitError, _native.ResourceLimitError)
        self.assertTrue(issubclass(aos.ResourceLimitError, aos.InvalidArgumentError))
        self.assertIn("ResourceLimitError", aos.__all__)
        self.assertIn("ResourceLimitError", exceptions.__all__)
        stub = pathlib.Path(aos.__file__).with_name("_native.pyi")
        declarations = {node.name: node for node in ast.parse(stub.read_text()).body
                        if isinstance(node, ast.ClassDef)}
        self.assertEqual([ast.unparse(base) for base in declarations["ResourceLimitError"].bases],
                         ["InvalidArgumentError"])

    def test_definition_budget_is_distinct_from_bad_schema_and_mismatch(self):
        with self.assertRaises(aos.ResourceLimitError):
            aos.Interface.load(message({"allOf": [True] * 1024}))
        with self.assertRaises(aos.InvalidArgumentError) as caught:
            aos.Interface.load(message({"type": 43}))
        self.assertNotIsInstance(caught.exception, aos.ResourceLimitError)
        interface = aos.Interface.load(message({"type": "integer"}))
        with self.assertRaises(aos.InvalidArgumentError) as caught:
            interface.validate("wrong")
        self.assertNotIsInstance(caught.exception, aos.ResourceLimitError)

    def test_payload_refusal_keeps_the_legacy_catch_category(self):
        interface = aos.Interface.load(message({"type": "array", "items": {
            "allOf": [{"minimum": 0}] * 1022}}))
        with self.assertRaises(aos.InvalidArgumentError) as caught:
            interface.validate([0] * 8192)
        self.assertIsInstance(caught.exception, aos.ResourceLimitError)
        interface.validate([0] * 8191)

    def test_compiled_regex_budget_has_its_own_category(self):
        with self.assertRaises(aos.ResourceLimitError):
            aos.Interface.load(message({"pattern": "^[0-9a-zA-Z]{1000000}$"}))
        with self.assertRaises(aos.InvalidArgumentError) as caught:
            aos.Interface.load(message({"pattern": "(?=unsupported)"}))
        self.assertNotIsInstance(caught.exception, aos.ResourceLimitError)

    def test_enum_fast_path_and_numeric_fallback_are_distinct(self):
        interface = aos.Interface.load(message({"type": "array", "items": {"enum": list(range(60000))}}))
        interface.validate([59999] * 174762)
        with self.assertRaises(aos.ResourceLimitError):
            interface.validate([59999.0] * 140)
        small = aos.Interface.load(message({"type": "array", "items": {"enum": list(range(250))}}))
        small.validate([249] * 10000)

    def test_action_parent_includes_wrapper_resources(self):
        definition = {"name": "test/action/WrapperBudget", "kind": "action",
                      "goal": {"allOf": [True] * 1014},
                      "result": {"type": "object", "default": {}}, "feedback": {"type": "object"}}
        with self.assertRaises(aos.ResourceLimitError):
            aos.Interface.load(definition)
        definition["goal"] = {"allOf": [True] * 1013}
        aos.Interface.load(definition)

    def test_plain_image_preserves_large_data_compatibility(self):
        interface = aos.Interface.load(message({"type": "object", "properties": {
            "header": {"type": "object", "properties": {
                "stamp": {"type": "object", "properties": {
                    "sec": {"type": "integer"}, "nanosec": {"type": "integer"}}},
                "frame_id": {"type": "string"}}},
            "height": {"type": "integer"}, "width": {"type": "integer"},
            "encoding": {"type": "string"}, "step": {"type": "integer"}, "data": {"type": "string"}}}))
        interface.validate({"header": {"stamp": {"sec": 1, "nanosec": 0}, "frame_id": "camera"},
                            "height": 480, "width": 640, "encoding": "mono16", "step": 1280,
                            "data": "A" * 819200})


if __name__ == "__main__":
    unittest.main()
