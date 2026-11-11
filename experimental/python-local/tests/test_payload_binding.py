import unittest

from tpf_experimental_python_local import PolyglotInt32PayloadBinding, PythonLocalDiagnostic


class PayloadBindingTest(unittest.TestCase):
    type_ids = ("InputValue", "FirstValue", "SecondValue", "OutputValue")
    binding = PolyglotInt32PayloadBinding(*type_ids)
    envelope = binding.encode("InputValue", 1)

    def test_round_trips_each_nominal_type_as_compact_int32_json(self):
        for type_id in self.type_ids:
            for value in (-(2**31), -1, 0, 1, 2**31 - 1):
                with self.subTest(type_id=type_id, value=value):
                    encoded = self.binding.encode(type_id, value)
                    self.assertEqual(encoded, {
                        "payloadTypeId": type_id,
                        "payloadEncoding": "tpf-polyglot-int32-json/1",
                        "payload": f'{{"value":{value}}}',
                    })
                    self.assertEqual(value, self.binding.decode(type_id, encoded))
        escaped = {**self.envelope, "payload": ' \r{\t"\\u0076a\\u006Cue":-0\n} '}
        self.assertEqual(0, self.binding.decode("InputValue", escaped))

    def test_rejects_decimal_or_exponent_tokens_escaped_duplicates_and_non_json_whitespace(self):
        for payload in (
            '{"value":1.0}', '{"value":1e0}', '{"value":2147483648}', '{"value":-2147483649}',
            '{"value":null}', '{"value":1,"\\u0076alue":2}', '{"value":1}\u00a0', '\uFEFF{"value":1}',
        ):
            with self.subTest(payload=payload), self.assertRaisesRegex(PythonLocalDiagnostic, "^INVALID_PAYLOAD_VALUE$"):
                self.binding.decode("InputValue", {**self.envelope, "payload": payload})
        for value in (2**31, -(2**31) - 1, False, "1", 1.0, None):
            with self.subTest(value=value), self.assertRaisesRegex(PythonLocalDiagnostic, "^INVALID_PAYLOAD_VALUE$"):
                self.binding.encode("InputValue", value)

    def test_checks_catalog_expected_type_and_encoding_before_body(self):
        for catalog in (("A", "A", "B", "C"), ("A", "B", "C"), ("A", "B", "C", "java.lang.Integer")):
            with self.subTest(catalog=catalog), self.assertRaisesRegex(PythonLocalDiagnostic, "^INVALID_PAYLOAD_CATALOG$"):
                PolyglotInt32PayloadBinding(*catalog)
        with self.assertRaisesRegex(PythonLocalDiagnostic, "^UNKNOWN_PAYLOAD_TYPE$"):
            self.binding.encode("String", 1)
        malformed = {**self.envelope, "payloadEncoding": "application/tpf-transition-envelope+json", "payload": "invalid"}
        for type_id, diagnostic in (
            ("String", "UNKNOWN_PAYLOAD_TYPE"),
            ("FirstValue", "PAYLOAD_TYPE_MISMATCH"),
            ("InputValue", "PAYLOAD_ENCODING_MISMATCH"),
        ):
            with self.subTest(type_id=type_id), self.assertRaisesRegex(PythonLocalDiagnostic, f"^{diagnostic}$"):
                self.binding.decode(type_id, malformed)

    def test_rejects_dynamic_or_inexact_envelopes(self):
        class DynamicEnvelope(dict):
            def copy(self):
                raise AssertionError("must not read dict subclass")

        for malformed in (
            DynamicEnvelope(self.envelope),
            {**self.envelope, "unexpected": True},
            {**self.envelope, "payload": 1},
            {"payload": "{}", "payloadTypeId": "InputValue"},
        ):
            with self.subTest(envelope=malformed), self.assertRaisesRegex(
                PythonLocalDiagnostic, "^INVALID_PAYLOAD_ENVELOPE$",
            ):
                self.binding.decode("InputValue", malformed)
