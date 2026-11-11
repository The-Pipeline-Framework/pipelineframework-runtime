"""Opt-in inner payload binding for the experimental four-type native worker profile."""

import json
import re
from dataclasses import dataclass
from typing import TypedDict

from ._admission import PythonLocalDiagnostic

_ENCODING = "tpf-polyglot-int32-json/1"
_IDENTIFIER = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_ENVELOPE_KEYS = {"payloadTypeId", "payloadEncoding", "payload"}
# Only five literal letters or Unicode escapes can spell the field name. Match
# the closed JSON shape and bounded integer token before converting either.
_RECORD = re.compile(
    r'[ \t\r\n]*\{[ \t\r\n]*("(?:[A-Za-z]|\\u[0-9A-Fa-f]{4}){5}")'
    r'[ \t\r\n]*:[ \t\r\n]*(-?(?:0|[1-9][0-9]{0,9}))[ \t\r\n]*\}[ \t\r\n]*'
)


class SerializedTransitionPayload(TypedDict):
    payloadTypeId: str
    payloadEncoding: str
    payload: str


@dataclass(frozen=True, slots=True, init=False)
class PolyglotInt32PayloadBinding:
    """Use the four logical type IDs from a verified compiler projection."""

    _type_ids: frozenset[str]

    def __init__(self, *type_ids: str) -> None:
        if (
            len(type_ids) != 4
            or any(type(entry) is not str or _IDENTIFIER.fullmatch(entry) is None for entry in type_ids)
            or len(set(type_ids)) != 4
        ):
            raise PythonLocalDiagnostic("INVALID_PAYLOAD_CATALOG")
        object.__setattr__(self, "_type_ids", frozenset(type_ids))

    def encode(self, type_id: str, value: int) -> SerializedTransitionPayload:
        self._require_known_type(type_id)
        if type(value) is not int or not -(2**31) <= value < 2**31:
            raise PythonLocalDiagnostic("INVALID_PAYLOAD_VALUE")
        return {"payloadTypeId": type_id, "payloadEncoding": _ENCODING, "payload": f'{{"value":{value}}}'}

    def decode(self, expected_type_id: str, envelope: SerializedTransitionPayload) -> int:
        self._require_known_type(expected_type_id)
        if type(envelope) is not dict:
            raise PythonLocalDiagnostic("INVALID_PAYLOAD_ENVELOPE")
        fields = envelope.copy()
        if (
            any(type(key) is not str or type(value) is not str for key, value in fields.items())
            or fields.keys() != _ENVELOPE_KEYS
        ):
            raise PythonLocalDiagnostic("INVALID_PAYLOAD_ENVELOPE")
        if fields["payloadTypeId"] != expected_type_id:
            raise PythonLocalDiagnostic("PAYLOAD_TYPE_MISMATCH")
        if fields["payloadEncoding"] != _ENCODING:
            raise PythonLocalDiagnostic("PAYLOAD_ENCODING_MISMATCH")
        match = _RECORD.fullmatch(fields["payload"])
        if match is None or json.loads(match[1]) != "value":
            raise PythonLocalDiagnostic("INVALID_PAYLOAD_VALUE")
        value = int(match[2])
        if not -(2**31) <= value < 2**31:
            raise PythonLocalDiagnostic("INVALID_PAYLOAD_VALUE")
        return value

    def _require_known_type(self, type_id: str) -> None:
        if type(type_id) is not str or type_id not in self._type_ids:
            raise PythonLocalDiagnostic("UNKNOWN_PAYLOAD_TYPE")
