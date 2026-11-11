"""Admit the compiler's closed projection and generated Python ABI."""

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal, InvalidOperation
from types import MemberDescriptorType
from typing import Any, TypeGuard


class PythonLocalDiagnostic(ValueError):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


@dataclass(frozen=True, slots=True)
class Carriers:
    invocation_context: type
    value: type
    failure: type
    completed: type
    failed: type
    rejected: type
    cancelled: type


@dataclass(frozen=True, slots=True)
class Step:
    id: str
    input_id: str
    output_id: str
    input_type: type
    output_type: type
    retry_limit: int
    invoke: Callable[[Any, Any], object]


@dataclass(frozen=True, slots=True)
class Projection:
    pipeline_id: str
    steps: tuple[Step, ...]
    carriers: Carriers


_PROFILE = "tpf-polyglot-unary-policy/1"
_CAPABILITIES = ["pure", "linear", "unary", "local", "retry", "cancellation", "context"]
_POLICY = {
    "failureMapping": "structured-failure/1",
    "retryOwner": "LOCAL_SHELL",
    "resultMapping": "explicit-value/1",
    "cancellation": "first-accepted-terminal/1",
    "context": "explicit-snapshot/1",
    "recovery": "REJECT",
    "legacyPolicy": "REJECT",
}
_VALUE_FIELD = {"name": "value", "type": "int32", "presence": "REQUIRED", "nullability": "NON_NULL"}
_IDENTIFIER = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_PIPELINE_ID = re.compile(r"[a-z][a-z0-9-]*")
_DIGEST = re.compile(r"[a-f0-9]{64}")


def _matches(value: object, pattern: re.Pattern[str]) -> bool:
    return type(value) is str and pattern.fullmatch(value) is not None


def _record(value: Any, names: set[str], code: str, detail: str) -> dict[str, Any]:
    if type(value) is not dict or value.keys() != names:
        raise PythonLocalDiagnostic(code, detail)
    return value


def _unique_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    record: dict[str, Any] = {}
    for key, value in pairs:
        if key in record:
            raise PythonLocalDiagnostic("INVALID_JSON", f"duplicate field {key}")
        record[key] = value
    return record


def _exact_integer(source: str) -> int:
    try:
        number = Decimal(source)
        if number.is_finite() and number.copy_abs() <= (2**53 - 1) and number == number.to_integral_value():
            return int(number)
    except InvalidOperation:
        pass
    raise PythonLocalDiagnostic("INVALID_NUMBER", "expected an exact safe integer in the projection")


def _parse(source: bytes) -> Any:
    try:
        return json.loads(
            source.decode("utf-8"), object_pairs_hook=_unique_fields,
            parse_int=_exact_integer, parse_float=_exact_integer, parse_constant=_exact_integer,
        )
    except PythonLocalDiagnostic:
        raise
    except (UnicodeError, ValueError, RecursionError) as error:
        raise PythonLocalDiagnostic("INVALID_JSON", "expected compiler projection JSON") from error


def _type_ids(declarations: Any) -> set[str]:
    detail = "expected four distinct canonical records with one required non-null value:int32"
    if type(declarations) is not list or len(declarations) != 4:
        raise PythonLocalDiagnostic("UNSUPPORTED_TYPE", detail)
    names: set[str] = set()
    for declaration in declarations:
        record = _record(declaration, {"id", "fields"}, "UNSUPPORTED_TYPE", detail)
        name = record["id"]
        if not _matches(name, _IDENTIFIER) or name in names or record["fields"] != [_VALUE_FIELD]:
            raise PythonLocalDiagnostic("UNSUPPORTED_TYPE", detail)
        names.add(name)
    return names


def _frozen_slots(record: object, names: tuple[str, ...], kind: str = "") -> TypeGuard[type]:
    if not isinstance(record, type) or not is_dataclass(record):
        return False
    parameters = getattr(record, "__dataclass_params__")
    data_fields = fields(record)
    if (
        not parameters.frozen or record.__dictoffset__
        or tuple(field.name for field in data_fields) != names
        or any(type(vars(record).get(name)) is not MemberDescriptorType for name in names)
    ):
        return False
    if kind:
        return all(field.init for field in data_fields[:-1]) and (
            not data_fields[-1].init and data_fields[-1].default == kind
        )
    return all(field.init for field in data_fields)


def _models(raw: object, ids: set[str]) -> dict[str, type]:
    if not isinstance(raw, Mapping):
        raise PythonLocalDiagnostic("INVALID_MODEL_REGISTRY", "expected generated model types")
    models = dict(raw)
    if models.keys() != ids or any(
        not _frozen_slots(model, ("value",)) or model.__name__ != name or fields(model)[0].type is not int
        for name, model in models.items()
    ):
        raise PythonLocalDiagnostic("INVALID_MODEL_REGISTRY", "expected distinct generated nominal model types")
    return models


def _carriers(raw: Any) -> Carriers:
    layouts = (
        ("invocation_context", ("executionId", "traceId"), ""),
        ("value", ("value", "kind"), "VALUE"),
        ("failure", ("step_id", "code", "message", "retryable", "owner"), "LOCAL_SHELL"),
        ("completed", ("outcome", "kind"), "COMPLETED"),
        ("failed", ("failure", "kind"), "FAILED"),
        ("rejected", ("diagnostic", "kind"), "REJECTED"),
        ("cancelled", ("kind",), "CANCELLED"),
    )
    carriers: dict[str, type] = {}
    for name, names, kind in layouts:
        carrier = getattr(raw, name, None)
        if not _frozen_slots(carrier, names, kind):
            raise PythonLocalDiagnostic("INVALID_CARRIERS", f"expected generated carrier {name}")
        carriers[name] = carrier
    return Carriers(**carriers)


def _steps(raw: Any, pipeline: dict[str, Any], models: dict[str, type], handlers: object) -> tuple[Step, ...]:
    if type(raw) is not list or len(raw) != 3:
        raise PythonLocalDiagnostic("UNSUPPORTED_STEP_SHAPE", "expected three ordered unary steps")
    if not isinstance(handlers, Mapping):
        raise PythonLocalDiagnostic("INVALID_HANDLER_REGISTRY", "expected generated handler bindings")
    registry = dict(handlers)
    preceding = pipeline["inputType"]
    if type(preceding) is not str or preceding not in models:
        raise PythonLocalDiagnostic("TYPE_MISMATCH", "expected a canonical pipeline input")
    boundaries = {preceding}
    steps = []
    step_ids = set()
    handler_names = set()
    for raw_step in raw:
        step = _record(raw_step, {"id", "inputType", "outputType", "handler", "retryLimit"},
                       "UNSUPPORTED_STEP", "expected a closed unary step binding")
        step_id, handler = step["id"], step["handler"]
        if not _matches(step_id, _IDENTIFIER) or step_id in step_ids:
            raise PythonLocalDiagnostic("INVALID_STEP_ID", "expected distinct step identifiers")
        if not _matches(handler, _IDENTIFIER) or handler in handler_names:
            raise PythonLocalDiagnostic("AMBIGUOUS_HANDLER", f"invalid or repeated binding for {step_id}")
        output = step["outputType"]
        if step["inputType"] != preceding or type(output) is not str or output not in models or output in boundaries:
            raise PythonLocalDiagnostic("TYPE_MISMATCH", f"invalid nominal boundary at {step_id}")
        budget = step["retryLimit"]
        if type(budget) is not int or not 0 <= budget <= 2:
            raise PythonLocalDiagnostic("UNSUPPORTED_RETRY", f"expected retry budget 0, 1 or 2 at {step_id}")
        invoke = registry.get(handler)
        if not callable(invoke):
            raise PythonLocalDiagnostic("MISSING_HANDLER", f"register handler {handler}")
        steps.append(Step(step_id, preceding, output, models[preceding], models[output], budget, invoke))
        step_ids.add(step_id)
        handler_names.add(handler)
        boundaries.add(output)
        preceding = output
    if preceding != pipeline["outputType"]:
        raise PythonLocalDiagnostic("TYPE_MISMATCH", "pipeline endpoints do not match the ordered step boundaries")
    if registry.keys() != handler_names:
        raise PythonLocalDiagnostic("EXTRA_HANDLER", "register only handlers declared by the compiler")
    return tuple(steps)


def admit(binding: Any) -> Projection:
    version = getattr(binding, "schema_version", None)
    if type(version) is not int or version != 1:
        raise PythonLocalDiagnostic("UNSUPPORTED_SCHEMA", "expected binding schema 1")
    source = getattr(binding, "projection_bytes", None)
    digest = getattr(binding, "projection_sha256", None)
    if type(source) is not bytes or not _matches(digest, _DIGEST) or hashlib.sha256(source).hexdigest() != digest:
        raise PythonLocalDiagnostic("ARTIFACT_DIGEST_MISMATCH", "retrieve matching generated outputs")
    projection = _record(
        _parse(source),
        {"schemaVersion", "profile", "kind", "pipeline", "requiredCapabilities", "types", "steps",
         "contextKeys", "policy", "provenance"},
        "UNSUPPORTED_PROJECTION", "unexpected projection fields",
    )
    version = projection["schemaVersion"]
    if type(version) is not int or version != 1:
        raise PythonLocalDiagnostic("UNSUPPORTED_SCHEMA", "expected projection schema 1")
    if projection["profile"] != _PROFILE or projection["kind"] != "compiler-python-local":
        raise PythonLocalDiagnostic("UNSUPPORTED_PROFILE", f"expected compiler-python-local and {_PROFILE}")
    if projection["requiredCapabilities"] != _CAPABILITIES:
        raise PythonLocalDiagnostic("UNSUPPORTED_CAPABILITY", "capabilities differ from Python LOCAL")
    if projection["contextKeys"] != ["executionId", "traceId"]:
        raise PythonLocalDiagnostic("UNSUPPORTED_CONTEXT", "expected executionId and traceId")
    if projection["policy"] != _POLICY:
        raise PythonLocalDiagnostic("UNSUPPORTED_POLICY", "policy differs from Python LOCAL")
    pipeline = _record(projection["pipeline"], {"id", "inputType", "outputType", "transport", "platform", "execution"},
                       "UNSUPPORTED_PIPELINE", "unexpected pipeline fields")
    if not _matches(pipeline["id"], _PIPELINE_ID):
        raise PythonLocalDiagnostic("INVALID_PIPELINE_ID")
    if (pipeline["transport"], pipeline["platform"], pipeline["execution"]) != ("LOCAL", "COMPUTE", "DIRECT"):
        raise PythonLocalDiagnostic("UNSUPPORTED_EXECUTION", "expected LOCAL / COMPUTE / DIRECT")
    provenance = _record(projection["provenance"], {"dslSha256", "bindingsSha256"},
                         "INVALID_PROVENANCE", "expected input digests")
    if not all(_matches(value, _DIGEST) for value in provenance.values()):
        raise PythonLocalDiagnostic("INVALID_PROVENANCE", "input digests must be lowercase SHA-256")

    models = _models(getattr(binding, "model_types", None), _type_ids(projection["types"]))
    carriers = _carriers(getattr(binding, "carrier_types", None))
    steps = _steps(projection["steps"], pipeline, models, getattr(binding, "handlers", None))
    return Projection(pipeline["id"], steps, carriers)
