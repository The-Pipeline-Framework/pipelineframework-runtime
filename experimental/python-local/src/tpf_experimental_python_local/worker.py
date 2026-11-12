"""Opt-in HTTPS worker for one deployment-verified native application."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
import hashlib
import hmac
from http.client import LineTooLong
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BufferedIOBase
import json
from os import PathLike
import re
import socket
import ssl
import threading
import time
from typing import Any, cast

from . import PythonLocalDiagnostic, _Application, _Observer, _Runtime, create_python_local_runtime
from ._payload_binding import PolyglotInt32PayloadBinding

_EXECUTE_PATH = "/pipeline/worker/transitions/execute"
_CAPABILITIES_PATH = "/pipeline/worker/capabilities"
_PROFILES = {
    "outerEncoding": "application/tpf-transition-envelope+json",
    "payloadEncoding": "tpf-polyglot-int32-json/1",
    "executionProfile": "tpf-polyglot-unary-policy/1",
    "resultProfile": "tpf-polyglot-failed-result/1",
}
_DIAGNOSTICS = {
    "TRANSIENT": ("TRANSIENT_EXHAUSTED", "Local retry budget exhausted"),
    "BUSINESS_REJECTION": ("BUSINESS_REJECTION", "Business function rejected input"),
    "UNCLASSIFIED": ("UNCLASSIFIED", "Unclassified business function failure"),
}
_RANGE = {"currentStepIndex": 0, "stopBeforeStepIndex": -1, "resultShape": "SINGLE",
          "redriveIntent": "REPLAY", "redriveStepIndex": -1}
_EMPTY_FIELDS = {"redriveCommandId", "redriveReason", "pageContext", "awaitOccurrenceCause"}
_BODY_LIMIT = 16 * 1024
_HEADER_LIMIT = 8192
_INTAKE_TIMEOUT = 10
_TOLERANCE_MS = 120_000
_NONCE_LIMIT = 10_000
_JAVA_NON_BLANK = re.compile(r"[^\u0009-\u000d\u001c-\u0020\u1680\u2000-\u2006\u2008-\u200a\u2028\u2029\u205f\u3000]")
_TIMESTAMP = re.compile(r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]{1,9}))?Z")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_ServerRequest = socket.socket | tuple[bytes, socket.socket]


class _RejectedRequest(Exception):
    def __init__(self, status: int) -> None:
        self.status = status


class _BoundedHeaderReader(BufferedIOBase):
    def __init__(self, stream: BufferedIOBase) -> None:
        self._stream = stream
        self._remaining = _HEADER_LIMIT

    def readline(self, size: int | None = -1, /) -> bytes:
        limit = self._remaining + 1
        line = self._stream.readline(limit if size is None or size < 0 else min(size, limit))
        self._remaining -= len(line)
        if self._remaining < 0:
            raise LineTooLong("native worker request headers")
        return line


def _command_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for key, value in pairs:
        if key in fields or type(value) not in (str, int, type(None)):
            raise _RejectedRequest(400)
        if type(value) is str:
            value.encode("utf-8", "strict")  # Reject unpaired escaped surrogates as well as invalid input bytes.
        fields[key] = value
    return fields


def _integer(token: str) -> int:
    if re.fullmatch(r"(?:0|-?[1-9][0-9]{0,18})", token) is None:
        raise _RejectedRequest(400)
    return int(token)


def _non_integer(token: str) -> None:
    raise _RejectedRequest(400)


def _read_command(body: bytes, pins: Mapping[str, str], payloads: PolyglotInt32PayloadBinding,
                  input_type: str) -> tuple[int, str, str]:
    command = json.loads(body.decode("utf-8", "strict"), object_pairs_hook=_command_fields,
                         parse_int=_integer, parse_float=_non_integer, parse_constant=_non_integer)
    if type(command) is not dict:
        raise _RejectedRequest(400)
    fixed = dict(pins) | _RANGE
    required = fixed.keys() | {"executionId", "traceId", "transitionKey", "attempt", "executionVersion",
                               "payloadTypeId", "payloadEncoding", "payload"}
    if (not required <= command.keys()
            or any(key not in _EMPTY_FIELDS or command[key] is not None for key in command.keys() - required)
            or any(command[key] != value for key, value in fixed.items())):
        raise _RejectedRequest(400)

    execution_id, trace_id, transition_key = (command[key] for key in ("executionId", "traceId", "transitionKey"))
    attempt, version = command["attempt"], command["executionVersion"]
    if (any(type(value) is not str or not value.strip(" \t\r\n") for value in (execution_id, trace_id))
            or type(transition_key) is not str or _JAVA_NON_BLANK.search(transition_key) is None
            or type(attempt) is not int or not 0 <= attempt < 2**31
            or type(version) is not int or not 0 <= version < 2**63):
        raise _RejectedRequest(400)
    number = payloads.decode(input_type, {"payloadTypeId": command["payloadTypeId"],
                                         "payloadEncoding": command["payloadEncoding"], "payload": command["payload"]})
    return number, execution_id, trace_id


def _timestamp_ms(value: str) -> int:
    match = _TIMESTAMP.fullmatch(value)
    if match is None:
        raise _RejectedRequest(401)
    try:
        seconds = datetime.fromisoformat(match[1] + "+00:00") - _EPOCH
    except ValueError:
        raise _RejectedRequest(401) from None
    return (seconds.days * 86400 + seconds.seconds) * 1000 + int((match[2] or "").ljust(3, "0")[:3])


class _Authentication:
    def __init__(self, secret: str) -> None:
        self._key = secret.encode("utf-8", "strict")
        self._accepted: dict[str, int] = {}
        self._oldest = 0
        self._earliest_nonce = float("inf")
        self._lock = threading.Lock()

    def verify(self, method: str, path: str, headers: Mapping[str, str], body: bytes) -> None:
        timestamp = headers.get("x-tpf-worker-timestamp", "")
        nonce = headers.get("x-tpf-worker-nonce", "")
        signature = headers.get("x-tpf-worker-signature", "")
        issued = _timestamp_ms(timestamp)
        now = time.time_ns() // 1_000_000
        if (abs(now - issued) > _TOLERANCE_MS or re.fullmatch(r"[!-~]{1,128}", nonce) is None
                or re.fullmatch(r"[a-f0-9]{64}", signature) is None):
            raise _RejectedRequest(401)
        digest = hashlib.sha256(body).hexdigest()
        canonical = f"{method}\n{path}\n{timestamp}\n{nonce}\n{digest}".encode("utf-8")
        if not hmac.compare_digest(hmac.digest(self._key, canonical, "sha256"), bytes.fromhex(signature)):
            raise _RejectedRequest(401)

        # Preserve live nonces at capacity and never revive an expired timestamp on clock rollback.
        with self._lock:
            self._oldest = max(self._oldest, now - _TOLERANCE_MS)
            if self._earliest_nonce < self._oldest:
                self._accepted = {seen: at for seen, at in self._accepted.items() if at >= self._oldest}
                self._earliest_nonce = min(self._accepted.values(), default=float("inf"))
            if issued < self._oldest or nonce in self._accepted or len(self._accepted) == _NONCE_LIMIT:
                raise _RejectedRequest(401)
            self._accepted[nonce] = issued
            self._earliest_nonce = min(self._earliest_nonce, issued)


class _PreparedRuntime:
    def __init__(self, runtime: _Runtime) -> None:
        self._runtime = runtime
        self.application: _Application | None = None

    def load(self, binding: object, /) -> _Application:
        if self.application is not None:
            raise ValueError("Expected one prepared native application")
        self.application = self._runtime.load(binding)
        return self.application


class _WorkerServer(ThreadingHTTPServer):
    request_queue_size = 16

    def __init__(self, address: tuple[str, int], handler: type[BaseHTTPRequestHandler], tls: ssl.SSLContext) -> None:
        self._tls = tls
        self._connections = threading.BoundedSemaphore(self.request_queue_size)
        super().__init__(address, handler)

    def process_request(self, request: _ServerRequest, client_address: tuple[str, int]) -> None:
        if not self._connections.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._connections.release()
            raise

    def process_request_thread(self, request: _ServerRequest, client_address: tuple[str, int]) -> None:
        connection = cast(socket.socket, request)  # HTTPServer is a TCP server.
        try:
            connection.settimeout(_INTAKE_TIMEOUT)
            with self._tls.wrap_socket(connection, server_side=True) as secured:
                super().process_request_thread(secured, client_address)
        except OSError:
            connection.close()
        finally:
            self._connections.release()

    def handle_error(self, request: _ServerRequest, client_address: tuple[str, int]) -> None:
        # Do not expose request contents or business exception diagnostics via the server default logger.
        pass


def create_native_transition_worker(
    *, bind: Callable[[_PreparedRuntime], object], expected_projection: bytes,
    tenant_id: str, pipeline_id: str, contract_version: str, release_version: str, artifact_id: str,
    artifact_digest: str, profiles: Mapping[str, str], shared_secret: str, certfile: str | PathLike[str],
    keyfile: str | PathLike[str], address: tuple[str, int], on_observation: _Observer | None = None,
) -> ThreadingHTTPServer:
    """Bind an HTTPS listener to a trusted, verified prepared application and projection."""
    pins = {"tenantId": tenant_id, "pipelineId": pipeline_id, "contractVersion": contract_version,
            "releaseVersion": release_version}
    if (any(type(value) is not str or _JAVA_NON_BLANK.search(value) is None
            for value in (*pins.values(), artifact_id, shared_secret))
            or any(type(value) is not str or re.fullmatch(r"sha256:[a-f0-9]{64}", value) is None
                   for value in (contract_version, artifact_digest))
            or type(expected_projection) is not bytes or dict(profiles) != _PROFILES or not callable(bind)):
        raise ValueError("Invalid native worker deployment binding")
    for value in (*pins.values(), artifact_id, shared_secret):
        value.encode("utf-8", "strict")
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.minimum_version = ssl.TLSVersion.TLSv1_2
    tls.load_cert_chain(certfile, keyfile)
    tls.set_alpn_protocols(["http/1.1"])

    runtime = _PreparedRuntime(create_python_local_runtime(expected_projection=expected_projection,
                                                           on_observation=on_observation))
    bind(runtime)
    application = runtime.application
    if application is None or application.projection.pipeline_id != pipeline_id:
        raise ValueError("Prepared application does not match the native worker deployment binding")
    # The existing runtime admitted the exact projection, policy and nominal boundary types.
    steps = application.projection.steps
    input_type, output_type = steps[0], steps[-1]
    carriers = application.projection.carriers
    step_ids = {step.id for step in steps}
    payloads = PolyglotInt32PayloadBinding(input_type.input_id, *(step.output_id for step in steps))
    capabilities = {"protocolVersion": "1", "providerName": "rest", "pipelineId": pipeline_id,
                    "contractVersion": contract_version, "releaseVersion": release_version, "artifactId": artifact_id,
                    "artifactDigest": artifact_digest, "payloadEncodings": [_PROFILES["outerEncoding"]],
                    "workerProtocols": ["local", "rest"]}
    authentication = _Authentication(shared_secret)

    async def invoke(number: int, execution_id: str, trace_id: str) -> dict[str, Any]:
        result = await application.run(input_type.input_type(number), carriers.invocation_context(execution_id, trace_id))
        if type(result) is carriers.completed:
            value = getattr(result, "outcome").value.value
            return {"outcome": "COMPLETED", "outputPayloads": [payloads.encode(output_type.output_id, value)],
                    "terminalOutputPublished": False, "terminalInputPassthrough": False}
        if type(result) is carriers.failed:
            failure = getattr(result, "failure")
            diagnostic = _DIAGNOSTICS.get(failure.code)
            if diagnostic is not None and failure.step_id in step_ids:
                return {"outcome": "FAILED", "resultProfile": _PROFILES["resultProfile"],
                        "failure": {"code": diagnostic[0], "diagnostic": diagnostic[1], "stepId": failure.step_id}}
        raise RuntimeError("Unsupported native terminal result")

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self) -> None:
            super().setup()
            self._intake_deadline = time.monotonic() + _INTAKE_TIMEOUT
            self._intake_timer = threading.Timer(_INTAKE_TIMEOUT, self._stop_reading)
            self._intake_timer.daemon = True
            self._intake_timer.start()

        def finish(self) -> None:
            self._intake_timer.cancel()
            super().finish()

        def _stop_reading(self) -> None:
            try:
                self.connection.shutdown(socket.SHUT_RD)
            except OSError:
                pass

        def parse_request(self) -> bool:
            # Bound raw bytes while the standard parser reads headers. Restore the original
            # stream for the body, including any bytes already buffered by the connection.
            stream = self.rfile
            self.rfile = _BoundedHeaderReader(stream)
            try:
                parsed = super().parse_request()
            finally:
                self.rfile = stream
            if not parsed:
                return False
            # Check the raw target. BaseHTTPRequestHandler normalises leading slashes.
            if (self.command, self.requestline.split()[1]) not in {("GET", _CAPABILITIES_PATH), ("POST", _EXECUTE_PATH)}:
                self._respond(404)
                return False
            return True

        def do_GET(self) -> None:
            self._handle(_CAPABILITIES_PATH)

        def do_POST(self) -> None:
            self._handle(_EXECUTE_PATH)

        def handle_expect_100(self) -> bool:
            self._respond(400)
            return False

        def log_message(self, format: str, *args: object) -> None:
            pass

        def send_error(self, code: int, message: str | None = None, explain: str | None = None) -> None:
            self._respond(code)

        def _handle(self, path: str) -> None:
            execute = path == _EXECUTE_PATH
            try:
                headers = self._headers()
                body = self._read_body(headers, execute)
                self._intake_timer.cancel()
                if time.monotonic() > self._intake_deadline:
                    raise _RejectedRequest(408)
                authentication.verify(self.command, path, headers, body)
                if execute:
                    invocation = _read_command(body, pins, payloads, input_type.input_id)
            except _RejectedRequest as error:
                self._respond(error.status)
                return
            except (ValueError, RecursionError, OSError):
                self._respond(400)
                return
            if not execute:
                self._respond(200, capabilities)
                return
            try:
                # Once admitted, losing the HTTP client does not cancel or retry business execution.
                response = asyncio.run(invoke(*invocation))
            except Exception:
                self._respond(500)
                return
            self._respond(200, response)

        def _headers(self) -> dict[str, str]:
            headers: dict[str, str] = {}
            if self.headers.defects:
                raise _RejectedRequest(400)
            for name, value in self.headers.raw_items():
                name = name.lower()
                if name in headers or "\r" in value or "\n" in value:
                    raise _RejectedRequest(400)
                headers[name] = value.strip(" \t")
            if headers.keys() & {"content-encoding", "transfer-encoding", "expect"}:
                raise _RejectedRequest(400)
            return headers

        def _read_body(self, headers: Mapping[str, str], execute: bool) -> bytes:
            length = headers.get("content-length", "" if execute else "0")
            if re.fullmatch(r"(?:0|[1-9][0-9]{0,19})", length) is None:
                raise _RejectedRequest(400)
            count = int(length)
            if count > _BODY_LIMIT:
                raise _RejectedRequest(413)
            if not execute and count != 0:
                raise _RejectedRequest(400)
            if execute and headers.get("content-type", "").lower() != "application/json":
                raise _RejectedRequest(415)
            body = self.rfile.read(count)
            if len(body) != count:
                raise _RejectedRequest(400)
            return body

        def _respond(self, status: int, value: dict[str, Any] | None = None) -> None:
            self.close_connection = True
            body = b"" if value is None else json.dumps(value, separators=(",", ":")).encode("utf-8")
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
            except OSError:
                pass

    return _WorkerServer(address, Handler, tls)
