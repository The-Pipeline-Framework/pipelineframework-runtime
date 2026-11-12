from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
from http.client import HTTPResponse, HTTPSConnection
import json
from pathlib import Path
import socket
import ssl
import subprocess
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch
from uuid import uuid4

from test_runtime import binding
from tpf_experimental_python_local import PythonLocalDiagnostic, StructuredFailure
from tpf_experimental_python_local.worker import (
    _Authentication,
    _RejectedRequest,
    create_native_transition_worker,
)


EXECUTE_PATH = "/pipeline/worker/transitions/execute"
CAPABILITIES_PATH = "/pipeline/worker/capabilities"
PROFILES = {"outerEncoding": "application/tpf-transition-envelope+json",
            "payloadEncoding": "tpf-polyglot-int32-json/1", "executionProfile": "tpf-polyglot-unary-policy/1",
            "resultProfile": "tpf-polyglot-failed-result/1"}
CONTRACT = "sha256:" + "1" * 64
ARTIFACT = "sha256:" + "2" * 64
SECRET = "worker-secret"


def command(**changes):
    fields = {"tenantId": "tenant", "pipelineId": "local", "contractVersion": CONTRACT, "releaseVersion": "release",
              "executionId": "execution", "traceId": "tracé", "transitionKey": "transition", "attempt": 0,
              "executionVersion": 2, "currentStepIndex": 0, "stopBeforeStepIndex": -1, "resultShape": "SINGLE",
              "redriveIntent": "REPLAY", "redriveStepIndex": -1, "redriveCommandId": None, "redriveReason": None,
              "pageContext": None, "awaitOccurrenceCause": None, "payloadTypeId": "Input",
              "payloadEncoding": PROFILES["payloadEncoding"], "payload": '{"value":3}'}
    return json.dumps(fields | changes, ensure_ascii=False, separators=(",", ":")).encode()


def signed(method, path, body, *, timestamp=None, nonce=None, signature=None):
    timestamp = timestamp or datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "001Z")
    nonce = nonce or str(uuid4())
    canonical = f"{method}\n{path}\n{timestamp}\n{nonce}\n{hashlib.sha256(body).hexdigest()}".encode()
    return {"X-TPF-Worker-Timestamp": timestamp, "X-TPF-Worker-Nonce": nonce,
            "X-TPF-Worker-Signature": signature or hmac.digest(SECRET.encode(), canonical, "sha256").hex()}


class WorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        directory = TemporaryDirectory(prefix="tpf-python-worker-")
        cls.addClassCleanup(directory.cleanup)
        cls.keyfile = Path(directory.name) / "key.pem"
        cls.certfile = Path(directory.name) / "cert.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1",
                        "-keyout", str(cls.keyfile), "-out", str(cls.certfile)], check=True, capture_output=True)
        cls.context = ssl.create_default_context(cafile=cls.certfile)

    def options(self, *, handlers=None):
        generated = binding()
        generated.handlers.update(handlers or {})
        return {"bind": lambda runtime: runtime.load(generated), "expected_projection": generated.projection_bytes,
                "tenant_id": "tenant", "pipeline_id": "local", "contract_version": CONTRACT,
                "release_version": "release", "artifact_id": "runnable", "artifact_digest": ARTIFACT,
                "profiles": PROFILES, "shared_secret": SECRET, "certfile": self.certfile, "keyfile": self.keyfile,
                "address": ("127.0.0.1", 0)}

    def listen(self, **options):
        server = create_native_transition_worker(**options)
        self.addCleanup(server.server_close)
        thread = threading.Thread(target=server.serve_forever, args=(0.01,), daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.shutdown)
        return server

    def send(self, server, method, path, body=b"", *, headers=None, extra=()):
        connection = HTTPSConnection("127.0.0.1", server.server_port, context=self.context, timeout=5)
        try:
            connection.putrequest(method, path, skip_accept_encoding=True)
            if method == "POST":
                connection.putheader("Content-Type", "application/json")
                connection.putheader("Content-Length", str(len(body)))
            for name, value in (*(headers or signed(method, path, body)).items(), *extra):
                connection.putheader(name, value)
            connection.endheaders(body)
            response = connection.getresponse()
            data = response.read()
            return response.status, json.loads(data) if data else None
        finally:
            connection.close()

    def test_startup_pins_verified_projection_pipeline_and_profiles(self):
        options = self.options()
        with self.assertRaisesRegex(PythonLocalDiagnostic, "TRUSTED_PROJECTION_MISMATCH"):
            create_native_transition_worker(**(options | {"expected_projection": options["expected_projection"] + b"\n"}))
        with self.assertRaisesRegex(ValueError, "does not match"):
            create_native_transition_worker(**(options | {"pipeline_id": "different"}))
        for field in PROFILES:
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "Invalid native worker deployment binding"):
                create_native_transition_worker(**(options | {"profiles": PROFILES | {field: "future/2"}}))

    def test_accepts_java_golden_raw_body_signature_and_bounds_nonce_cache(self):
        now = 1789380930000
        with patch("tpf_experimental_python_local.worker.time.time_ns", return_value=now * 1_000_000) as clock:
            server = self.listen(**self.options())
            golden = b'{"outcome":"COMPLETED"}'
            headers = signed("POST", EXECUTE_PATH, golden, timestamp="2026-09-14T10:15:30Z", nonce="nonce-1",
                             signature="e22366b38e2b071ccc7e31df9d7c4f7ad13d0b6ac17d2ef0b0f43e89cfb53523")
            self.assertEqual((400, None), self.send(server, "POST", EXECUTE_PATH, golden, headers=headers))
            self.assertEqual((401, None), self.send(server, "POST", EXECUTE_PATH, golden, headers=headers))

            auth = _Authentication(SECRET)

            def verify(nonce, timestamp="2026-09-14T10:15:30.000999999Z"):
                headers = signed("GET", CAPABILITIES_PATH, b"", timestamp=timestamp, nonce=nonce)
                auth.verify("GET", CAPABILITIES_PATH, {key.lower(): value for key, value in headers.items()}, b"")

            for index in range(10_000):
                verify(f"capacity-{index}")
            for nonce in ("one-too-many", "capacity-0"):
                with self.subTest(nonce=nonce), self.assertRaises(_RejectedRequest) as raised:
                    verify(nonce)
                self.assertEqual(401, raised.exception.status)
            clock.return_value = (now + 120_000) * 1_000_000
            with self.assertRaises(_RejectedRequest):
                verify("one-too-many")
            clock.return_value += 1_000_000
            with self.assertRaises(_RejectedRequest):
                verify("capacity-0")
            verify("new", "2026-09-14T10:17:30.001Z")
            clock.return_value = now * 1_000_000
            with self.assertRaises(_RejectedRequest):
                verify("capacity-0")

    def test_bounds_raw_header_bytes_before_waiting_for_body(self):
        server = self.listen(**self.options())
        body = command()
        request_line = f"POST {EXECUTE_PATH} HTTP/1.1\r\n".encode()
        headers = (b"Host: localhost\r\nContent-Type: application/json\r\n"
                   + f"Content-Length: {len(body)}\r\n".encode()
                   + b"".join(f"{name}: {value}\r\n".encode()
                              for name, value in signed("POST", EXECUTE_PATH, body).items()))

        def send_raw(payload):
            with socket.create_connection(("127.0.0.1", server.server_port), timeout=3) as stream:
                with self.context.wrap_socket(stream, server_hostname="127.0.0.1") as secured:
                    secured.sendall(request_line + headers + payload)
                    with HTTPResponse(secured) as response:
                        response.begin()
                        return response.status, response.read()

        cases = {
            "leading whitespace": b"X-Pad:" + b" \t" * 4096 + b"v\r\n\r\n" + body,
            "cumulative bytes": b"X-First: " + b"a" * 4096 + b"\r\nX-Second: " + b"b" * 4096 + b"\r\n",
            "unterminated long line": b"X-Pad: " + b"a" * 8192,
        }
        for case, payload in cases.items():
            with self.subTest(case=case):
                self.assertEqual((431, b""), send_raw(payload))

        field = b"X-Pad: "
        padding = b"a" * (8192 - len(headers) - len(field) - len(b"\r\n\r\n"))
        status, response = send_raw(field + padding + b"\r\n\r\n" + body)
        self.assertEqual(200, status)
        self.assertEqual("COMPLETED", json.loads(response)["outcome"])

    def test_authenticates_capabilities_and_rejects_invalid_commands_before_business_invocation(self):
        calls = []

        def observe(event):
            if event["kind"] == "ATTEMPT_STARTED":
                calls.append((event["stepId"], event["context"].executionId, event["context"].traceId))

        server = self.listen(**self.options(), on_observation=observe)
        self.assertEqual((200, {"protocolVersion": "1", "providerName": "rest", "pipelineId": "local",
                                "contractVersion": CONTRACT, "releaseVersion": "release", "artifactId": "runnable",
                                "artifactDigest": ARTIFACT, "payloadEncodings": [PROFILES["outerEncoding"]],
                                "workerProtocols": ["local", "rest"]}), self.send(server, "GET", CAPABILITIES_PATH))
        self.assertEqual((404, None), self.send(server, "GET", CAPABILITIES_PATH + "?query=1"))
        self.assertEqual((404, None), self.send(server, "GET", "/" + CAPABILITIES_PATH))
        self.assertEqual((401, None), self.send(server, "POST", EXECUTE_PATH, command(),
                                               headers=signed("POST", EXECUTE_PATH, command(traceId="changed"))))
        stale = (datetime.now(timezone.utc) - timedelta(seconds=121)).isoformat().replace("+00:00", "Z")
        self.assertEqual((401, None), self.send(server, "POST", EXECUTE_PATH, command(),
                                               headers=signed("POST", EXECUTE_PATH, command(), timestamp=stale)))
        self.assertEqual((400, None), self.send(server, "POST", EXECUTE_PATH, command(),
                                               extra=(("x-tpf-worker-nonce", "duplicate"),)))
        self.assertEqual((400, None), self.send(server, "POST", EXECUTE_PATH, command(),
                                               extra=(("Content-Encoding", "gzip"),)))
        self.assertEqual((413, None), self.send(server, "POST", EXECUTE_PATH, b"a" * 16385))

        invalid = [command(tenantId="other"), command(pipelineId="other"), command(contractVersion="other"),
                   command(releaseVersion="other"), command(currentStepIndex=1), command(stopBeforeStepIndex=2),
                   command(resultShape="MATERIALIZED_MULTI"), command(redriveIntent="REISSUE_COMMAND"),
                   command(traceId=" \t"), command(awaitOccurrenceCause="cause"), command(pageContext={"page": 2}),
                   command(resultProfile="spoofed"), command(payloadTypeId="Second"), command(payloadEncoding="json"),
                   command(payload='{"value":1,"value":2}'), command(payload='{"value":2147483648}'),
                   command(attempt=True), command(attempt=2**31), command(executionVersion=2**63),
                   command().replace(b'"executionVersion":2', b'"executionVersion":2.0000000000000001'),
                   command().replace(b'"tenantId":', b'"tenantI\\u0064":"other","tenantId":'),
                   command().replace('"traceId":"tracé"'.encode(), b'"traceId":"\\ud800"'),
                   command()[:-1] + b',"unknown":"\xc0\xaf"}']
        for body in invalid:
            with self.subTest(body=body):
                self.assertEqual((400, None), self.send(server, "POST", EXECUTE_PATH, body))
        self.assertEqual([], calls)

        exact_long = command(executionVersion=2**63 - 1)
        headers = signed("POST", EXECUTE_PATH, exact_long)
        completed = (200, {"outcome": "COMPLETED", "outputPayloads": [{"payloadTypeId": "Output",
                            "payloadEncoding": PROFILES["payloadEncoding"], "payload": '{"value":5}'}],
                            "terminalOutputPublished": False, "terminalInputPassthrough": False})
        with ThreadPoolExecutor(2) as executor:
            replies = [executor.submit(self.send, server, "POST", EXECUTE_PATH, exact_long, headers=headers)
                       for _ in range(2)]
            results = [reply.result(timeout=5) for reply in replies]
        self.assertCountEqual([completed, (401, None)], results)
        self.assertEqual([(step, "execution", "tracé") for step in "ABC"], calls)

    def test_serializes_only_safe_portable_failure_outcomes_from_existing_local_policy(self):
        cases = [("TRANSIENT", "TRANSIENT_EXHAUSTED", "Local retry budget exhausted", 2),
                 ("BUSINESS_REJECTION", "BUSINESS_REJECTION", "Business function rejected input", 1),
                 ("raw", "UNCLASSIFIED", "Unclassified business function failure", 1)]
        for original, code, diagnostic, attempts in cases:
            with self.subTest(code=code):
                calls = []

                def fail(input, context):
                    calls.append(input.value)
                    if original == "raw":
                        raise RuntimeError("private business detail")
                    raise StructuredFailure({"code": original, "message": "private business detail", "stepId": "B",
                                             "owner": "LOCAL_SHELL", "retryable": original == "TRANSIENT"})

                def never(input, context):
                    self.fail("business function after failed step")

                server = self.listen(**self.options(handlers={"second": fail, "third": never}))
                self.assertEqual((200, {"outcome": "FAILED", "resultProfile": PROFILES["resultProfile"],
                                        "failure": {"code": code, "diagnostic": diagnostic, "stepId": "B"}}),
                                 self.send(server, "POST", EXECUTE_PATH, command()))
                self.assertEqual([4] * attempts, calls)

        rejected = self.listen(**self.options(handlers={"second": lambda input, context: {"kind": "EMPTY"},
                                                        "third": never}))
        self.assertEqual((500, None), self.send(rejected, "POST", EXECUTE_PATH, command()))
