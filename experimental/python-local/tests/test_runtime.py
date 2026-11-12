import asyncio
import gc
import hashlib
import json
import unittest
from contextvars import ContextVar
from dataclasses import field, make_dataclass
from functools import partial
from types import SimpleNamespace
from unittest.mock import Mock

from tpf_experimental_python_local import (
    CancellationToken,
    PythonLocalDiagnostic,
    StructuredFailure,
    create_python_local_runtime,
)


def record(name, *fields, tag=None):
    layout = [(key, annotation) for key, annotation in fields]
    if tag is not None:
        key, value = tag
        layout.append((key, str, field(init=False, default=value)))
    return make_dataclass(name, layout, slots=True, frozen=True, module=__name__)


Input, First, Second, Output = (
    record(name, ("value", int)) for name in ("Input", "First", "Second", "Output")
)
Context = record("Context", ("executionId", str), ("traceId", str))
Value = record("Value", ("value", object), tag=("kind", "VALUE"))
Failure = record("Failure", ("step_id", str), ("code", str), ("message", str), ("retryable", bool),
                 tag=("owner", "LOCAL_SHELL"))
Completed = record("Completed", ("outcome", Value), tag=("kind", "COMPLETED"))
Failed = record("Failed", ("failure", Failure), tag=("kind", "FAILED"))
Rejected = record("Rejected", ("diagnostic", str), tag=("kind", "REJECTED"))
Cancelled = record("Cancelled", tag=("kind", "CANCELLED"))
CONTEXT = Context("execution", "trace")
MODELS = {model.__name__: model for model in (Input, First, Second, Output)}
CARRIERS = SimpleNamespace(
    invocation_context=Context, value=Value, failure=Failure, completed=Completed,
    failed=Failed, rejected=Rejected, cancelled=Cancelled,
)


def projection():
    return {
        "schemaVersion": 1,
        "profile": "tpf-polyglot-unary-policy/1",
        "kind": "compiler-python-local",
        "pipeline": {"id": "local", "inputType": "Input", "outputType": "Output", "transport": "LOCAL",
                     "platform": "COMPUTE", "execution": "DIRECT"},
        "requiredCapabilities": ["pure", "linear", "unary", "local", "retry", "cancellation", "context"],
        "types": [{"id": name, "fields": [
            {"name": "value", "type": "int32", "presence": "REQUIRED", "nullability": "NON_NULL"},
        ]} for name in MODELS],
        "steps": [
            {"id": "A", "inputType": "Input", "outputType": "First", "handler": "first", "retryLimit": 0},
            {"id": "B", "inputType": "First", "outputType": "Second", "handler": "second", "retryLimit": 1},
            {"id": "C", "inputType": "Second", "outputType": "Output", "handler": "third", "retryLimit": 0},
        ],
        "contextKeys": ["executionId", "traceId"],
        "policy": {"failureMapping": "structured-failure/1", "retryOwner": "LOCAL_SHELL",
                   "resultMapping": "explicit-value/1", "cancellation": "first-accepted-terminal/1",
                   "context": "explicit-snapshot/1", "recovery": "REJECT", "legacyPolicy": "REJECT"},
        "provenance": {"dslSha256": "0" * 64, "bindingsSha256": "1" * 64},
    }


def binding(source=None, **overrides):
    if source is None:
        source = json.dumps(projection()).encode()
    values = {
        "schema_version": 1,
        "projection_bytes": source,
        "projection_sha256": hashlib.sha256(source).hexdigest(),
        "model_types": MODELS,
        "carrier_types": CARRIERS,
        "handlers": {
            "first": lambda value, ctx: Value(First(value.value + 1)),
            "second": lambda value, ctx: Value(Second(value.value * 2)),
            "third": lambda value, ctx: Value(Output(value.value - 3)),
        },
    }
    return SimpleNamespace(**(values | overrides))


def application(observer=None, **handlers):
    generated = binding()
    generated.handlers.update(handlers)
    return create_python_local_runtime(on_observation=observer).load(generated)


class LoaderTests(unittest.TestCase):
    def test_rejects_incompatible_artifacts_before_invocation(self):
        never = Mock()
        runtime = create_python_local_runtime()
        original = json.dumps(projection()).encode()
        registry = binding().handlers | {"first": never}
        cases = [
            ("ARTIFACT_DIGEST_MISMATCH", binding(original, projection_sha256="0" * 64)),
            ("UNSUPPORTED_SCHEMA", binding(original, schema_version=True)),
            ("UNSUPPORTED_CAPABILITY", binding(original.replace(b'"pure"', b'"hosted"'))),
            ("INVALID_JSON", binding(original.replace(b'"schemaVersion": 1', b'"schemaVersion": 1, "schemaVersion": 1'))),
            ("INVALID_NUMBER", binding(original.replace(b'"retryLimit": 1', b'"retryLimit": 1.00000000000000000001'))),
            ("INVALID_NUMBER", binding(original.replace(b'"retryLimit": 1', b'"retryLimit": 1e1000000'))),
            ("TYPE_MISMATCH", binding(original.replace(b'"inputType": "First"', b'"inputType": "Output"'))),
            ("INVALID_MODEL_REGISTRY", binding(original, model_types=MODELS | {"First": Input})),
            ("INVALID_CARRIERS", binding(original, carrier_types=SimpleNamespace())),
            ("MISSING_HANDLER", binding(original, handlers={"first": never})),
            ("EXTRA_HANDLER", binding(original, handlers=registry | {"extra": never})),
        ]
        for expected, candidate in cases:
            with self.subTest(expected=expected), self.assertRaises(PythonLocalDiagnostic) as raised:
                runtime.load(candidate)
            self.assertEqual(raised.exception.code, expected)
        never.assert_not_called()

    def test_rejects_async_observers_and_unknown_options(self):
        async def observer(event):
            raise AssertionError("must never run")

        class Observer:
            async def __call__(self, event):
                return await observer(event)

        for callback in (observer, partial(observer), Observer(), partial(Observer())):
            with self.subTest(callback=callback), self.assertRaises(PythonLocalDiagnostic) as raised:
                create_python_local_runtime(on_observation=callback)
            self.assertEqual(raised.exception.code, "UNSUPPORTED_RUNTIME_OPTIONS")
        with self.assertRaises(PythonLocalDiagnostic) as raised:
            create_python_local_runtime(retry=None)
        self.assertEqual(raised.exception.code, "UNSUPPORTED_RUNTIME_OPTIONS")

    def test_trusted_projection_requires_immutable_bytes(self):
        class ProjectionBytes(bytes):
            pass

        for invalid in ("{}", bytearray(b"{}"), memoryview(b"{}"), ProjectionBytes(b"{}"), 42):
            with self.subTest(invalid=type(invalid)), self.assertRaises(PythonLocalDiagnostic) as raised:
                create_python_local_runtime(expected_projection=invalid)
            self.assertEqual(raised.exception.code, "UNSUPPORTED_RUNTIME_OPTIONS")

    def test_rejects_incompatible_model_and_carrier_constructors(self):
        keyword_model = make_dataclass("Input", [("value", int)], slots=True, frozen=True, kw_only=True)
        keyword_context = make_dataclass(
            "Context", [("executionId", str), ("traceId", str)], slots=True, frozen=True, kw_only=True,
        )
        carrier_layout = vars(CARRIERS) | {"invocation_context": keyword_context}
        never = Mock()
        handlers = binding().handlers | {"first": never}
        runtime = create_python_local_runtime()
        cases = (
            ("INVALID_MODEL_REGISTRY", binding(model_types=MODELS | {"Input": keyword_model}, handlers=handlers)),
            ("INVALID_CARRIERS", binding(carrier_types=SimpleNamespace(**carrier_layout), handlers=handlers)),
        )
        for expected, candidate in cases:
            with self.subTest(expected=expected):
                with self.assertRaises(PythonLocalDiagnostic) as raised:
                    runtime.load(candidate)
                self.assertEqual(raised.exception.code, expected)
        never.assert_not_called()


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_trusted_projection_matches_exact_bytes_before_registry_admission(self):
        class UnreadableRegistry(SimpleNamespace):
            @property
            def handlers(self):
                raise AssertionError("loader inspected handlers before verifying the projection")

        original = binding()
        expected = bytearray(original.projection_bytes)
        runtime = create_python_local_runtime(expected_projection=bytes(expected))
        expected[:] = expected.replace(b'"retryLimit": 1', b'"retryLimit": 2')
        for candidate in (binding(bytes(expected)), binding(original.projection_bytes + b"\n")):
            fields = vars(candidate).copy()
            del fields["handlers"]
            with self.assertRaises(PythonLocalDiagnostic) as raised:
                runtime.load(UnreadableRegistry(**fields))
            self.assertEqual(raised.exception.code, "TRUSTED_PROJECTION_MISMATCH")
            local = create_python_local_runtime().load(candidate)
            self.assertEqual(await local.run(Input(3), CONTEXT), Completed(Value(Output(5))))
        self.assertEqual(await runtime.load(original).run(Input(3), CONTEXT), Completed(Value(Output(5))))
        with self.assertRaises(PythonLocalDiagnostic) as raised:
            runtime.load(binding(original.projection_bytes, projection_sha256="0" * 64))
        self.assertEqual(raised.exception.code, "ARTIFACT_DIGEST_MISMATCH")

    async def test_exact_generated_nominal_value_and_context_admission(self):
        app = application()
        wrong_inputs = [None, {"value": 3}, First(3), Input(True), Input(3.0), Input(None),
                        Input(2**31), Input(-(2**31) - 1), object.__new__(Input)]
        for invalid in wrong_inputs:
            with self.subTest(invalid=type(invalid)):
                self.assertEqual(await app.run(invalid, CONTEXT), Rejected("INVALID_VALUE"))
        self.assertEqual(await app.run(Input(3), {"executionId": "execution", "traceId": "trace"}),
                         Rejected("INVALID_CONTEXT"))
        self.assertEqual(await app.run(Input(3), Context(" \t\r\n", "trace")), Rejected("INVALID_CONTEXT"))
        self.assertEqual(await app.run(Input(3), CONTEXT), Completed(Value(Output(5))))
        for bound in (-(2**31), 2**31 - 1):
            passthrough = application(
                first=lambda item, ctx: Value(First(item.value)),
                second=lambda item, ctx: Value(Second(item.value)),
                third=lambda item, ctx: Value(Output(item.value)),
            )
            self.assertEqual(await passthrough.run(Input(bound), CONTEXT), Completed(Value(Output(bound))))

    async def test_coroutines_and_only_exact_result_carriers(self):
        async def good(item, ctx):
            await asyncio.sleep(0)
            return Value(First(item.value + 1))

        class OtherAwaitable:
            invoked = False

            def __await__(self):
                self.invoked = True
                return good(Input(3), CONTEXT).__await__()

        fake = OtherAwaitable()
        self.assertEqual(await application(first=good).run(Input(3), CONTEXT), Completed(Value(Output(5))))
        for result in (fake, Value(Second(4)), Value(First(True)), First(4), object.__new__(Value)):
            app = application(first=lambda item, ctx: result)
            self.assertEqual(await app.run(Input(3), CONTEXT), Rejected("INVALID_VALUE"))
        self.assertFalse(fake.invoked)
        self.assertEqual(await application(first=lambda *args: {"kind": "EMPTY"}).run(Input(3), CONTEXT),
                         Rejected("UNSUPPORTED_EMPTY"))

    async def test_retries_use_admitted_input_and_context_snapshots(self):
        caller_input = Input(3)
        caller_context = Context("execution", "trace")
        attempts = []

        def first(item, ctx):
            self.assertEqual((item.value, ctx.traceId), (3, "trace"))
            return Value(First(4))

        def second(item, ctx):
            attempts.append((item.value, ctx.traceId))
            object.__setattr__(item, "value", 999)
            object.__setattr__(ctx, "traceId", "mutated handler context")
            if len(attempts) == 1:
                raise StructuredFailure({"code": "TRANSIENT", "message": "try again", "stepId": "B",
                                         "owner": "LOCAL_SHELL", "retryable": True})
            return Value(Second(8))

        def observer(event):
            if event["kind"] == "ATTEMPT_STARTED":
                object.__setattr__(caller_input, "value", 99)
                object.__setattr__(caller_context, "traceId", "mutated caller context")
                object.__setattr__(event["context"], "traceId", "mutated observer context")
            if event["kind"] == "TERMINAL":
                object.__setattr__(event["record"].outcome.value, "value", 99)

        app = application(observer, first=first, second=second)
        self.assertEqual(await app.run(caller_input, caller_context), Completed(Value(Output(5))))
        self.assertEqual(attempts, [(4, "trace"), (4, "trace")])

    async def test_overlapping_contexts_remain_separate(self):
        ambient = ContextVar("ambient")
        left_started, both_started = asyncio.Event(), asyncio.Event()

        async def first(item, ctx):
            if ctx.executionId == "left":
                left_started.set()
            else:
                both_started.set()
            await both_started.wait()
            self.assertEqual(ambient.get(), ctx.executionId)
            return Value(First(item.value + 1))

        app = application(first=first)
        ambient.set("left")
        left = asyncio.create_task(app.run(Input(3), Context("left", "trace-left")))
        await left_started.wait()
        ambient.set("right")
        right = asyncio.create_task(app.run(Input(9), Context("right", "trace-right")))
        self.assertEqual(await asyncio.gather(left, right), [Completed(Value(Output(5))), Completed(Value(Output(17)))])

    async def test_explicit_cancellation_is_inline_and_drains(self):
        token = CancellationToken()
        seen = []
        started, release = asyncio.Event(), asyncio.Event()
        never = Mock()

        async def first(item, ctx):
            started.set()
            await release.wait()
            raise RuntimeError("late handler failure")

        app = application(seen.append, first=first, second=never)
        task = asyncio.create_task(app.run(Input(3), CONTEXT, token))
        await started.wait()
        token.cancel()
        self.assertEqual(seen[-1]["record"], Cancelled())
        self.assertFalse(task.done())
        release.set()
        self.assertEqual(await task, Cancelled())
        self.assertEqual(await app.run(Input(3), CONTEXT, token), Cancelled())
        never.assert_not_called()

    async def test_malformed_signal_cannot_commit_cancellation_during_admission(self):
        async def async_subscribe(callback):
            callback()

        def incomplete_subscribe(callback):
            callback()

        app = application(first=Mock(side_effect=AssertionError("invalid signal entered handler")))
        for signal in (
            SimpleNamespace(cancelled=True, subscribe=incomplete_subscribe),
            SimpleNamespace(cancelled=False, subscribe=incomplete_subscribe),
            SimpleNamespace(cancelled=False, subscribe=async_subscribe),
            SimpleNamespace(cancelled=False, subscribe=lambda callback: async_subscribe(callback)),
        ):
            self.assertEqual(await app.run(Input(3), CONTEXT, signal), Rejected("INVALID_SIGNAL"))

    async def test_repeated_raw_cancellation_drains_before_detaching(self):
        terminal = asyncio.Event()
        release, started = asyncio.Event(), asyncio.Event()
        records = []

        def observer(event):
            if event["kind"] == "TERMINAL":
                records.append(event["record"])
                terminal.set()

        async def first(item, ctx):
            started.set()
            await release.wait()
            return Value(First(4))

        app = application(observer, first=first, second=Mock(side_effect=AssertionError("late step")))
        task = asyncio.create_task(app.run(Input(3), CONTEXT))
        await started.wait()
        task.cancel("first")
        await terminal.wait()
        task.cancel("second")
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        release.set()
        with self.assertRaisesRegex(asyncio.CancelledError, "first"):
            await task
        self.assertEqual(records, [Cancelled()])
        app.projection.steps[1].invoke.assert_not_called()

    async def test_handler_raised_and_self_cancellation_are_failures(self):
        async def explicit(item, ctx):
            raise asyncio.CancelledError("private detail")

        async def self_cancel(item, ctx):
            asyncio.current_task().cancel()
            await asyncio.sleep(0)
            return Value(First(4))

        async def self_cancel_before_return(item, ctx):
            asyncio.current_task().cancel()
            return Value(First(4))

        for handler in (explicit, self_cancel, self_cancel_before_return):
            result = await application(first=handler).run(Input(3), CONTEXT)
            self.assertEqual(result, Failed(Failure("A", "UNCLASSIFIED", "Unclassified business function failure", False)))

    async def test_terminal_observer_self_cancel_preserves_handler_outcome(self):
        async def self_cancel(item, ctx):
            asyncio.current_task().cancel()
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                if not handles_cancellation:
                    raise
            return Value(First(4))

        def observer(event):
            if event["kind"] == "TERMINAL":
                task = asyncio.current_task()
                terminals.append((event["record"], task.cancelling()))
                task.cancel()

        failure = Failed(Failure("A", "UNCLASSIFIED", "Unclassified business function failure", False))
        for handles_cancellation, expected in ((False, failure), (True, Completed(Value(Output(5))))):
            with self.subTest(handles_cancellation=handles_cancellation):
                terminals = []
                app = application(observer, first=self_cancel)
                self.assertEqual(await app.run(Input(3), CONTEXT), expected)
                self.assertEqual(terminals, [(expected, 0)])

    async def test_sync_return_yields_before_handoff_and_final_observation_preserves_commit(self):
        token = CancellationToken()
        second = Mock()

        def first(item, ctx):
            asyncio.get_running_loop().call_soon(token.cancel)
            return Value(First(4))

        self.assertEqual(await application(first=first, second=second).run(Input(3), CONTEXT, token), Cancelled())
        second.assert_not_called()

        token = CancellationToken()
        events = []

        def observer(event):
            events.append(event)
            if event["kind"] == "RETURNED" and event["stepId"] == "C":
                token.cancel()

        self.assertEqual(await application(observer).run(Input(3), CONTEXT, token), Completed(Value(Output(5))))
        self.assertEqual([event["kind"] for event in events[-2:]], ["RETURNED", "TERMINAL"])
        self.assertEqual(events[-1]["record"], Completed(Value(Output(5))))

    async def test_observer_returns_are_discarded_safely(self):
        loop = asyncio.get_running_loop()
        reports = []
        loop.set_exception_handler(lambda _loop, context: reports.append(context))
        release, finish = asyncio.Event(), asyncio.Event()
        coroutine_ran = False

        async def lazy():
            nonlocal coroutine_ran
            coroutine_ran = True

        async def fail_late():
            await release.wait()
            finish.set()
            raise RuntimeError("observer task failed")

        class Future(asyncio.Future):
            def add_done_callback(self, *args, **kwargs):
                raise AssertionError("overridden add_done_callback")

            def exception(self):
                raise AssertionError("overridden exception")

        class Awaitable:
            touched = False

            def __await__(self):
                self.touched = True
                return lazy().__await__()

        custom = Awaitable()
        finished = Future()
        finished.set_exception(ValueError("finished observer future failed"))
        cancelled = Future()
        cancelled.cancel()
        lazy_coroutine = lazy()
        pending = asyncio.create_task(fail_late())
        results = iter((lazy_coroutine, finished, pending, cancelled, custom))

        def observer(event):
            if event["kind"] == "RETURNED" and event["stepId"] == "C":
                asyncio.current_task().cancel()
                raise asyncio.CancelledError()
            return next(results, None)

        app = application(observer)
        self.assertEqual(await app.run(Input(3), CONTEXT), Completed(Value(Output(5))))
        self.assertFalse(pending.done())
        release.set()
        await finish.wait()
        await asyncio.sleep(0)
        self.assertIsNone(lazy_coroutine.cr_frame)
        self.assertFalse(coroutine_ran)
        self.assertFalse(custom.touched)
        del pending, finished
        gc.collect()
        self.assertEqual(reports, [])

    async def test_observer_return_does_not_close_a_suspended_coroutine(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def independent_work():
            started.set()
            await release.wait()
            return 1

        coroutine = independent_work()
        task = asyncio.create_task(coroutine)
        await started.wait()
        try:
            self.assertEqual(await application(lambda event: coroutine).run(Input(3), CONTEXT),
                             Completed(Value(Output(5))))
        finally:
            release.set()
        self.assertEqual(await asyncio.wait_for(task, timeout=5), 1)

    async def test_returned_coroutine_scheduled_on_the_loop_keeps_its_task(self):
        async def independent_work():
            return 1

        for malformed_signal in (False, True):
            with self.subTest(malformed_signal=malformed_signal):
                scheduled = []

                def schedule():
                    coroutine = independent_work()
                    scheduled.append(asyncio.create_task(coroutine, eager_start=False))
                    return coroutine

                def observer(event):
                    if event["kind"] == "ATTEMPT_STARTED" and event["stepId"] == "A":
                        return schedule()

                if malformed_signal:
                    signal = SimpleNamespace(cancelled=False, subscribe=lambda callback: schedule())
                    result = await application().run(Input(3), CONTEXT, signal)
                    self.assertEqual(result, Rejected("INVALID_SIGNAL"))
                else:
                    result = await application(observer).run(Input(3), CONTEXT)
                    self.assertEqual(result, Completed(Value(Output(5))))
                self.assertEqual(len(scheduled), 1)
                self.assertEqual(await asyncio.wait_for(scheduled[0], timeout=5), 1)


if __name__ == "__main__":
    unittest.main()
