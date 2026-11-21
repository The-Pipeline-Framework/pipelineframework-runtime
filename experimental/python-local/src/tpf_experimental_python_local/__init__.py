"""Runtime for compiler-generated TPF Python LOCAL applications."""

import asyncio
import inspect
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from functools import partial
from types import CoroutineType, MappingProxyType
from typing import Any, Protocol, cast

from ._admission import Carriers, Projection, PythonLocalDiagnostic, Step, admit
from ._payload_binding import PolyglotInt32PayloadBinding, SerializedTransitionPayload

__all__ = [
    "CancellationToken",
    "PolyglotInt32PayloadBinding",
    "PythonLocalDiagnostic",
    "SerializedTransitionPayload",
    "StructuredFailure",
    "create_python_local_runtime",
]

_UNCLASSIFIED = "Unclassified business function failure"
_FAILURE_KEYS = {"code", "message", "retryable", "owner", "stepId"}
_Observer = Callable[[Mapping[str, Any]], object]


class _CancellationSignal(Protocol):
    @property
    def cancelled(self) -> bool: ...

    def subscribe(self, callback: Callable[[], None], /) -> Callable[[], None]: ...


class CancellationToken:
    """A single-event-loop signal that notifies subscribers inline."""

    def __init__(self) -> None:
        self._cancelled = False
        self._callbacks: dict[object, Callable[[], None]] = {}

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def subscribe(self, callback: Callable[[], None], /) -> Callable[[], None]:
        key = object()
        if self._cancelled:
            callback()
        else:
            self._callbacks[key] = callback

        def unsubscribe() -> None:
            self._callbacks.pop(key, None)

        return unsubscribe

    def cancel(self) -> None:
        if self._cancelled:
            return
        self._cancelled = True
        callbacks = tuple(self._callbacks.values())
        self._callbacks.clear()
        for callback in callbacks:
            try:
                callback()
            except (Exception, asyncio.CancelledError):
                # A subscriber cannot prevent the remaining inline notifications.
                continue


class StructuredFailure(Exception):
    """Explicit host carrier for the shared structured failure envelope."""

    def __init__(self, envelope: object, /) -> None:
        super().__init__("Structured business function failure")
        self._envelope = envelope


def _bind_failure(error: BaseException, step_id: str, carriers: Carriers) -> Any:
    if type(error) is not StructuredFailure:
        return carriers.failure(step_id, "UNCLASSIFIED", _UNCLASSIFIED, False)
    envelope = getattr(error, "_envelope", None)
    if type(envelope) is not dict:
        raise PythonLocalDiagnostic("MALFORMED_FAILURE")
    fields = envelope.copy()
    if fields.keys() != _FAILURE_KEYS or (
        type(fields["retryable"]) is not bool
        or any(type(fields[key]) is not str for key in _FAILURE_KEYS - {"retryable"})
    ):
        raise PythonLocalDiagnostic("MALFORMED_FAILURE")
    code, message, retryable = fields["code"], fields["message"], fields["retryable"]
    if code not in {"TRANSIENT", "BUSINESS_REJECTION", "UNCLASSIFIED"}:
        raise PythonLocalDiagnostic("UNKNOWN_FAILURE_CODE")
    if (
        fields["owner"] != "LOCAL_SHELL" or fields["stepId"] != step_id
        or retryable != (code == "TRANSIENT") or (code == "UNCLASSIFIED" and message != _UNCLASSIFIED)
    ):
        raise PythonLocalDiagnostic("FAILURE_BINDING_MISMATCH")
    return carriers.failure(step_id, code, message, retryable)


def _bind_value(value: Any, model: type) -> int:
    number = getattr(value, "value", None) if type(value) is model else None
    if type(number) is not int or not -(2**31) <= number < 2**31:
        raise PythonLocalDiagnostic("INVALID_VALUE")
    return number


def _bind_result(result: Any, carriers: Carriers, output: type) -> int:
    if type(result) is dict and result.keys() == {"kind"}:
        kind = result["kind"]
        if type(kind) is str and kind == "EMPTY":
            raise PythonLocalDiagnostic("UNSUPPORTED_EMPTY")
    if type(result) is not carriers.value:
        raise PythonLocalDiagnostic("INVALID_VALUE")
    kind = getattr(result, "kind", None)
    if type(kind) is not str or kind != "VALUE":
        raise PythonLocalDiagnostic("INVALID_VALUE")
    return _bind_value(getattr(result, "value", None), output)


def _bind_context(value: Any, model: type) -> tuple[str, str]:
    if type(value) is not model:
        raise PythonLocalDiagnostic("INVALID_CONTEXT")
    execution_id, trace_id = getattr(value, "executionId", None), getattr(value, "traceId", None)
    if (
        type(execution_id) is not str or type(trace_id) is not str
        or not execution_id.strip(" \t\r\n") or not trace_id.strip(" \t\r\n")
    ):
        raise PythonLocalDiagnostic("INVALID_CONTEXT")
    return execution_id, trace_id


def _is_async_callable(callback: object) -> bool:
    while issubclass(type(callback), partial):
        callback = cast(partial[Any], callback).func
    call = getattr(type(callback), "__call__", None)
    return any(inspect.iscoroutinefunction(candidate) or inspect.isasyncgenfunction(candidate)
               for candidate in (callback, call))


def _consume_future(future: asyncio.Future[Any]) -> None:
    if not asyncio.Future.cancelled(future):
        asyncio.Future.exception(future)


def _restore_cancellation_count(task: asyncio.Task[Any] | None, previous: int) -> None:
    if task is not None:
        while task.cancelling() > previous:
            task.uncancel()


def _close_unowned_coroutine(coroutine: CoroutineType[Any, Any, Any]) -> None:
    if inspect.getcoroutinestate(coroutine) != inspect.CORO_CREATED:
        return
    # A scheduled task can own the coroutine before it has taken its first turn.
    if not any(asyncio.Task.get_coro(task) is coroutine for task in asyncio.all_tasks()):
        coroutine.close()


def _observe(callback: _Observer, fields: dict[str, Any]) -> None:
    task = asyncio.current_task()
    pending = task.cancelling() if task is not None else 0
    try:
        result = callback(MappingProxyType(fields))
        if type(result) is CoroutineType:
            _close_unowned_coroutine(result)
        elif issubclass(type(result), asyncio.Future):
            future = cast(asyncio.Future[Any], result)
            if asyncio.Future.done(future):
                _consume_future(future)
            else:
                asyncio.Future.add_done_callback(future, _consume_future)
    except (Exception, asyncio.CancelledError):
        # Observations cannot settle business execution.
        pass
    finally:
        # A synchronous hook runs on the shell task. It may cancel a token, but
        # cancellation it requests on that same task is not a caller detachment.
        _restore_cancellation_count(task, pending)


@dataclass(frozen=True, slots=True)
class _Returned:
    value: object


@dataclass(frozen=True, slots=True)
class _Raised:
    error: BaseException


async def _invoke(step: Step, value: object, context: object) -> _Returned | _Raised:
    task = asyncio.current_task()
    pending = task.cancelling() if task is not None else 0
    try:
        result = step.invoke(value, context)
        if type(result) is CoroutineType:
            result = await result
        # Even an immediate return gives queued cancellation a turn before
        # the shell accepts the result or invokes the next handler.
        await asyncio.sleep(0)
        return _Returned(result)
    except asyncio.CancelledError as error:
        return _Raised(error)
    except Exception as error:
        if task is not None and task.cancelling() > pending:
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError as cancellation:
                return _Raised(cancellation)
        return _Raised(error)
    finally:
        # Cancellation requested on the private task belongs to this handler.
        # Once classified or handled, its count must not affect later hooks.
        _restore_cancellation_count(task, pending)


class _Invocation:
    def __init__(self, projection: Projection, observer: _Observer | None) -> None:
        self.projection = projection
        self.observer = observer
        self.context: tuple[str, str] | None = None
        self.terminal: Any = None

    def _context_snapshot(self) -> Any:
        if self.context is not None:
            return self.projection.carriers.invocation_context(*self.context)

    def _emit(self, kind: str, **fields: Any) -> None:
        if self.observer is not None:
            _observe(self.observer, {
                "kind": kind, "pipelineId": self.projection.pipeline_id,
                "context": self._context_snapshot(), **fields,
            })

    def finish(self, terminal: Any, final_return: dict[str, Any] | None = None) -> Any:
        if self.terminal is None:
            self.terminal = terminal
            if final_return is not None:
                self._emit("RETURNED", **final_return)
            if self.observer is not None:
                self._emit("TERMINAL", record=deepcopy(terminal))
        return self.terminal

    def cancel(self) -> None:
        self.finish(self.projection.carriers.cancelled())

    def subscribe(self, signal: _CancellationSignal) -> Callable[[], None]:
        admitted = False
        requested = False

        def request() -> None:
            nonlocal requested
            requested = True
            if admitted:
                self.cancel()

        try:
            cancelled = signal.cancelled
            subscribe = signal.subscribe
            if type(cancelled) is not bool or not callable(subscribe) or _is_async_callable(subscribe):
                raise TypeError("invalid cancellation signal")
            unsubscribe: object = subscribe(request)
            if type(unsubscribe) is CoroutineType:
                _close_unowned_coroutine(unsubscribe)
            if not callable(unsubscribe) or _is_async_callable(unsubscribe):
                raise TypeError("invalid cancellation subscription")
        except (Exception, asyncio.CancelledError) as error:
            raise PythonLocalDiagnostic("INVALID_SIGNAL", "expected a synchronous cancellation signal") from error
        admitted = True
        if cancelled or requested:
            self.cancel()
        return unsubscribe

    async def execute(self, number: int) -> Any:
        carriers = self.projection.carriers
        try:
            for index, step in enumerate(self.projection.steps, 1):
                for attempt in range(1, step.retry_limit + 2):
                    if self.terminal is not None:
                        return self.terminal
                    event = {"stepId": step.id, "inputType": step.input_id, "outputType": step.output_id,
                             "attempt": attempt}
                    self._emit("ATTEMPT_STARTED", **event)
                    if self.terminal is not None:
                        return self.terminal
                    result = await _invoke(step, step.input_type(number), self._context_snapshot())
                    if self.terminal is not None:
                        return self.terminal
                    if isinstance(result, _Raised):
                        failure = _bind_failure(result.error, step.id, carriers)
                        if not failure.retryable or attempt > step.retry_limit:
                            return self.finish(carriers.failed(failure))
                        self._emit("RETRY_SCHEDULED", **event)
                        continue
                    number = _bind_result(result.value, carriers, step.output_type)
                    if index == len(self.projection.steps):
                        return self.finish(carriers.completed(carriers.value(step.output_type(number))), event)
                    self._emit("RETURNED", **event)
                    break
        except PythonLocalDiagnostic as error:
            return self.finish(carriers.rejected(error.code))
        raise AssertionError("admitted projection has no final step")


@dataclass(frozen=True, slots=True)
class _Application:
    projection: Projection
    observer: _Observer | None

    async def run(self, input: object, context: object, signal: _CancellationSignal | None = None) -> Any:
        # The generated protocol supplies the precise nominal terminal type for
        # each application. The runtime only knows the admitted carrier classes.
        invocation = _Invocation(self.projection, self.observer)
        carriers = self.projection.carriers
        unsubscribe = None
        try:
            number = _bind_value(input, self.projection.steps[0].input_type)
            invocation.context = _bind_context(context, carriers.invocation_context)
            if signal is not None:
                unsubscribe = invocation.subscribe(signal)
            if invocation.terminal is not None:
                return invocation.terminal
            task = asyncio.create_task(invocation.execute(number), eager_start=False)
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                # Caller detachment requests cancellation but cannot interrupt
                # started business code. Repeated Task.cancel calls still drain.
                invocation.cancel()
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        continue
                    except Exception:
                        break
                _consume_future(task)
                raise
        except PythonLocalDiagnostic as error:
            return invocation.finish(carriers.rejected(error.code))
        finally:
            if unsubscribe is not None:
                try:
                    unsubscribe()
                except (Exception, asyncio.CancelledError):
                    pass


@dataclass(frozen=True, slots=True)
class _Runtime:
    observer: _Observer | None

    def load(self, binding: object, /) -> _Application:
        return _Application(admit(binding), self.observer)


def create_python_local_runtime(*, on_observation: _Observer | None = None, **options: object) -> _Runtime:
    """Create the runtime for the compiler-generated create_application entry point."""
    if options or (on_observation is not None and (
        not callable(on_observation) or _is_async_callable(on_observation)
    )):
        raise PythonLocalDiagnostic("UNSUPPORTED_RUNTIME_OPTIONS", "only synchronous on_observation is supported")
    return _Runtime(on_observation)
