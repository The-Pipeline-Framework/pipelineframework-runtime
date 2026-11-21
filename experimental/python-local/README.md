# Experimental Python LOCAL runtime

This opt-in package executes the compiler's `compiler-python-local` projection
for `tpf-polyglot-unary-policy/1`. It runs three ordered unary business steps
using LOCAL transport and COMPUTE with direct execution. The compiler owns the
order, nominal boundary types, handler names and per-step retry budgets.

Install a wheel from the runtime candidate's CI artifact after verifying its
SHA-256 against the accompanying manifest. The runtime uses the Python standard
library. The qualification target is CPython 3.14.10 on Linux.

```sh
python -m pip install --no-deps --force-reinstall /path/to/tpf_experimental_python_local-0.0.0-py3-none-any.whl
```

Use the generated entrypoint and the application's typed handler registry.
This example assumes the generated input class is named `Input`.

```python
from generated.application import InvocationContext, create_application
from generated.models import Input
from tpf_experimental_python_local import CancellationToken, create_python_local_runtime
from my_application.handlers import handlers

runtime = create_python_local_runtime(on_observation=print)
app = create_application(runtime, handlers)
token = CancellationToken()

async def run():
    return await app.run(Input(3), InvocationContext("execution-1", "trace-1"), token)
```

Loading checks the projection's embedded SHA-256 and validates its closed
profile, capabilities, nominal types, policy and handler registry before
execution. Incompatible bindings raise `PythonLocalDiagnostic` with a
`code`. Retrieve matching generated files if the digest differs. This digest
checks projection bytes. Build provenance and Release identity are owned by
their respective workflows.

## Execution

- A handler receives a fresh instance of the generated frozen input class
  and a frozen snapshot of the explicit `InvocationContext` with
  `executionId` and `traceId`. Use that argument as the invocation context
  in synchronous and coroutine handlers. Asyncio propagates ordinary Python
  context variables into the execution task.
- A handler returns the generated `Value` wrapping the exact generated
  output class, directly or through a native coroutine. Each boundary record
  contains a signed 32-bit integer. The runtime validates the exact Python
  `int` type and nominal class without coercion.
- To report a structured failure, raise `StructuredFailure(envelope)` with
  a dictionary containing `code`, `message`, `owner` (`LOCAL_SHELL`),
  `stepId` and `retryable`. Only a valid `TRANSIENT` failure spends the
  current step's retry budget. Each retry gets a fresh record of the same
  admitted input. `BUSINESS_REJECTION` is terminal. Other exceptions,
  including a handler's `asyncio.CancelledError`, map to a sanitised,
  non-retryable `UNCLASSIFIED` failure. Invalid value or structured failure
  carriers produce a `REJECTED` terminal record.
- `CancellationToken.cancel()` requests cancellation inline. The first
  accepted terminal record wins. The runtime drains started handlers and
  discards their late results or failures after cancellation. An attached
  caller receives `CANCELLED` when cancellation wins. If a caller cancels
  its asyncio task, the runtime requests cancellation, drains started work,
  and then re-raises `asyncio.CancelledError` to that caller.
- Synchronous `on_observation` hooks receive immutable mappings.
  `ATTEMPT_STARTED`, `RETURNED`, and `RETRY_SCHEDULED` include the
  pipeline, step, and boundary identities, the one-based attempt, and the
  invocation context. A single `TERMINAL` observation includes the
  terminal record. An intermediate observer can cancel the token before
  the next attempt. The final validated result is committed before its
  `RETURNED` observation. Hooks' exceptions and return values have no
  authority over settlement. Keep hooks short because delivery runs inline.

Keep each application's invocations, signals and observers on one event-loop
thread. Multiple invocations can overlap on that loop. Synchronous handlers
run on the event-loop thread. There is no threaded scheduling, external I/O
or durable execution in this profile.

## Experimental worker payload binding

The package exports `PolyglotInt32PayloadBinding`. Construct it with the four distinct
logical type IDs from a verified compiler projection. `encode(type_id, value)`
creates a serialized payload carrier. `decode(expected_type_id, envelope)` checks
the exact logical type and inner encoding and returns the signed int32. The
caller can then construct the generated model for that boundary.

```python
from tpf_experimental_python_local import PolyglotInt32PayloadBinding

binding = PolyglotInt32PayloadBinding("InputValue", "FirstValue", "SecondValue", "OutputValue")
envelope = binding.encode("InputValue", 7)
value = binding.decode("InputValue", envelope)
```

`tpf-polyglot-int32-json/1` encodes only the application value as a JSON object
with a single required integer field, `value`. The REST worker command and result
use `application/tpf-transition-envelope+json` as their outer encoding. A future
worker host must select the inner encoding and expected type from its trusted,
verified Release binding and pin them to the dispatch. This helper is available
for future host integration and conformance tests.

## Verify or remove

From this directory, build a local wheel, install it in a fresh environment
and run the tests against that installation.

```sh
python -m venv /path/to/venv
/path/to/venv/bin/python -m pip wheel --no-deps --wheel-dir /path/to/wheels .
/path/to/venv/bin/python -m pip install --no-deps --force-reinstall /path/to/wheels/tpf_experimental_python_local-0.0.0-py3-none-any.whl
/path/to/venv/bin/python -W error -m unittest discover -s tests -v
```

To remove the experiment, delete `experimental/python-local/` and
`.github/workflows/experimental-python-local.yml` and remove the opt-in
dependency from consuming applications. The package sits outside the Java
reactor and publication workflows. CI builds downloadable wheel candidates
for evaluation without publishing them to PyPI. An explicit decision to
continue, redesign, or retire the experiment governs any expansion.
