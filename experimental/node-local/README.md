# Experimental Node LOCAL runtime

This private ESM package runs the compiler's `compiler-node-local` projection for
`tpf-polyglot-unary-policy/1`. It executes three ordered `ONE_TO_ONE` steps in a single
Node process using `LOCAL` transport and `COMPUTE` with direct execution. The compiler
owns the flow, nominal boundary types, handler names, and per-step retry budgets. Once
generation and dependencies are available, running and debugging the application
requires only Node.js 22 or later. This package is for experimental evaluation.
An explicit continue, redesign, or retire decision governs any expansion or
production support.

## Opt in

Install the tarball from the `node-local-runtime-<source SHA>` CI artifact in a native
application that already has the matching compiler-generated `application.ts` and
`models.ts`. Verify the tarball against the accompanying `SHA256SUMS` first. From the
application directory, install the local tarball:

```sh
npm install --ignore-scripts /path/to/tpf-experimental-node-local-0.0.0.tgz
```

Pass the runtime and the application's typed registry to the generated entrypoint.
The registry must contain precisely the handlers declared in the generated binding.

```ts
import { createApplication } from './generated/application.js';
import { createNodeLocalRuntime } from '@tpf/experimental-node-local';
import { handlers } from './handlers.js';

const runtime = createNodeLocalRuntime({
  onObservation: event => console.log(event),
});
const application = createApplication(runtime, handlers);
const terminal = await application.run({value: 3}, {
  executionId: 'execution-1',
  traceId: 'trace-1',
});
```

Loading checks the generated projection string against its embedded SHA-256 before
validating the schema, profile, capabilities, nominal boundaries, and policy. An
incompatible binding throws `NodeLocalDiagnostic` with a `code` and, where available,
a corrective detail. Retrieve matching generated outputs before retrying an
integrity error. The projection digest checks the bytes passed to the loader.
Source/build provenance and final Release identity belong to their respective
generation and Release workflows.

## Load against verified Compiled Truth

Ordinary LOCAL use can omit `expectedProjectionJson`. For Release-bound use,
the deployment host verifies the runnable, its dependencies and the Release's
Compiled Truth before loading the generated application. The host supplies the
exact projection from that verified carrier. Decode its bytes as UTF-8 with
`new TextDecoder('utf-8', {fatal: true, ignoreBOM: true})` and pass the resulting
string to `createNodeLocalRuntime({expectedProjectionJson})`. Preserve whitespace
and use the Compiled Truth carrier as the source of the expected projection.

The runtime captures the text at construction and compares it with the generated
binding during load, before inspecting the handler registry. A different projection
raises `TRUSTED_PROJECTION_MISMATCH` even when its embedded digest matches.
Matching bindings still undergo all ordinary LOCAL admission checks. Release
verification and deployment isolation are host responsibilities. Same-process
handlers run with the process's privileges.

## Execution

- Each handler receives a frozen input record and a frozen context snapshot with
  exactly `executionId` and `traceId`. The runtime accepts
  `{kind: 'VALUE', value: {value: number}}` from the handler directly or through a
  native Promise. Each value is a signed 32-bit integer in the qualified native
  carrier, which excludes negative zero. The four compiler-owned boundary IDs
  retain their nominal identity even though the records have the same shape.
- A handler can throw or reject with `new StructuredFailure(envelope)`. An envelope
  includes `code`, `message`, `owner: 'LOCAL_SHELL'`, `stepId`, and `retryable`.
  Only a validated `TRANSIENT` failure can spend the current step's budget (zero
  to two retries). Each retry sees the same admitted input. `BUSINESS_REJECTION`
  is terminal, and ordinary throws are sanitised as `UNCLASSIFIED`. Invalid
  structured failures or value carriers produce `REJECTED` with a diagnostic.
- The optional native `AbortSignal` commits `CANCELLED` if it wins terminal
  acceptance. The terminal observation is synchronous. If a handler has already
  started, `run` waits for it to settle and discards that late result or failure.
  The `run` Promise returns the terminal record, including `CANCELLED` without
  a value or failure. A previously committed terminal record keeps its result.
- The optional synchronous `onObservation` callback sees `ATTEMPT_STARTED`,
  `RETURNED`, and `RETRY_SCHEDULED` with one-based attempt numbers, step and type
  identities, and invocation context. The shell emits one `TERMINAL` observation
  containing the terminal record. Abort from an intermediate `RETURNED` or
  `RETRY_SCHEDULED` observer prevents dispatch of the subsequent work. The final
  validated value is committed before the final `RETURNED` observation. Observer
  throws and Promise rejections have no settlement authority. Keep observers
  short because synchronous callbacks run on the execution path.

The bounded profile excludes streaming, Connectors, durable Await, providers,
plugins, blocking-work hints, and remote or distributed execution. The loader
rejects incompatible artifacts and the factory rejects unsupported options.

## Experimental worker payload binding

The package exports `PolyglotInt32PayloadBinding`. Construct it with the four distinct
logical type IDs from a verified compiler projection. `encode(typeId, value)`
creates a serialized payload carrier. `decode(expectedTypeId, envelope)` checks
the exact logical type and inner encoding and returns the signed int32. The
caller can then construct the generated model for that boundary.

```javascript
import { PolyglotInt32PayloadBinding } from '@tpf/experimental-node-local';

const binding = new PolyglotInt32PayloadBinding('InputValue', 'FirstValue', 'SecondValue', 'OutputValue');
const envelope = binding.encode('InputValue', 7);
const value = binding.decode('InputValue', envelope);
```

`tpf-polyglot-int32-json/1` encodes only the application value as a JSON object
with a single required integer field, `value`. The REST worker command and result
use `application/tpf-transition-envelope+json` as their outer encoding. A future
worker host must select the inner encoding and expected type from its trusted,
verified Release binding and pin them to the dispatch. This helper is available
for future host integration and conformance tests.

## Verify or remove

From the runtime repository root:

```sh
npm --prefix experimental/node-local test
```

For an application compiled to JavaScript with source maps, run its entrypoint with
`node --enable-source-maps --inspect-brk dist/main.js` and set breakpoints in the
authored handlers. Use the application's own test and TypeScript build commands to
check its generated registry.

The module and its path-filtered CI workflow are the removal boundary. Delete
`experimental/node-local/` and `.github/workflows/experimental-node-local.yml`
and remove the opt-in dependency from consuming applications. The package and
workflow sit outside the Java reactor and its publication paths. The package is
private and the workflow creates downloadable test artifacts only.
