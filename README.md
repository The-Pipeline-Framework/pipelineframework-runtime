# The Pipeline Framework Runtime

This repository publishes TPF's runtime integrations:

- the Quarkus `pipelineframework` runtime and `pipelineframework-deployment` build-time pair;
- the Spring runtime adapter;
- the release descriptor Maven plugin;
- foundational persistence, cache, and repository plugins.

Spring smoke-test modules stay here because they verify behavior owned by the Spring adapter; they are internal test
modules rather than ecosystem examples.

The Quarkus deployment artifact consumes the released compiler and runs during application builds. The Quarkus
runtime artifact is packaged with applications. This duality is an integration mechanism: compiler semantics and
shared runtime contracts remain owned by their standalone repositories.

Build with an isolated Maven repository:

```sh
./mvnw clean verify -Dmaven.repo.local="$PWD/.m2/repository"
```

Use the `central-publishing` profile only to sign and deploy the canonical reactor. For the component map and
compatibility policy, see the
[TPF Components and Repositories](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/docs/architecture/components-and-repositories.md) page.

## System-test candidates

`TPF Candidate Build` runs at the exact pull-request or `main` SHA with read-only permissions and no secrets. It
builds a commit-specific runtime candidate and uploads only allowlisted Maven files and preliminary metadata. The
trusted `TPF Candidate Publish` workflow validates the build and current head, publishes those files to this
repository's GitHub Packages registry, then dispatches `tpf-candidate-v1` to the coordination repository without
executing project or fork code.

Fork pull requests require `safe-to-system-test`. Candidate publication uses the coordination GitHub App and the
workflow package token; it uses no Maven Central credentials or GPG key.

For a missing `main` candidate, check the candidate build and publisher against the exact source SHA. The
publisher's workflow-run page can display the default-branch SHA even when its candidate event identifies a pull
request. Check the published event's `source_sha` and `pull_request_number` before retrying downstream verification.

## Coordinator Maven Release admission

The existing private, default-disabled release admin registration endpoint accepts canonical `maven:`
application-archive Releases through the public `pipelineframework-release-resolver`. Admission verifies the closure
before storage, preserves the descriptor and retains native immutable registration and primary-artifact
activation verification. Legacy local file/JAR registration remains supported; admission does not deploy
or execute customer code.

See [Coordinator admission configuration](https://pipelineframework.org/deploy/release-descriptors#register-with-a-self-hosted-coordinator)
for environment-owned Maven access and current OCI/settings/native-image limits. Secondary-artifact
activation verification and garbage collection are unchanged.

## Experimental worker payload binding

Construct `PolyglotInt32PayloadBinding` with the four distinct logical type IDs
from the verified compiler projection. Use `encode(typeId, value)` to create a
`SerializedTransitionPayload`. Use `decode(expectedTypeId, envelope)` to check
the type and inner encoding and recover the int32 for the generated model.

The application value uses `tpf-polyglot-int32-json/1`. The REST worker command
and result use the outer `application/tpf-transition-envelope+json` encoding.
The deployment host owns Release verification, authorization and selection of
the expected type and inner encoding for the dispatch. This helper is available
for future host integration and conformance tests.

## Experimental native worker Release binding

The explicit `RestPipelineTransitionWorker.executeTransition(Attempt)` overload
uses a prepared binding's HTTPS endpoint, credential reference and result profile.
It signs the exact request with the existing worker HMAC protocol. Its direct
HTTP/1.1 client disables proxies and redirects; the request body refuses a second
subscription, preventing the HTTP implementation from replaying the signed body.
Connection establishment is not an exactly-once execution guarantee. Responses
are bounded to 4 KiB and checked against the retained output type or portable
failure profile. A lost or invalid active response becomes
`REMOTE_OUTCOME_UNKNOWN`, without a fallback decoder or another invocation.

The existing runtime result model retains the attempt and portable failure code.
For this bounded pairing, all three portable failures are terminal: local retry
exhaustion does not grant a new coordinator retry budget. Existing Java failure
handling is unchanged. `PairedTransitionSettlementStore` is a runtime-local
capability implemented by the existing in-memory and Dynamo execution stores,
not another store or ledger. It atomically checks RUNNING status, version, owner
and lease expiry when recording the result. The bounded serialized output is
stored in the same update; stale replies cannot overwrite it before a failed
conditional write. Ledger and DLQ effects follow a successful state update.
Paired native completion requires checkpoint and object output publication to
be disabled. With either configured, the coordinator records a non-retryable
`PAIRED_TERMINAL_PUBLICATION_UNSUPPORTED` failure under the same live lease check.

This overload is not selected by registration or ordinary dispatch. Its caller
must own durable dispatch intent and current authority before sending. Those
activation prerequisites, native HTTP hosts, hosted admission and restart-after-send
recovery remain unqualified. Unit HTTP fixtures and persistent-store tests are
not deployed Node/Python worker evidence. Pure LOCAL loading remains JVM-free.

The deployment supplies a `NativeWorkerReleaseBinding.Target` with the scoped
tenant and worker, HTTPS origin, secret reference, native language, generated
coordinator Contract, and exact Release Descriptor and runnable SHA-256 pins.
It selects the outer envelope, inner payload, execution, and failed-result
profiles explicitly. The four accepted selections are
`application/tpf-transition-envelope+json`, `tpf-polyglot-int32-json/1`,
`tpf-polyglot-unary-policy/1`, and `tpf-polyglot-failed-result/1`, respectively.

`prepare` verifies the descriptor bytes before opening artifacts, then uses
the existing Release resolver to verify the closure and recover Compiled Truth.
The native Contract must equal the coordinator's generated Contract. The
selected `application-archive` must cover all three Steps and both `local` and
`rest` capabilities. The descriptor-named Compiled Truth supplies the exact
projection bytes, which are limited to 1 MiB. The runnable can carry these
resources itself.

Prepare one application archive per language with its generated application
and models, authored handlers, and the complete unpacked native runtime package.

| Target | Required runnable files |
| --- | --- |
| Node | `package.json`, `app/application.js`, `app/models.js`, `app/business.js`, and `package.json`, `index.mjs`, `payload-binding.mjs`, `projection.mjs`, and `records.mjs` in `node_modules/@tpf/experimental-node-local/` |
| Python | `app/__init__.py`, `app/business.py`, `app/generated/project/application.py`, `app/generated/project/models.py`, and `__init__.py`, `_admission.py`, and `_payload_binding.py` in `tpf_experimental_python_local/` |

The experimental archive accepts up to 512 files and 32 MiB of expanded bytes.
Additional files belong under `app/`, the target's runtime package, or
`META-INF/pipeline/`. Preparation materializes archive members as regular
files into a fresh directory in the supplied deployment workspace.

Call `verifyPreparedTree` immediately before controlled launch. Load the
application, handlers, and runtime from the returned tree and pass the exact
`projectionBytes()` to the native runtime's verified projection binding. The
deployment supplies the trusted interpreter and standard library, controls
imports, and keeps the prepared files immutable throughout their use.

`pin(command)` checks scoped identity, the whole-bundle `0/-1` SINGLE range,
explicit execution context, and the expected input type and encoding before
payload decoding. The resulting attempt retains the exact command and the
trusted target and profile selection. Use `requireCommand` to compare a
received command with that retained expectation. Authentication, lease
authority, and settlement remain with the host and coordinator.

## Unit coverage

The normal Maven verify lifecycle writes a JaCoCo HTML and XML report for each
module's Surefire unit tests under `target/site/jacoco/`. The reports use a
separate `jacoco-unit.exec` data file and do not include Failsafe integration
tests or the Quarkus coverage data those tests may produce. Failsafe remains the
integration and end-to-end evidence lane.

Generated MapStruct mapper implementations and Quarkus `_Bean` classes are
excluded from the reports because they are generated framework plumbing rather
than maintained source. The `spring-smoke-tests` and
`spring-blocking-smoke-tests` modules are also excluded: they exercise
generated application behavior as integration smoke lanes and do not measure
coverage of the runtime's maintained source.

CI uploads the per-module HTML/XML reports and lists line and branch coverage in
the workflow summary. Coverage is currently reported for trend and review; no
percentage threshold is enforced until owner-repository baselines support one.
