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
