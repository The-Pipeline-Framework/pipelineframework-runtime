# Release production and package association

The default producer still emits schema 1. Existing six-argument
`ReleaseProductionRequest` calls and schema-1 JSON remain supported. Supplying a
`PipelineReleaseDeployment` emits schema 2 in the **same immutable Release**;
there is no additional topology file.

Schema 2 carries packaging facts:

- `profile`: the requested versioned provider capability, not permission or proof
  that a provider supports it.
- `entries`: existing artifact ID, compiled module identity (`runtimeId`), Java
  entry class, trigger, and logical binding references. `packagePath` is `.` or
  the exact nested application JAR containing the complete compiler resource set.
  `entryPackagePath` defaults to `packagePath`; it may identify a shared framework
  handler JAR in the same artifact. Multiple entries do not duplicate semantic
  step ownership.
- `bindings`: logical IDs and required capabilities. Account IDs, resource ARNs,
  credentials, grants and policy remain environment/deployment concerns.
- `check`: an entry, bounded timeout, input and expected result. Both values use
  the existing `SerializedTransitionPayload` wire fields (`payloadTypeId`,
  `payloadEncoding`, `payload`). This declares a check; producing a Release does
  not execute it or establish runtime readiness.

The Maven plugin accepts these facts under its existing configuration's
`<deployment>` element through `ReleaseDeploymentConfiguration`. The independent producer tests cover two different pipeline shapes. Their
`TEST` trigger and `java-archive-test-v1` profile are packaging-test fixtures.
AWS host qualification is owned by the runtime host integration suite.

Producer and independent closure consumer both compare the complete packaged
`META-INF/pipeline` resource set with the original Compiled Truth, and verify the
selected entry class is present at the declared package location. The artifact
hash still covers the complete outer package. This is package association, not
code attestation: provider delivery must additionally observe the actual running
version, code hash, compiled runtime identity and declared execution result.

Current association verification supports JAR, application archive and Lambda
ZIP. Schema-2 native/image entries reject structurally before artifact resolution
until their owners provide a corresponding verifier. The current compiled-module
contract requires each entry's `runtimeId` to equal `PipelineContractDescriptor.module`;
this is not a second runtime-layout model. Compiler resources are bounded to 8 MiB
and 4096 resources; nested Java packages are bounded to 64 MiB expanded. A provider
must reject unsupported profiles/triggers or unauthorized/incomplete bindings
before provisioning, registration or delivery effects.

The existing AWS host's fresh/drained profile is **not** automatically qualified
for retained A/B updates by these changes. Retained physical bindings, routing,
IAM, concurrency capacity, activation checks and live conformance remain separate
requirements. No producer output authorizes infrastructure operations.
