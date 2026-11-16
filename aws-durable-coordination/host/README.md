# AWS Durable coordination host

This module owns AWS Durable host handlers and deployment configuration, including
startup resolution of pinned signing-key versions. It consumes framework-neutral
contracts; application pipeline semantics remain with their existing owners.

The existing signing source is Secrets Manager. The explicit `SSM_STANDARD`
reader is an experimental source candidate, not a supported deployment profile;
the published hardened SAM template and v1 deployment manifest use Secrets Manager.
A [new unpublished restricted profile qualification](../deployment/profiles/README.md)
adds conditional deployment/IAM/schema2 support without changing the host binary;
it does not establish live AWS conformance or zero fixed cost.

The canonical [AWS Durable deployment contract](https://pipelineframework.org/deploy/orchestrator-runtime/aws-durable#experimental-pinned-ssm-signing-source)
owns configuration details, retained-version requirements and support limits.
Source and regression coverage live in `AwsDurableDeploymentConfigSourceFactory`
and `AwsDurableDeploymentConfigSourceFactoryTest` in this module.

Build from the repository root with its isolated Maven repository:

```sh
./mvnw clean verify -Dmaven.repo.local="$PWD/.m2/repository"
```

Publication and immutable candidate validation follow the repository's normal
runtime workflow. This module does not provision parameters or attest their tier,
permissions, retention, deployment provenance or real-AWS conformance.

## Hosted Release verification

The coordination model exposes the packaged function roles and artifact groups
for the queue and in-process Hosted profiles. Consumers use these facts to
validate a Release against its compiled contract and numbered functions.

The existing private Lambda action gateway accepts `READ_RUNTIME_IDENTITY` with
the tenant and exact pipeline, contract and Release pins. After successful
strict startup it reports the loaded compiled contract and the selected
Command and Query stores. A missing registration, foreign pin or unhealthy
runtime fails the read. The action uses the gateway's existing Lambda invoke
permissions.

## Queue-only native execution

The native deployment sets `pipeline.orchestrator.sqs.local-loopback=false` (the
`sqs.local-loopback` entry in `TPF_AWS_DURABLE_CONFIGURATION`) on all eight semantic
hosts. Validated startup rejects a missing or enabled value before resolving signing
keys, including an unsafe override at or above the pinned configuration's priority.
Update the deployment configuration together with the host artifact.

`process-loops-disabled=true` does not disable the SQS dispatcher's local CDI event.
Leaving local loopback enabled would both enqueue the work and start asynchronous
execution inside the submitting Lambda, competing with the native Work consumer
and allowing work to outlive the Lambda invocation. Native execution belongs to
the SQS handler, which awaits processing before acknowledging the message. The
ordinary runtime's local-development default is unchanged.

## Hosted execution evidence

The private Lambda action gateway reads the exact tenant, execution, Release
and driver generation. `READ_EXECUTION_LOCATOR` returns the retained
`AwsDurableRecoveryLocator`, or JSON null when that generation has no current
locator. An execution can have a locator without registering an Await callback.
`TYPED_RESULT` requires one successful terminal item and a current locator.
It returns a `SerializedTransitionPayload` through the existing transition
codec. The existing raw `RESULT` action still serves native driver replay.

## Standalone transition Lambda

Select `aws-durable-transition-worker` with `QUARKUS_LAMBDA_HANDLER` and opt in
with `tpf.aws.durable.transition-worker-only=true`. Configure
`pipeline.orchestrator.mode=SYNC` and
`pipeline.orchestrator.process-loops-disabled=true`, the compiled pipeline ID
and release version, the SQS Region and response queue URL, and
`pipeline.orchestrator.worker.sqs.shared-secret-ref` with a worker-local reference.
The packaged contract supplies the contract version. Configure the normal
payload allowlist for application classes when needed.

A Lambda event source mapping supplies requests. Use
`ReportBatchItemFailures` so the mapping retries failed records. The worker
runs the existing signed transition action and publishes its result to the
fixed response queue. The coordinator handles execution claims and commits.
A plain typed worker needs queue access and its signing material. Keep
coordinator settings, resume signing keys, and retained Lambda forwarding
configuration on the coordinator side. Providers used by application steps
retain their own resource and permission requirements.
