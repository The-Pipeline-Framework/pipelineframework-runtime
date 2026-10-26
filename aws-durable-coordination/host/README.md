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

### Queue-only native execution

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
