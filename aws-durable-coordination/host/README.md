# AWS Durable coordination host

This module owns AWS Durable host handlers and deployment configuration, including
startup resolution of pinned signing-key versions. It consumes framework-neutral
contracts; application pipeline semantics remain with their existing owners.

The existing signing source is Secrets Manager. The explicit `SSM_STANDARD`
reader is an experimental source candidate, not a supported deployment profile;
the hardened SAM template and v1 deployment manifest still use Secrets Manager.

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
