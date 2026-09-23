# The Pipeline Framework Runtime

This repository publishes TPF's runtime integrations:

- the Quarkus `pipelineframework` runtime and `pipelineframework-deployment` build-time pair;
- the Spring runtime adapter;
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
[TPF Components and Repositories](https://pipelineframework.org/architecture/components-and-repositories) page.

## System-test candidates

`TPF Candidate Build` runs at the exact pull-request or `main` SHA with read-only permissions and no secrets. It
builds a commit-specific runtime candidate and uploads only allowlisted Maven files and preliminary metadata. The
trusted `TPF Candidate Publish` workflow validates the build and current head, publishes those files to this
repository's GitHub Packages registry, then dispatches `tpf-candidate-v1` to the coordination repository without
executing project or fork code.

Fork pull requests require `safe-to-system-test`. Candidate publication uses the coordination GitHub App and the
workflow package token; it uses no Maven Central credentials or GPG key.
