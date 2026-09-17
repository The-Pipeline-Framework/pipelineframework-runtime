# Runtime Repository Boundary

This repository owns the published Quarkus runtime/deployment pair, the Spring
runtime adapter, and the three foundational plugins. It consumes
framework-neutral contracts and compiler artifacts as released dependencies.
Connectors, representation providers, hosts, Blocks, transport completeness
tests, and application examples remain outside this repository. Spring smoke
tests and build-only aggregators remain internal and non-deployable.

Snapshot publication is activated only through the manually dispatched,
handoff-gated workflow in `.github/workflows/publish-snapshot.yml`. The workflow
must verify that the monorepo has stopped publishing the six runtime-owned
artifacts before any deploy can proceed.

Use the repository-local Maven cache on every invocation:

```text
./mvnw verify -Dmaven.repo.local="$PWD/.m2/repository"
```
