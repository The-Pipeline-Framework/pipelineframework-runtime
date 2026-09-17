# Runtime Repository Boundary

This repository owns the published Quarkus runtime/deployment pair, the Spring
runtime adapter, the three foundational plugins, and their Spring smoke tests.
It consumes framework-neutral contracts and compiler artifacts as released
dependencies. Connectors, representation providers, hosts, Blocks, transport
completeness tests, and application examples remain outside this repository.

This is staging-only until publisher handoff. Keep every artifact non-deployable
and do not add publishing workflows or credentials here.

Use the repository-local Maven cache on every invocation:

```text
./mvnw verify -Dmaven.repo.local="$PWD/.m2/repository"
```

