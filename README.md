# Pipeline Framework Runtime

This repository is the standalone runtime distribution for The Pipeline Framework.
It contains the Quarkus `pipelineframework` extension and its
`pipelineframework-deployment` pair, the Spring runtime adapter, the three
foundational plugins, and internal Spring smoke-test modules.

The framework-neutral contracts are consumed at the released version declared by
`tpf.contracts.version`; compiler artifacts used only by tests are declared by
`tpf.compiler.version`. This repository is currently staging-only: all artifacts
have deployment disabled until publisher handoff.

Build locally with:

```text
./mvnw clean verify -Dmaven.repo.local="$PWD/.m2/repository"
```

