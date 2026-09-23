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
