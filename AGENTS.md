# Runtime Repository Instructions

This repository owns the Quarkus runtime/deployment pair, Spring runtime adapter, foundational plugins, and their
integration tests. It consumes compiler and framework-neutral contracts as released dependencies.

## Boundary

- Quarkus runtime/deployment modules may change atomically here, but the deployment module adapts compiler output;
  it does not own Pipeline language semantics.
- Keep shared model, serialization, runtime protocol, and provider SPI in `pipelineframework-contracts`.
- Keep compiler phases and source renderers in `pipelineframework-compiler`.
- Keep Connectors, representation-provider implementations, Blocks, Expansions, examples, and applications outside
  this repository.
- Keep Spring smoke tests here and internal. Do not place them in the ecosystem catalogue.
- Preserve equivalent cardinality, failure, lineage, replay, and generated-boundary behavior across supported
  runtime adapters unless a difference is explicit and tested.

## Cross-repository changes

Update canonical documentation or an ADR in `pipelineframework` when runtime work changes a public contract or
semantic owner. Use the GitNexus `tpf` group for cross-repository impact and verify findings in the owning worktree.
Do not add source fallbacks for unpublished component changes; publish the dependency and test the released seam.

## Build and publication

Owner-local verification is the first gate. `.github/tpf-system-tests.json` owns the stable runtime deployment suite
command. `TPF Candidate Build` and the trusted publisher create an immutable, commit-specific runtime candidate;
`tpf/system-tests` records downstream evidence on that exact source SHA.

For an ordinary single-repository pull request, use the candidate publisher and singleton system-test path above.
For a coordinated change, give every participating pull request the same head-branch name under the same GitHub
owner. Candidate intake discovers those open component pull requests and dispatches one deterministic compatibility
set automatically; later intake events for the same set cancel the older in-flight run. Do **not** wait for
participating candidate publishers and do not merge or publish snapshots one repository at a time.

Use the manual
[`TPF System Tests — Compatibility Set`](https://github.com/The-Pipeline-Framework/pipelineframework/actions/workflows/system-test-compatibility-set.yml)
entry point only when a coordinated set intentionally uses different branch names. Supply one stable set ID and
2–10 pull-request URLs, one per line. Both paths pin each exact PR head and current base, build the participating
Maven reactors and derived downstream closure in dependency order into one isolated repository, and report one
`tpf/system-tests` result to every participating SHA. Do not manually add unchanged downstream repositories or
publish intermediate snapshots; the coordinator derives current-main and downstream closure targets. Do not
substitute snapshots, floating branch heads, source checkouts or a composite Maven reactor. See the canonical
[cross-repository system-test runbook](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/docs/evolve/cross-repository-system-tests.md).

Repository setup requires repository-scoped dispatch credentials. If the workflow exposes them as
`SYSTEM_TEST_APP_ID` and `SYSTEM_TEST_APP_PRIVATE_KEY`, they must belong to a dispatch-only App installed solely on
`pipelineframework`, never the coordinator App. The trusted publisher uses the repository `GITHUB_TOKEN` with
`packages: write`; fork publication additionally requires the
`safe-to-system-test` label. Never expose publication, dispatch or status credentials to owner-suite jobs.

Require a green full train for formal BOM or release promotion.

Always use the repository-local Maven cache:

```sh
./mvnw <goals> -Dmaven.repo.local="$PWD/.m2/repository"
```

Do not introduce Maven profiles except `central-publishing`. It may attach, sign, and deploy artifacts but must not
select another source universe, module graph, or build topology.

Do not commit, push, publish, or change another repository unless explicitly requested.
