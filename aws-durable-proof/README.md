# Deployed AWS Durable coordinator proof

This non-published reactor proves the candidate AWS production boundary from
[pipelineframework#976](https://github.com/The-Pipeline-Framework/pipelineframework/issues/976):

- AWS Lambda Durable Functions owns orchestration liveness, checkpoints, suspension, callback wake-up, and mechanical replay/retry timing.
- TPF owns execution and Await identity, typed completion admission, parent release, release pinning, signed worker transitions, retry/DLQ evidence, results, and re-drive decisions.
- SQS remains the worker boundary for backpressure, redelivery, DLQ evidence, and uncertain remote outcomes.
- The native single-shot coordinator remains the portable semantic reference.

The proof is deliberately isolated from production packaging. `shared` contains proof messages, `durable-driver` is the plain Java durable Lambda, `action-host` is the Quarkus TPF action/worker host, and `fault-tests` is an opt-in real-AWS gate. A normal Maven build never deploys cloud resources.

## Callback binding protocol

The provider callback ID is disposable mechanical state in `BindingTable`, not TPF Await state. `waitForCallback` creates the durable callback first; its submitter conditionally records a provider registration keyed by the already-stable TPF execution checkpoint and generation. The TPF Await stream later joins that registration to the authoritative interaction and correlation identities. A missing registration makes the stream batch retry instead of losing the wake-up, and the scheduled reconciler repairs either event ordering. If a registration or binding row is deleted, the reconciler rebuilds it from public `GetDurableExecutionHistory` plus the read-only TPF semantic checkpoint. It does not use `GetDurableExecutionState`, which requires an SDK checkpoint token and is not an external reconciliation API.

Callback success is attempted only after the authoritative TPF interaction is `COMPLETED`. A zero-padded generation sort key fences older provider executions. A crash after callback success is classified from public durable history before append-only delivery evidence is written. Callback expiry or a terminal provider event starts a deterministic replacement generation from the TPF execution checkpoint; it does not submit or re-drive the TPF execution again.

The EventBridge-hosted reconciler calls the bounded `sweepOnce` action while Durable is suspended on a callback, and durable condition polling does the same after wake-up. AWS owns when those mechanical retry wake-ups run; TPF still decides which due execution is admitted, how attempts advance, when DLQ evidence is written, and whether an operator-authorised re-drive returns the execution to `QUEUED`.

The proof also records two provider constraints that affect a production design. `ListDurableExecutionsByFunction` is queried by the unqualified function name because the list API does not accept an alias filter; the returned execution ARN and numbered function version remain the authority for recovery. Provider history is disposable: if its public API reports the execution as unavailable after retention expiry, the reconciler starts a generation-fenced replacement from the retained TPF checkpoint and binding. The runtime now materialises a terminal Await's admitted completion directly at the coordinator, so the proof no longer needs a synthetic result step.

## Current evidence

On 2026-09-30, a clean `us-east-2` deployment passed all 12 deployed test methods: zero failures/errors, one then-intentionally skipped elapsed-retention test, 16 passing evidence groups, and all 22 catalogued fault scenarios. A targeted promotion deployment then passed ten seeded randomized callback/binding race repetitions (`1790770016`) and a provider-history-unavailable recovery. Both lanes asserted the admitted terminal Await value in the final Durable output. The retained stack exposed one missing Await descriptor registration in the reconciliation shell; after that local registration was added, the history-loss test passed on the same stack without repeating the randomized lane. The runner removed the stack and artifact bucket.

Earlier diagnostic deployments also exposed and closed two harness/recovery hazards: outstanding SQS receives can outlive a mapping's reported `Disabled` state, and one malformed retained binding must not poison reconciliation of valid bindings. None of these findings moved semantic authority or required parallel durable TPF state.

## Local validation

Every Maven invocation must use this worktree's isolated repository:

```bash
./mvnw -pl \
  :pipelineframework-aws-durable-proof-shared,
  :pipelineframework-aws-durable-proof-driver,
  :pipelineframework-aws-durable-proof-action-host,
  :pipelineframework-aws-durable-proof-fault-tests \
  -am verify -DskipITs \
  -Dmaven.repo.local="$PWD/.m2/repository"

sam validate --template-file aws-durable-proof/template.yaml --region us-east-2
(cd aws-durable-proof && sam validate --lint --template-file template.yaml --region us-east-2)
```

## Disposable deployment

Use a non-root deployment identity. The runner rejects account-root credentials before any mutation, optionally assumes a supplied least-privilege role, builds with the isolated Maven repository, uploads the artifacts, deploys the stack, runs the fast fault lane, writes `proof-report.json`, and deletes a successful stack by default. A failure preserves the stack for diagnosis.

```bash
./aws-durable-proof/scripts/run-deployed-proof.sh \
  --region us-east-2 \
  --role-arn arn:aws:iam::123456789012:role/tpf-durable-proof \
  --stack-name tpf-durable-proof-001
```

Use `--promotion-only` to run only the ten randomized callback/binding repetitions and history-loss recovery. Use `--history-only` to rerun only the latter against an already deployed stack. Lambda exposes stop and configured retention, but no API that expires one execution's history on demand, so the deployed test injects the same unavailable-history condition while a focused unit test verifies that the real `ResourceNotFoundException` response takes the replacement-generation path.

## Promotion and rejection gates

The architecture is promoted as the candidate initial AWS production coordinator because:

1. scenarios 1–22 passed in the deployed fault suite;
2. ten additional randomized callback/binding races converged without duplicate TPF execution or Await admission;
3. provider-history loss started a generation-fenced replacement from retained TPF state and completed normally;
4. callback and recovery convergence stayed within the five-minute acceptance bound without manual data repair;
5. a parked execution completed after its alias advanced while retaining its numbered version;
6. the deployed functions executed under the scoped roles in the template; and
7. the terminal-Await result passthrough defect was fixed and verified both locally and in the deployed promotion lane.

Reject this architecture for initial TPF Cloud if active callback identity cannot be reconstructed from the supported history API, uncertain callback delivery cannot be classified, parked versions cannot finish, TPF semantic state cannot reconstruct/re-drive after provider-history loss, or least-privilege permissions require broad account access.

See [PROMOTION-REPORT.md](./PROMOTION-REPORT.md) for the evidence and decision. This stack remains a proof, not production Lambda support. It intentionally omits production networking, KMS/customer key policy, multi-region disaster recovery, tenancy isolation, quotas, Terraform, and release automation.
