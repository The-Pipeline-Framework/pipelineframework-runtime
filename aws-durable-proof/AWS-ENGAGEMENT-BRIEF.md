# AWS engagement brief: TPF coordination on Lambda Durable Functions

## What TPF is

The Pipeline Framework (TPF) executes strongly typed application flows. TPF's durable semantics include stable execution identity, release pinning, signed worker identity, Await identity and typed completion admission, parent release, retry/DLQ evidence, inspection, and operator-controlled re-drive.

We are evaluating Lambda Durable Functions as the initial AWS coordination engine for TPF Cloud. The proposed authority boundary is intentional: Lambda Durable owns orchestration liveness, checkpoint/replay, suspension, callback wake-up, and mechanical retry; TPF retains a reconstructable semantic checkpoint and every pipeline transition decision. The portable native coordinator remains the conformance implementation.

## What is already proved

- The local durable runner safely drives existing bounded `PipelineControlPlane` and SQS message actions.
- Provider replay returns the same TPF execution and does not repeat a signed worker transition.
- TPF admits Await completion and releases the parent before the provider callback is completed.
- TPF can reconstruct execution after provider-history loss from its semantic checkpoint.
- The deployed proof packages real Durable Lambda, DynamoDB/Streams, SQS/DLQs, Quarkus Lambda workers, generation-fenced callback bindings, reconciliation, and fault injection.
- The deployed callback race is closed without a dual-write into TPF: the `waitForCallback` submitter records an idempotent provider registration against the TPF execution checkpoint, and a stream/reconciler join adds the later TPF Await identity.
- One clean `us-east-2` deployment passed all 22 fault scenarios with zero test failures/errors. Diagnostic runs also proved that SQS mapping disablement needs an explicit receive-quiescence allowance and that malformed retained binding records must be isolated per item so reconciliation continues for healthy executions.
- A targeted promotion lane passed ten seeded randomized callback/binding races, including completion on both sides of binding creation, duplicate completion delivery, callback API uncertainty, and explicit reconciliation.
- A deployed provider-history-loss test stopped the old Durable execution, made its history unavailable to reconciliation, and completed a generation-fenced replacement from the retained TPF checkpoint and provider binding. A focused unit test maps the real history API's `ResourceNotFoundException` to the same recovery path.
- A terminal Await now returns the admitted completion value directly; the proof no longer requires a synthetic post-Await result step.

Durable Functions materially lowers launch risk if AWS can own the hardest liveness machinery without becoming semantic authority. The service provides managed checkpoint/replay and durable waits for up to one year; versions or aliases are required for deterministic execution. The function role needs `CheckpointDurableExecution` and `GetDurableExecutionState`; external reconciliation uses `GetDurableExecutionHistory`, while callback senders use `SendDurableExecutionCallbackSuccess`. See the official [IaC guidance](https://docs.aws.amazon.com/lambda/latest/dg/durable-getting-started-iac.html), [security model](https://docs.aws.amazon.com/lambda/latest/dg/durable-security.html), and [history API](https://docs.aws.amazon.com/lambda/latest/api/API_GetDurableExecutionHistory.html).

The deployed shape also uses AWS mechanisms that the first spike did not exercise: Durable callback timeouts schedule mechanical wake-up, replay-safe condition polling invokes TPF's bounded `sweepOnce`, terminal Durable status changes route through EventBridge, and function/stream DLQs retain failed provider events. These mechanisms reduce scheduler and acknowledgement machinery without moving TPF's retry, timeout, DLQ, or re-drive decisions. AWS documents numbered-version pinning across alias updates, EventBridge Durable status events, and Durable-function DLQ behaviour in its [invocation](https://docs.aws.amazon.com/lambda/latest/dg/durable-invoking.html), [monitoring](https://docs.aws.amazon.com/lambda/latest/dg/durable-monitoring.html), and [best-practices](https://docs.aws.amazon.com/lambda/latest/dg/durable-best-practices.html) guidance.

## Remaining questions for AWS engineering

1. Does the `waitForCallback` submitter run only after the callback registration is durably checkpointed, and is re-running its idempotent side effect the supported way to survive failure between callback creation and external registration persistence?
2. Is discovery of an active callback ID through `GetDurableExecutionHistory` a supported long-term reconstruction contract, including event `id`, `name`, and `CallbackStartedDetails.callbackId` correlation across pagination?
3. After an uncertain `SendDurableExecutionCallbackSuccess` outcome, is history classification (`CALLBACK_SUCCEEDED`, `CALLBACK_FAILED`, or `CALLBACK_TIMED_OUT`) the recommended idempotency pattern? Is there a stronger callback-status or idempotency API?
4. Can callback registration expose a caller correlation value or idempotency key so the separate provider-registration row and reconciliation join become unnecessary?
5. What ordering/delivery guarantees apply when a standard Lambda asynchronously starts a named durable execution through a qualified alias? Which response definitively distinguishes “already exists” from an uncertain start?
6. AWS documentation confirms that an execution started through an alias remains pinned to the resolved numbered version. What operational path does AWS recommend for urgent security fixes, version retirement, and migrations affecting executions parked for months?
7. `ListDurableExecutionsByFunction` does not accept an alias as its function filter. Is unqualified discovery followed by validation of the returned numbered function ARN the recommended recovery pattern?
8. Which history, operation-count, payload-size, callback-duration, checkpoint-rate, and concurrent parked-execution limits should shape a high-throughput coordinator? Which limits are adjustable?
9. Is there a supported test capability to expire or isolate one execution's retained history, rather than injecting the documented `ResourceNotFoundException` outcome or waiting the configured minimum retention period?
10. Can AWS review the least-privilege split in this proof: durable execution role for checkpointing/action invocation, action role for TPF Dynamo/SQS plus callback/history APIs, and no cross-account callback authority?
11. What disaster-recovery guarantees and export/restore patterns exist for active durable executions and their history?
12. Which CloudWatch metrics, EventBridge events, and CloudTrail data events are recommended to diagnose callback loss, replay storms, and version-specific incidents?

## Product/API gaps versus TPF-owned concerns

Potential AWS gaps:

- no atomic transaction or first-class correlation between a TPF Await identity and a newly registered callback; the proof narrows this to a replay-safe provider registration plus a reconstructable stream join, subject to AWS confirming the submitter/history guarantees above;
- no dedicated idempotent “complete or read terminal callback status” operation;
- no per-execution history-expiry control for deterministic disaster-recovery testing;
- uncertainty about supported callback discovery and long-lived version migration.

TPF concerns that AWS should not own:

- typed Await completion validation and duplicate admission;
- pipeline/release identity and signed worker protocol;
- retry/DLQ evidence as a TPF semantic record;
- the operator decision, checkpoint, and authorization for re-drive.

## Decision requested from the joint review

TPF has promoted the Durable-backed coordinator as the candidate initial AWS production architecture on the strength of the deployed fault and promotion gates. We want AWS to validate whether the public history/callback contract is intended to support reconstructable mechanical bindings and history-based classification of uncertain callback delivery. If either contract is unsupported, we need a small AWS integration primitive or must revisit the native Lambda + DynamoDB + SQS + scheduler coordinator before productisation.
