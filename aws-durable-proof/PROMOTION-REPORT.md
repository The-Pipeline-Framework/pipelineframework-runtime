# AWS Durable coordinator promotion report

Date: 30 September 2026
Issue: [pipelineframework#976](https://github.com/The-Pipeline-Framework/pipelineframework/issues/976)

## Decision

Promote the AWS Durable-backed coordinator as the candidate initial AWS production architecture for TPF Cloud.

AWS Lambda Durable Functions owns orchestration liveness, checkpointing, suspension, callback wake-up, and mechanical retry timing. TPF remains authoritative for execution and Await identity, typed completion admission, parent release, release pinning, signed worker transitions, retry/DLQ evidence, results, and operator-authorised re-drive. The native coordinator remains the portable semantic reference and conformance implementation.

This is an architecture promotion, not a claim that the proof stack is production-ready. Networking, customer-managed encryption policy, tenant isolation, quota engineering, multi-region recovery, Terraform, release automation, and operational runbooks remain productisation work.

## Evidence completed

| Evidence | Result | Material assertion |
| --- | --- | --- |
| Deployed 22-scenario fault suite | Passed | 12 test methods, zero failures/errors, 16 evidence groups and all 22 catalogued faults passed on real Lambda Durable, DynamoDB/Streams, SQS/DLQs, EventBridge, and Lambda workers. |
| Randomized callback/binding races | Passed | Ten repetitions with seed `1790770016` varied binding checkpoint failure, completion timing, duplicate delivery, callback uncertainty, and explicit reconciliation. Every repetition retained one TPF execution and interaction, recorded delivery evidence, and returned the admitted Await result. |
| Provider-history-loss recovery | Passed | The old Durable execution was stopped, history was made unavailable to reconciliation, and generation 2 was started from the retained TPF checkpoint and binding. It completed with the same TPF execution identity and the admitted Await result. Duration: 46,672 ms. |
| Real expiry response classification | Passed | A focused unit test proves `GetDurableExecutionHistory` returning `ResourceNotFoundException` is classified as closed mechanical state and enters replacement-generation recovery. |
| Terminal-Await result passthrough | Passed | The coordinator now loads the canonical Await resume payload and materialises it when no authored step remains. The focused runtime test proves no no-op worker dispatch; deployed promotion executions returned `approved-result`. |
| Cleanup | Passed | The promotion stack and artifact bucket were absent after the successful lane. |

The successful 22-scenario deployment is accepted as sufficient evidence for the already-covered properties. No additional complete clean deployments were required or run.

## Finding closed during promotion

The first history-loss invocation failed because the reconciliation Lambda queried durable Await records before registering the generated Await descriptor needed to decode them. This was a shell-initialisation omission, not a conflict in semantic authority. Registering the same generated descriptor already used by the wake-up action allowed the preserved stack to recover and pass without repeating the randomized race lane.

No new durable TPF record, dual-write, lease, queue, callback identity, or semantic transition was introduced.

## Callback and history conclusion

The callback binding remains disposable provider-mechanical state. TPF stores no provider callback ID in execution or Await semantic state. The durable callback submitter records an idempotent registration against the stable TPF checkpoint; streams and reconciliation join it to the authoritative Await identity. Generation fencing makes starting a replacement safe when provider history is unavailable, even if unavailability reflects an uncertain old provider outcome.

AWS does not expose an API to expire one execution's retained history on demand. The promotion evidence therefore combines:

- a deployed fault that prevents the reconciler from reading old history while exercising real replacement and callback infrastructure; and
- a focused test of the exact `ResourceNotFoundException` returned after provider history is no longer available.

Elapsed wall-clock retention expiry was not repeated because the recovery decision depends only on that supported API outcome, not on elapsed time.

## Promotion gates

| Gate | Verdict |
| --- | --- |
| No duplicate TPF execution, Await admission, or signed transition | Passed |
| Callback registration and completion order converge | Passed |
| Uncertain callback outcomes are replay-safe | Passed |
| Provider-history loss reconstructs a replacement from retained TPF state | Passed |
| Terminal Await returns the admitted typed result | Passed |
| Release identity remains pinned across worker transitions | Passed in the 22-scenario suite |
| Retry/DLQ and operator re-drive remain TPF-owned | Passed in the 22-scenario suite |
| Parked execution survives alias advancement | Passed in the 22-scenario suite |
| Disposable deployment cleans up successfully | Passed |

No promotion rejection condition was observed.

## Remaining product risks

- AWS should confirm that callback discovery through public Durable history and history-based uncertain-outcome classification are supported long-term integration contracts.
- The mechanical provider binding must outlive the configured provider-history retention window or be exportable with the TPF checkpoint; simultaneous loss of both is outside this proof.
- A production implementation needs quotas, alarms, tenant isolation, disaster recovery, version retirement, and least-privilege deployment automation.
- SQS remains the worker boundary for backpressure, redelivery, DLQ evidence, and uncertain remote outcomes. Direct Durable-to-worker invocation remains a separate decision.

These are productisation and provider-contract questions, not evidence of semantic divergence in the promoted boundary.
