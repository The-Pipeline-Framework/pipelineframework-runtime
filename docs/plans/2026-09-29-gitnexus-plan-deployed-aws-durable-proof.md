# GitNexus Engineering Plan

Issue: [pipelineframework#976](https://github.com/The-Pipeline-Framework/pipelineframework/issues/976)

## 1. Objective

Build the smallest deployed AWS fault-injection proof that can promote or kill AWS Lambda Durable Functions as the initial TPF Cloud AWS coordinator.

Authority boundary:

- AWS Durable owns orchestration liveness, checkpointing, suspension, wake-up delivery, and mechanical retries.
- TPF owns execution identity, reconstructable semantic checkpoints, Await identity/admission/parent release, release pinning, worker identity, retry/DLQ evidence, results, and re-drive decisions.
- SQS worker boundaries remain.
- The native single-shot coordinator remains the portable semantic reference.

The deliverable is proof-only infrastructure/handlers, a repeatable fault suite, machine-readable evidence, and an AWS engineering brief. It must not become a production provider adapter, change PipelineControlPlane, or move TPF semantic authority.

## 2. Current Behaviour

The stacked #492 worktree proves the authority model locally. AwsShapedCoordinatorActionsIT drives the Lambda Durable local runner over PipelineControlPlane and the SQS actions, exercises replay/callback completion, and reconstructs from a TPF checkpoint after provider-history loss [runtime/src/test/java/org/pipelineframework/AwsShapedCoordinatorActionsIT.java:368] [runtime/src/test/java/org/pipelineframework/AwsShapedCoordinatorActionsIT.java:459].

The Durable dependency is test-scoped [runtime/pom.xml:168]. LocalStack supplies AWS-shaped DynamoDB/SQS, but no real Durable Lambda, DynamoDB Streams mapping, SQS-triggered Lambda worker, or provider callback is deployed.

PipelineControlPlane is the single-shot semantic facade [runtime/src/main/java/org/pipelineframework/orchestrator/PipelineControlPlane.java:16], implemented locally by LocalPipelineControlPlane [runtime/src/main/java/org/pipelineframework/LocalPipelineControlPlane.java:26]. Work, Await completion, and transition processing are bounded SQS actions [runtime/src/main/java/org/pipelineframework/orchestrator/SqsWorkItemAction.java:18] [runtime/src/main/java/org/pipelineframework/awaitable/sqs/SqsAwaitCompletionAction.java:23] [runtime/src/main/java/org/pipelineframework/orchestrator/SqsTransitionWorkerAction.java:27].

## 3. Relevant Architecture

### Deployed proof

Add a non-published proof aggregator with:

1. A plain Java 21 Durable driver Lambda using aws-durable-execution-sdk-java.
2. A Quarkus Lambda action host exposing named handlers for control-plane operations, the three SQS actions, Await/binding stream wakeup, and reconciliation.
3. Opt-in deployed Failsafe tests.

The Durable driver calls the control-plane action Lambda only inside named durable steps; it never mutates TPF tables. SQS handlers map AWS records to SqsInboundMessage and RETRY dispositions to partial-batch failures.

The disposable SAM stack contains a numbered/aliased Durable Lambda, standard ingress Lambda, action-host functions, TPF execution/Await DynamoDB tables, a separate callback-binding table plus streams, SQS queues/DLQs, event-source mappings, one reconciliation schedule, CloudWatch evidence, and least-privilege roles.

### Callback protocol

No transaction spans TPF Await and AWS Durable callback registration. The binding is reconstructable mechanical state, not TPF semantic state:

1. The initial durable execution name is derived from tenant, stable ingress idempotency key, and generation; recovery names use the TPF execution id as the stable key.
2. `waitForCallback` checkpoints the callback before its submitter runs.
3. The submitter conditionally writes a provider registration keyed by TPF execution checkpoint and generation; it does not need the not-yet-materialized Await identity.
4. The Await stream joins that registration to the authoritative interaction and correlation identities and conditionally writes the generation-fenced binding.
5. If Await materializes first, its stream record retries until registration exists; if registration arrives first, the later Await event completes the join. Scheduled reconciliation repairs missed events and can reconstruct either record.
6. Callback success is sent only after TPF admitted completion and released the parent.
7. Generation fences stale callbacks. A crash after callback success is resolved from provider execution/callback history.
8. A missing registration or binding is rebuilt from deterministic execution identity, public AWS durable history, and a read-only TPF semantic checkpoint. If AWS cannot expose the active callback identity/state reliably, the architecture fails.
9. The EventBridge-hosted reconciler invokes one bounded `sweepOnce` per tick so TPF retries and Await timeouts progress while Durable is suspended.
10. After terminal provider-history expiry, only the TPF checkpoint/evidence/re-drive intent must survive.

This avoids writing provider callback identity into TPF Await state.

### Recovery and versioning

Bounded TPF actions may be replayed because semantic transitions are idempotent. SQS remains for worker backpressure, redelivery, DLQ evidence, and uncertain remote outcomes. Durable executions use numbered versions. TPF decides re-drive; AWS may mechanically start a new generation from the TPF checkpoint.

## 4. GitNexus Findings

- [context] The index is at aa4a53c3b6544f138231d3a07be3d3ad722a9285; the active worktree is 3dae051c787d3cb099bc67579902ecbf61bbb098 plus two unstaged #492 files. The graph is five commits stale. Policy forbids automatic refresh, so source is authoritative.
- [clusters] Relevant areas: Orchestrator, Awaitable, Store, Function, Worker, Checkpoint, Controlplane.
- [query/context] PipelineControlPlane owns the action contract; provider code must adapt it, not add a parallel API.
- [impact] PipelineControlPlane upstream impact was LOW: 12 impacted, three direct. It remains unchanged.
- [impact + source] DynamoAwaitInteractionStore returned UNKNOWN because the stale graph could not resolve callers; source confirms active CDI usage [runtime/src/main/java/org/pipelineframework/awaitable/store/DynamoAwaitInteractionStore.java:58].
- [query/impact + source] #489 SQS actions are absent from the stale index; current source verifies them.
- [processes] Indexed Await, Dynamo persistence, re-drive, and startup flows predate stacked #489/#490/#492 and are orientation only.

## 5. Statement-Level PDG Findings

No PDG layer exists. Preserve/test current source ordering:

- TPF Await admission and parent release before provider callback success;
- transition response publication before acknowledgement;
- nonce recording after response publication.

Do not infer safety from absent graph edges.

## 6. Proposed Changes

### Modules

Add aws-durable-proof to the canonical reactor with maven.deploy.skip=true:

- durable-driver: shaded Java 21 Durable Lambda;
- action-host: Quarkus Lambda named handlers backed by CDI PipelineControlPlane/actions;
- fault-tests: opt-in deployed tests that skip without an explicit stack descriptor.

No Maven profile or normal-CI cloud deployment.

### Durable flow and handlers

AwsDurableCoordinatorHandler performs stable submit, dispatch-to-await, waitForCallback, resume, and typed/raw result steps. Output retains TPF execution and pinned release identity.

Handlers remain thin:

- ingress starts a deterministic durable execution;
- action gateway maps one operation to one PipelineControlPlane call;
- SQS handlers report RETRY records as batch failures;
- Await and binding streams call one idempotent wakeup service;
- scheduled reconciliation scans bounded open candidates;
- proof-only failpoints use named one-shot Dynamo control records.

### Binding repository

Use immutable conditional puts and append-only delivery-attempt evidence, not UpdateItem/upsert. Store tenant/execution/interaction/correlation ids, generation, deterministic provider execution name, callback id, status/expiry, and creation time. Implement conditional bind, generation-fenced delivery, provider-state classification, missing-row reconstruction, and paginated reconciliation. This store never admits completion or releases a parent.

### Infrastructure and runner

SAM/CloudFormation must publish versions/alias, configure streams/SQS/DLQs/partial responses/reserved concurrency/visibility/alarms, use least privilege, reject root credentials, emit a stack descriptor, tag resources issue=976, and delete successful stacks by default while preserving failures.

### Required fault scenarios

1. repeated ingress start with the same name;
2. submit-step replay before action invocation;
3. TPF submit succeeds but Durable checkpoint is lost;
4. action Lambda uncertain timeout after success;
5. duplicate work SQS record;
6. work succeeds then crash before acknowledgement;
7. worker times out after signed transition before response publication;
8. response publishes then crash before acknowledgement;
9. WAITING_EXTERNAL before callback binding;
10. binding before Await completion;
11. completion while Await stream is delayed;
12. wakeup crash before SendCallbackSuccess;
13. callback succeeds then crash before delivery evidence;
14. replayed completion and stream record;
15. stale generation after replacement binding;
16. deleted binding reconstructed from AWS state;
17. disabled stream recovered by reconciliation;
18. mixed stream batch retries only failures;
19. expired/already-closed callback classified from provider state;
20. new Durable version deployed while execution is parked;
21. poison SQS message reaches DLQ without semantic mutation;
22. terminal provider history loss followed by TPF inspection/re-drive.

### AWS engagement brief

Add AWS-ENGAGEMENT-BRIEF.md explaining TPF, the boundary, evidence, and why Durable lowers launch risk. Ask AWS whether active callback discovery is a supported reconstruction contract; how to classify repeated SendCallbackSuccess after uncertainty; whether callback metadata/registration hooks remove binding; guarantees for event-source/standard-Lambda starts; version pinning/migration; and relevant duration/history/operation limits.

## 7. Implementation Sequence

1. Preserve the two dirty #492 files and re-check evidence drift.
2. Run GitNexus impact before each existing-symbol edit.
3. Add proof modules; root verify must build without AWS access.
4. Add immutable models, naming, generation fences, partial-batch mapping, and failpoint tests.
5. Add Quarkus action host using existing CDI boundaries.
6. Add Durable driver and LocalDurableTestRunner coverage, reusing #492 fixtures.
7. Add binding/reconciliation and prove both race orders plus missing-row recovery locally.
8. Add SAM template, least-privilege IAM, outputs, alarms, versions/alias.
9. Add deployed runner and all 22 fault scenarios.
10. Generate proof-report.json and AWS brief.
11. Run focused, root, GitNexus detect_changes/review, and CodeRabbit review gates.
12. Do not commit/push/open a PR without explicit follow-up authorization.

Stop conditions:

- Do not deploy with current account-root credentials.
- If active callback identity cannot be reconstructed, report the AWS gap; do not duplicate TPF semantics.
- If uncertain callback success cannot be classified, fail the architecture unless AWS provides a documented pattern.

## 8. Test Strategy

Unit/local tests cover deterministic names/generations, immutable binding conflicts, disposition-to-batch mapping, completion-first/binding-first, callback-success-then-crash, binding reconstruction, config validation, Durable replay, and existing AwsShapedCoordinatorActionsIT.

Deployed assertions:

- exactly one TPF execution and no duplicate admitted/signed transition;
- TPF state changes before wakeup;
- stale generations cannot wake current execution;
- correct SQS/DLQ evidence;
- binding deletion is recoverable without manual ids;
- parked execution survives a new version;
- provider-history loss does not prevent inspection/re-drive;
- recovery completes within two reconciliation intervals and five minutes;
- no manual table/callback repair.

Promotion requires all 22 scenarios in three clean deployments and scenarios 9-18 in ten randomized repetitions per deployment. Reject if callback reconstruction/classification, version survival, idempotency, semantic reconstruction, or least-privilege deployment fails.

Commands:

    ./mvnw -pl runtime -Dit.test=AwsShapedCoordinatorActionsIT verify -Dmaven.repo.local="$PWD/.m2/repository"
    ./mvnw -pl aws-durable-proof -am verify -Dmaven.repo.local="$PWD/.m2/repository"
    ./mvnw clean verify --no-transfer-progress -Dmaven.repo.local="$PWD/.m2/repository"
    ./aws-durable-proof/scripts/run-deployed-proof.sh --region us-east-2 --role-arn <least-privilege-role-arn> --stack-name tpf-durable-proof-<id>

## 9. Risk and Impact Analysis

- HIGH: configured AWS credentials are account root; deployment is blocked pending a least-privilege session.
- HIGH: callback registration crosses systems; only reconstructable binding plus reconciliation is acceptable.
- MEDIUM: Quarkus Lambda and Durable handler bootstrap differ; keep two artifacts.
- MEDIUM: service duration/history/operation limits may reject long/chatty pipelines.
- MEDIUM: version behavior for parked executions must be proved.
- MEDIUM: real AWS tests incur cost; namespace/tag/cap/delete resources.
- UNKNOWN graph risk: store/actions are incomplete in stale index.
- Existing unstaged runtime/pom.xml and AwsShapedCoordinatorActionsIT.java are intentional #492 work and must be preserved.

## 10. Files Expected to Change

Existing:

- pom.xml: add proof aggregator.
- runtime/pom.xml: preserve test-scoped #492 dependency.
- runtime/src/test/java/org/pipelineframework/AwsShapedCoordinatorActionsIT.java: preserve/extract fixtures without weakening tests.

New:

- aws-durable-proof/pom.xml
- aws-durable-proof/{shared,durable-driver,action-host,fault-tests}/pom.xml
- aws-durable-proof/shared/src/main/java/org/pipelineframework/awsproof/model/**
- aws-durable-proof/durable-driver/src/main/java/org/pipelineframework/awsproof/AwsDurableCoordinatorHandler.java
- aws-durable-proof/action-host/src/main/java/org/pipelineframework/awsproof/{ProofActionGatewayHandler,ProofWorkItemHandler,ProofAwaitCompletionHandler,ProofTransitionWorkerHandler,ProofAwaitWakeupHandler,ProofAwaitReconcilerHandler,ProofCallbackBindingRepository,ProofWakeupService}.java
- aws-durable-proof/fault-tests/src/test/java/org/pipelineframework/awsproof/DeployedAwsDurableProofIT.java
- aws-durable-proof/template.yaml
- aws-durable-proof/scripts/run-deployed-proof.sh
- aws-durable-proof/{README.md,AWS-ENGAGEMENT-BRIEF.md,proof-report.schema.json}

Factoring may follow Quarkus packaging conventions, but ownership/module boundaries must remain.

## 11. Reusable Implementation Context

\`\`\`yaml
implementation_context:
  task_summary: "Implement issue #976: disposable real-AWS Durable proof with fault injection, reconstructable callback binding, SQS workers, and TPF-owned semantic checkpoints."
  acceptance_criteria:
    - "AWS owns mechanics only; TPF remains semantic authority."
    - "All 22 fault scenarios pass without duplicate transitions or lost wakeups."
    - "Active binding loss is reconstructable without changing TPF Await state."
    - "Ordinary verify never deploys and always uses the isolated Maven repository."
    - "Deployment refuses root credentials."
    - "Proof produces machine-readable evidence and AWS brief."
  evidence_provenance:
    schema_version: 2
    head_commit: "3dae051c787d3cb099bc67579902ecbf61bbb098"
    generated_plan_path: "docs/plans/2026-09-29-gitnexus-plan-deployed-aws-durable-proof.md"
    global_dirty_digest:
      algorithm: "sha256"
      canonicalization: "gitnexus-evidence-provenance-v2 NUL-framed UTF-8 records"
      value: "3ef37156ea25cf3e18c44d3f7120b1cfcf4e618bb3ebf6f755ac3c39f85cc092"
    cited_path_manifest:
      - path: ".github/tpf-system-tests.json"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "clean"
        rename_from: null
        rename_to: null
        head_digest: "sha256:5843a5a9c4814011104345232be9485c19fb8205bbda13bcdec1b72ee276fe5d"
        index_digest: "sha256:5843a5a9c4814011104345232be9485c19fb8205bbda13bcdec1b72ee276fe5d"
        worktree_digest: "sha256:5843a5a9c4814011104345232be9485c19fb8205bbda13bcdec1b72ee276fe5d"
        untracked_digest: "absent"
      - path: "AGENTS.md"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "clean"
        rename_from: null
        rename_to: null
        head_digest: "sha256:5b2b85808ee2d399a7f31234eadb749df3dfb12c423e492f8df414be54dae8ec"
        index_digest: "sha256:5b2b85808ee2d399a7f31234eadb749df3dfb12c423e492f8df414be54dae8ec"
        worktree_digest: "sha256:5b2b85808ee2d399a7f31234eadb749df3dfb12c423e492f8df414be54dae8ec"
        untracked_digest: "absent"
      - path: "pom.xml"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "clean"
        rename_from: null
        rename_to: null
        head_digest: "sha256:f83635e98318c37d951626e6531d8389d2e115dd3c906fac9ffd7a9ad077ae60"
        index_digest: "sha256:f83635e98318c37d951626e6531d8389d2e115dd3c906fac9ffd7a9ad077ae60"
        worktree_digest: "sha256:f83635e98318c37d951626e6531d8389d2e115dd3c906fac9ffd7a9ad077ae60"
        untracked_digest: "absent"
      - path: "runtime/pom.xml"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "unstaged"
        rename_from: null
        rename_to: null
        head_digest: "sha256:cd43b2fdc4aab507394ef40dd5b938b541b4b33a8fe1893a318891615f21ee4b"
        index_digest: "sha256:cd43b2fdc4aab507394ef40dd5b938b541b4b33a8fe1893a318891615f21ee4b"
        worktree_digest: "sha256:fd30a1c1020208581a634a041a6e62d6539e6eed3c942d22529be8a7d5857cca"
        untracked_digest: "absent"
      - path: "runtime/src/main/java/org/pipelineframework/LocalPipelineControlPlane.java"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "clean"
        rename_from: null
        rename_to: null
        head_digest: "sha256:d9f378687567b73496aa708bcceb83ac379ec62158c78301fd3d411c5418fc57"
        index_digest: "sha256:d9f378687567b73496aa708bcceb83ac379ec62158c78301fd3d411c5418fc57"
        worktree_digest: "sha256:d9f378687567b73496aa708bcceb83ac379ec62158c78301fd3d411c5418fc57"
        untracked_digest: "absent"
      - path: "runtime/src/main/java/org/pipelineframework/awaitable/sqs/SqsAwaitCompletionAction.java"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "clean"
        rename_from: null
        rename_to: null
        head_digest: "sha256:4470d3d5e24b04c7a0324fac9e6691e08d45441ee31164fc1fa2216115fc3af9"
        index_digest: "sha256:4470d3d5e24b04c7a0324fac9e6691e08d45441ee31164fc1fa2216115fc3af9"
        worktree_digest: "sha256:4470d3d5e24b04c7a0324fac9e6691e08d45441ee31164fc1fa2216115fc3af9"
        untracked_digest: "absent"
      - path: "runtime/src/main/java/org/pipelineframework/awaitable/store/DynamoAwaitInteractionStore.java"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "clean"
        rename_from: null
        rename_to: null
        head_digest: "sha256:d2ecce41bc3d40086b32f26d7d2ece930075882f695babaea3166378da554bcd"
        index_digest: "sha256:d2ecce41bc3d40086b32f26d7d2ece930075882f695babaea3166378da554bcd"
        worktree_digest: "sha256:d2ecce41bc3d40086b32f26d7d2ece930075882f695babaea3166378da554bcd"
        untracked_digest: "absent"
      - path: "runtime/src/main/java/org/pipelineframework/orchestrator/PipelineControlPlane.java"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "clean"
        rename_from: null
        rename_to: null
        head_digest: "sha256:ca7a9fb88f159603cddf8d38756f3b40108aea96af8bb46701710068591f134f"
        index_digest: "sha256:ca7a9fb88f159603cddf8d38756f3b40108aea96af8bb46701710068591f134f"
        worktree_digest: "sha256:ca7a9fb88f159603cddf8d38756f3b40108aea96af8bb46701710068591f134f"
        untracked_digest: "absent"
      - path: "runtime/src/main/java/org/pipelineframework/orchestrator/SqsTransitionWorkerAction.java"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "clean"
        rename_from: null
        rename_to: null
        head_digest: "sha256:5e9cc682d9d49b2f174bd848d2da5339c809041d33a88c8759eec2cf455e5a6c"
        index_digest: "sha256:5e9cc682d9d49b2f174bd848d2da5339c809041d33a88c8759eec2cf455e5a6c"
        worktree_digest: "sha256:5e9cc682d9d49b2f174bd848d2da5339c809041d33a88c8759eec2cf455e5a6c"
        untracked_digest: "absent"
      - path: "runtime/src/main/java/org/pipelineframework/orchestrator/SqsWorkItemAction.java"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "clean"
        rename_from: null
        rename_to: null
        head_digest: "sha256:20a7583e741357146e9f15c61b48f5be61cf5d298ec20af14ead7a99ff70100d"
        index_digest: "sha256:20a7583e741357146e9f15c61b48f5be61cf5d298ec20af14ead7a99ff70100d"
        worktree_digest: "sha256:20a7583e741357146e9f15c61b48f5be61cf5d298ec20af14ead7a99ff70100d"
        untracked_digest: "absent"
      - path: "runtime/src/test/java/org/pipelineframework/AwsShapedCoordinatorActionsIT.java"
        object_kind: {head: "regular", index: "regular", worktree: "regular", untracked: "absent"}
        state: "unstaged"
        rename_from: null
        rename_to: null
        head_digest: "sha256:8e49d0184c1c808a5509f40afc3d4fd1de2dc04bb3fb72e2c58948e10177fbe4"
        index_digest: "sha256:8e49d0184c1c808a5509f40afc3d4fd1de2dc04bb3fb72e2c58948e10177fbe4"
        worktree_digest: "sha256:e96a1c672336fbe6b8ca488b4eea2b97b76dede4d6309c02132b42230ffa1ad8"
        untracked_digest: "absent"
  primary_symbols:
    - {symbol: "PipelineControlPlane", file: "runtime/src/main/java/org/pipelineframework/orchestrator/PipelineControlPlane.java", lines: "16+", role: "Unchanged semantic action contract."}
    - {symbol: "SqsWorkItemAction", file: "runtime/src/main/java/org/pipelineframework/orchestrator/SqsWorkItemAction.java", lines: "18+", role: "Bounded work action."}
    - {symbol: "SqsAwaitCompletionAction", file: "runtime/src/main/java/org/pipelineframework/awaitable/sqs/SqsAwaitCompletionAction.java", lines: "23+", role: "Bounded Await admission action."}
    - {symbol: "SqsTransitionWorkerAction", file: "runtime/src/main/java/org/pipelineframework/orchestrator/SqsTransitionWorkerAction.java", lines: "27+", role: "Bounded signed transition action."}
    - {symbol: "AwsShapedCoordinatorActionsIT", file: "runtime/src/test/java/org/pipelineframework/AwsShapedCoordinatorActionsIT.java", lines: "368-937", role: "Local replay/callback proof."}
  related_symbols:
    - {symbol: "LocalPipelineControlPlane", relationship: "IMPLEMENTS", relevance: "CDI-backed action implementation."}
    - {symbol: "DynamoAwaitInteractionStore", relationship: "IMPLEMENTS AwaitInteractionStore", relevance: "Authoritative Await persistence."}
    - {symbol: "QueueAsyncCoordinator", relationship: "DELEGATED_TO", relevance: "Portable native reference."}
  execution_path:
    - "Ingress starts deterministic Durable execution."
    - "Durable step invokes bounded TPF submit."
    - "SQS actions drive TPF to WAITING_EXTERNAL."
    - "Callback checkpoints then mechanical binding is written."
    - "TPF admits completion/releases parent first."
    - "Stream/reconciler sends provider callback."
    - "Durable resumes and reads TPF result."
    - "TPF checkpoint supports inspection/re-drive after provider history loss."
  pdg_constraints:
    - description: "No PDG layer; preserve source-observed ordering."
      affected_statements: ["runtime/src/test/java/org/pipelineframework/AwsShapedCoordinatorActionsIT.java:368", "runtime/src/test/java/org/pipelineframework/AwsShapedCoordinatorActionsIT.java:459"]
      implementation_consequence: "Use focused ordering/fault tests."
  architectural_patterns:
    - {pattern: "Single-shot control plane", example_location: "runtime/src/main/java/org/pipelineframework/orchestrator/PipelineControlPlane.java", usage_guidance: "Adapt; do not alias."}
    - {pattern: "Reactive ACK/RETRY actions", example_location: "runtime/src/main/java/org/pipelineframework/orchestrator/SqsWorkItemAction.java", usage_guidance: "Map RETRY to partial batch failures."}
    - {pattern: "TPF-owned Await", example_location: "runtime/src/main/java/org/pipelineframework/awaitable/store/DynamoAwaitInteractionStore.java", usage_guidance: "Binding is auxiliary only."}
  files_to_modify:
    - {file: "pom.xml", symbols: ["modules"], intended_change: "Add proof aggregator."}
    - {file: "runtime/pom.xml", symbols: ["dependencies"], intended_change: "Preserve #492 test dependency."}
    - {file: "runtime/src/test/java/org/pipelineframework/AwsShapedCoordinatorActionsIT.java", symbols: ["durable proof tests"], intended_change: "Preserve/extract fixtures."}
    - {file: "aws-durable-proof/**", symbols: ["handlers", "binding", "fault suite"], intended_change: "Add deployed proof."}
  tests:
    - {file: "runtime/src/test/java/org/pipelineframework/AwsShapedCoordinatorActionsIT.java", scenarios: ["Replay -> same execution", "History loss -> reconstruct"]}
    - {file: "aws-durable-proof/action-host/src/test/**", scenarios: ["batch mapping", "both race orders", "uncertain callback", "binding reconstruction"]}
    - {file: "aws-durable-proof/fault-tests/src/test/java/org/pipelineframework/awsproof/DeployedAwsDurableProofIT.java", scenarios: ["Section 6 fault matrix"]}
  verification_commands:
    - "./mvnw -pl runtime -Dit.test=AwsShapedCoordinatorActionsIT verify -Dmaven.repo.local=\"$PWD/.m2/repository\""
    - "./mvnw -pl aws-durable-proof -am verify -Dmaven.repo.local=\"$PWD/.m2/repository\""
    - "./mvnw clean verify --no-transfer-progress -Dmaven.repo.local=\"$PWD/.m2/repository\""
    - "./aws-durable-proof/scripts/run-deployed-proof.sh --region us-east-2 --role-arn <least-privilege-role-arn> --stack-name tpf-durable-proof-<id>"
  risks:
    - "Root AWS credentials block deployment."
    - "Callback reconstruction may reveal an AWS product gap."
    - "GitNexus is stale and lacks PDG."
    - "Intentional #492 files are already dirty."
  assumptions:
    - "Re-verify SDK 2.2.1 APIs from local artifacts."
    - "Re-verify AWS identity before deployment and reject :root."
    - "Re-verify active callback discovery."
    - "Re-verify Quarkus named-handler packaging."
    - "Re-verify #492 dirty digests."
  open_questions:
    - "Which non-root role/session will run the proof?"
    - "Can repeated callback success be classified automatically?"
    - "Is callback discovery supported for the active lifetime?"
  avoid:
    - "Do not repeat full repository discovery."
    - "Do not refresh GitNexus without permission."
    - "Do not discard/overwrite stacked #492 work."
    - "Do not change PipelineControlPlane or semantic authority."
    - "Do not put callback identity in TPF Await state."
    - "Do not add Maven profiles or CI cloud deployment."
    - "Do not deploy with root credentials."
    - "Do not commit/push/open a PR without explicit instruction."
\`\`\`

## 12. Assumptions and Open Questions

Verify SDK 2.2.1 APIs, active callback discovery, Quarkus handler packaging, us-east-2 feature availability, and current #492 dirty digests before relying on them.

A non-root AWS role/session is required for deployed evidence. AWS confirmation may be required for duplicate/already-closed callback responses. If callback discovery is not a stable API, take that concrete gap to AWS rather than adding semantic duplication.

## 13. Definition of Done

- Issue #976 and this plan agree.
- Proof modules build in the canonical reactor without profiles/deployment.
- Real Durable handler, action hosts, binding/reconciliation, SAM stack, faults, and runner exist.
- #492 and root validation pass with the isolated Maven repository.
- Three clean deployed runs meet promotion gates, or a rejection report identifies the failing AWS capability.
- proof-report.json and AWS-ENGAGEMENT-BRIEF.md contain verified evidence without secrets.
- GitNexus detect_changes/review and CodeRabbit review complete.
- No commit/push/PR without explicit follow-up authorization.
