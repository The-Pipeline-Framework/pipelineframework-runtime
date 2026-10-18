# Restricted pilot deployment profile (unpublished qualification)

The canonical `src/main/resources/aws-durable-queue-async.yaml` remains one SAM graph.
`DeploymentProfile=HARDENED` is the default. Its resolved resource graph matches
published runtime63 (`a084c50e672645cde42b592b5415d218a8a24b73`), including its
CMKs, Secrets Manager reads and 64 alarms. The explicit signing-source environment
setting selects the reader's existing default; the native host binaries are unchanged.

`RESTRICTED_PILOT` is a fresh, isolated, supervised FUNCTION deployment. It omits
alarm resources and uses SSE-SQS, DynamoDB AWS-owned encryption, default Lambda /
Durable / Logs encryption, and externally verified S3 SSE-S3 / semantic AWS-owned
resources. Logs, metrics emission, PITR, deletion protection, Streams, seven native
mappings, retry/DLQ limits, one-minute due-work rule, signing and release pins remain.
Operator checks and closed admissions outside supervised windows are owner obligations;
this profile does not establish unattended operations acceptance.

This is **not a zero fixed-cost profile**. `HostKmsKeyArn` remains mandatory for
`DueWorkQueue`, `DueWorkDlq` and `RepairFailureDlq`: EventBridge Rules delivery and
its target DLQs lack affirmative SSE-SQS qualification. Both existing Rules remain
unchanged (due-work rate one minute, body `{"kind":"tpf-due-work-tick"}`, 10 retries /
3,600 seconds). The same externally owned retained CMK encrypts those three queues.
Only due-work receive and wake-up repair-failure delivery retain runtime CMK grants,
constrained through SQS and each exact queue encryption context. The key owner must
also authorize EventBridge Rules delivery with the exact Rule/source and queue
contexts. Shared CMK reuse requires its actual owner's approval; no key is presumed
to exist. Rotation, KMS requests, retained data and usage can add costs.

Removing alarms does not stop native EMF metric extraction. Raw log evidence and
host telemetry remain. A separately owned, authenticated, exact-log-group CloudWatch
Logs metric extraction policy may control that charge; this graph does not change
an account-wide policy. Free-tier allowance and actual idle traffic are unverified.

## Input and immutable signing contract

Nine added parameters bring the canonical parameter count to 73:

| Parameter | HARDENED | RESTRICTED_PILOT |
| --- | --- | --- |
| DeploymentProfile | HARDENED (default) | RESTRICTED_PILOT |
| RestrictedPilotValidation | not-applicable | validated |
| SigningSource | SECRETS_MANAGER (default) | SSM_STANDARD |
| SigningParameterLifecycleValidation | not-applicable | validated |
| ResumeTokenParameterArn | empty | exact purpose/generation ARN |
| ResumeTokenParameterVersion | empty | `1` |
| WorkerSharedParameterArn | empty | exact purpose/generation ARN |
| WorkerSharedParameterVersion | empty | `1` |
| SigningParameterAccessArns | empty list element | current + all retained exact ARNs |

Restricted requires `ReleaseIsolationMode=FRESH_RELEASE_ISOLATED_STACK` and
`TransitionWorkerPlacement=FUNCTION`. Supply explicit empty AlarmTopicArn (no SNS dependency), empty secret ARN/version/signing CMK
inputs, empty signing-secret/CMK and semantic-CMK access list elements. Mixed sources,
mutable versions and incompatible selectors fail closed in template Rules and preflight.
`HostKmsKeyArn` remains a real retained key ARN. The old deployment, callback and
execution-recovery validation attestations still apply.

SSM paths must be `/tpf/durable/<scope>/resume-token/g-<32..64 hex>` and
`/tpf/durable/<scope>/worker/g-<32..64 hex>`. Each is a Standard SecureString encrypted
by `aws/ssm`, created once at version 1. Rotation creates new purpose/generation
parameters alongside the old generation. Never overwrite or delete a retained
parameter: SSM's limited version history is not a retention mechanism. Retain
parameter identity, version, bytes, access, native packages and execution pins until
the owner proves every referencing obligation drained. Do not recreate a deleted
parameter under its old name. No parameter value is placed in Terraform/Git/logs.

Runtime roles get only `ssm:GetParameter` for exact current/retained ARNs. They get
no history, path, list, write or delete grant. The owner must prevent overwrite and
delete, restrict history/path reads (including ancestor paths), and retain existing release identity/version and retirement evidence. `aws/ssm` has account-wide decrypt
authority: isolation depends on Parameter Store IAM. It is not equivalent to a
customer-managed cryptographic boundary. The restricted security attestation accepts
that explicit change; deployment inputs alone do not install the owner policy.

The existing reader requests exact ARN plus numeric version with decryption and
rejects wrong identity, version, type, oversized values and mixed source settings.
The new profile uses that published reader unchanged. Its local reader tests cannot
prove a live account's IAM, parameter tier, encryption key or retention controls.

## Offline checks and honest owner receipts

Install `requirements.txt` in an isolated environment. Run:

```sh
python -m unittest discover -s aws-durable-coordination/deployment/profiles -v
python aws-durable-coordination/deployment/profiles/preflight.py \
  --parameters /absolute/path/owner-native-parameters.json \
  --signing-inventory /absolute/path/owner-signing-observations.json \
  --retention-guard /absolute/path/owner-retention-guard-receipt.json
```

Parameters are an object keyed by canonical SAM parameter names. Inventory keys
exactly cover `SigningParameterAccessArns`; each value is:

```json
{"arn":"<same exact key ARN>","version":1,"type":"SecureString","tier":"Standard","keyKind":"AWS_MANAGED_SSM"}
```

The retention guard receipt is:

```json
{"schemaVersion":1,"parameterArns":["<sorted exact retained parameter ARNs>"],"denyOverwrite":true,"denyDelete":true,"denyHistoryAndPathReads":true,"retainUntilDrained":true,"iamOnlyAccountAuthorityAccepted":true}
```

These are claims supplied by the resource owner, requiring authentic observation,
policy evaluation and retained-work evidence before admission. The script only
checks their consistency. It performs no AWS requests, IAM enforcement or resource
creation and grants no approval to spend. Cloud remains the deployment/input/lifecycle
owner. Preserve genuine stack ownership and fault restoration checks.

## Output contract and publication gap

The schema2 release manifest retains every scope, immutable version, queue, mapping,
code digest and publication identity. It adds `deploymentProfile`, `observability`,
`scheduleProvider=EVENTBRIDGE_RULE`, `encryption.host` (CUSTOMER_MANAGED or
SERVICE_OWNED_EXCEPT_EVENTBRIDGE_CMK), `encryption.semantic` (CUSTOMER_MANAGED or
AWS_OWNED_AND_S3_MANAGED), and ten actual `logGroupNames` resource references.
Signing is discriminated by `source`. SSM includes `tier=Standard`, `type=SecureString`,
`keyKind=AWS_MANAGED_SSM`, `lifecycle=IMMUTABLE_GENERATION`, resumeToken/worker
`{arn,version:1}` and `retainedParameterAccessArns`. Hardened retains its original
`{arn,versionId}` / CMK fields with `source=SECRETS_MANAGER` added.

This template/preflight/schema2 extension is uncommitted and unpublished. Existing
public runtime63 deployment bytes do not contain it; copying a profile name into
published inputs does not enable it. The unchanged host JAR already contains the
reader. Canonical documentation, downstream authenticated manifest consumers,
owner CI/system tests, publication and live AWS conformance remain separate gates.
