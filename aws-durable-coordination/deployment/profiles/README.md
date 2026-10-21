> **20 October deployment-only boundary amendment (unpublished):** the new optional
> `RuntimePermissionsBoundaryArn` raises this candidate template from 73 to 74
> parameters. The signed runtime64 73-parameter artifact remains an unchanged legacy
> contract; no deployment may substitute these unpublished bytes for that pin.

# Restricted pilot deployment profile

The canonical `src/main/resources/aws-durable-queue-async.yaml` remains one SAM graph.
Publish its literal YAML resource structures: CloudFormation rejects YAML aliases
and merge keys. Serialization tests retain the complete parsed runtime66 contract
and every profile/signing/transition-placement/boundary condition combination.
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

The existing profile/preflight/schema2 baseline was published by runtime64
(`931196a7bf7aa1102129da98f5344c5fd7b82f7b`), with 73 parameters and template SHA256
`5fee5ee1b7afa687903deaf19b9f10b4f6ad02c85cf6afd2cd91d3ca4414a3bc`.
Only the new 74-parameter permissions-boundary amendment below is unpublished;
copying its parameter into the old signed artifact does not enable it. The host
JAR is unchanged. Matching canonical documentation/consumers, owner/downstream CI,
normal signed publication and live AWS conformance remain distinct gates.

## Optional runtime permissions boundary

`RuntimePermissionsBoundaryArn` defaults to an empty string. Empty retains the
published hardened/restricted role graph exactly; it does not impose a boundary
on generic customers. A nonempty exact same-account customer-managed policy ARN
sets `Properties.PermissionsBoundary` on all six explicit roles: DueWork, Durable,
Ingress, Action, Worker and Wakeup. No role/queue/function/consumer is added, and
trust, inline/managed policies, runtime handlers, binaries, recovery and retention
are unchanged. The schema2 release manifest fields remain unchanged. Cloud's pilot
wrapper must separately require its approved boundary and verify actual GetRole
boundary ARNs for all six roles against the new authenticated deployment pin.

An execution-role boundary is not inherited by roles created through CloudFormation.
A platform policy requiring `iam:PermissionsBoundary` on CreateRole needs this
explicit resource property; CloudFormation/SAM does not inject a default. Owners
must retain the managed boundary policy and effective rights while any retained
role/execution/replay/redrive obligation needs them, and prevent unauthorized policy
version changes or boundary removal/replacement. This parameter supplies neither
IAM permission nor lifecycle enforcement. Do not deploy a boundary that denies a
required native recovery action or grants cross-application authority.

The offline preflight accepts optional `--account-id <owner-observed-account>` and
then rejects a boundary from another account. Without that observation it validates
syntax only; it does not infer the Lambda role account from a possibly separately
owned KMS key, call AWS, inspect policy versions/effective rights or claim lifecycle
verification. Actual account/partition, GetPolicy/default version, all role boundaries,
retained-policy authority and runtime recovery permissions remain deployment-owner
checks before admission. The ARN is a policy identity, not a secret or a version pin.

This is a provisioning-policy extension requiring owner review, canonical parameter
reference alignment, normal owner/downstream CI and signed publication before use.
The prior 73-parameter contract stays pinned for legacy deployments; no local source
fallback or Cloud-side mutation of a signed template is supported.

## SAM transform processing

The amended template runs `AWS::LanguageExtensions` before
`AWS::Serverless-2016-10-31`. Each SAM function expresses the existing
`ActionArtifactUri` as `!Sub '${ActionArtifactUri}'`, so the first transform
resolves the entire S3 URI before SAM parses its bucket and complete object key.
The `Fn::Sub` form also retains a resolvable CodeUri for the conditional transition
function when COMPUTE placement disables that function. All 74 parameters and
their existing values remain valid; no new code package, handler, consumer or
recovery mechanism is introduced.

The deployment principal must be allowed to use both exact AWS-owned transforms.
For stack updates, provide the original template rather than a previously
processed template: LanguageExtensions resolves parameter values during processing.
The offline tests pin AWS SAM CLI 1.169.0 and SAM Translator 1.115.0 and exercise
both official local stages across all 16 profile/signing/placement/boundary variants.
They reproduce the previous eight CodeUri rejections and compare the resulting
active graph against the intended original code locations, including nested keys.
These local processors do not execute the AWS-hosted macros. AWS ValidateTemplate
and GetTemplateSummary alone also do not establish transform success; an owner
must inspect an AWS change-set preview before executing a newly qualified asset.

AWS references: [CodeUri](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-resource-function.html#sam-function-codeuri),
[language extensions](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-specification-language-extensions.html),
and [transform ordering and parameter resolution](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/transform-aws-languageextensions.html).
