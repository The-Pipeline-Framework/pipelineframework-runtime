# Proposed canonical AWS Durable contract amendment

For framework-owner review; this is not a canonical documentation edit or a live
supported-profile claim. Based on merged runtime63 source
`a084c50e672645cde42b592b5415d218a8a24b73`.

1. Preserve the existing HARDENED deployment as default. Add RESTRICTED_PILOT as
   an explicit, fresh release-isolated, supervised FUNCTION profile in the same SAM
   artifact. It preserves all native semantic/recovery machinery and binary packages.
2. Qualify the already published SSM_STANDARD reader for deployment only when its
   two purpose-specific generation parameter ARNs use version 1, Standard SecureString,
   aws/ssm, retained exact runtime GetParameter access, no overwrite/delete/recreation,
   and owner-controlled retention of every existing release obligation. Account-level
   IAM authority replaces the hardened signing CMK boundary; the restricted security owner explicitly accepts it.
   Rotation creates new parameters, retains old references, and creates new numbered
   host versions and immutable manifests. A conditional profile name does not enforce
   the external policy or establish live IAM/key/tier observations.
3. Permit service-owned encryption on the callback table, Lambda environment/Durable
   state, log groups and non-EventBridge SQS queues, with external semantic tables and
   release/artifact buckets verified as AWS-owned / SSE-S3. Omit only their matching
   CMK grants. Preserve one real retained CMK for DueWorkQueue, DueWorkDlq and
   RepairFailureDlq, both EventBridge Rules and every delivery failure boundary.
4. Permit 64 CloudWatch alarm resources to be absent only in the restricted supervised
   promise. Existing telemetry/log emission, retries, DLQs and recovery stay intact.
   Operators must keep admission closed outside supervised windows and retain active
   failure-check/restoration evidence. Removing alarms does not remove EMF extraction
   cost or qualify unattended operation. An external exact-log-group extraction policy
   is a separate account-owner control, and raw logs remain evidence.
5. Version the deployment manifest to schema2, preserving all prior scope/publication,
   code/version, queue/mapping, encryption authority references. Add the explicit
   deployment, observation, schedule and encryption modes; ten actual log-group names;
   and a discriminated signing object. Hardened uses source SECRETS_MANAGER and exact
   `{arn,versionId}`. Restricted uses SSM_STANDARD with metadata/lifecycle and exact
   `{arn,version:1}`, retaining every old parameter ARN needed by pinned obligations.
6. Require Cloud consumers to bind the actual signed new deployment artifact and exact
   schema2 manifest to authentic CFN membership, publication and owner observation.
   Legacy published schema1 remains a valid hardened input where already supported;
   it cannot claim the new profile. No SDK code/config/version/alias/mapping split is
   introduced. Existing ownership/fault/restoration guards and live conformance catalogue
   remain binding and open.

Local candidate preparation is limited to conditional graph equivalence, configuration/IAM
consistency, source/version and retained-owner receipt denials, unchanged reader unit
controls and local packaging. Owner CI/system tests, canonical acceptance, signed
publication, authentic deployment, IAM/lifecycle enforcement and live fault evidence
are still required. The one CMK is a necessary nonzero baseline under current primary
service support evidence; no $0 idle result is claimed.
