package org.pipelineframework.orchestrator.release;

import java.net.URI;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import jakarta.annotation.PreDestroy;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/**
 * DynamoDB-backed release registry that stores immutable release and activation records.
 *
 * <p>Activation head v1 requires a coordinated, quiesced reader/writer rollout: older binaries neither
 * fence their writes nor read this head. Before the first head write, reads retain the legacy
 * timestamp/Release ordering of unchanged legacy history. The first new write starts a separate
 * ordered-event sequence; it does not rewrite history or manufacture receipts for previous activations.
 * All activation writers in this binary, including the legacy API, then use the common CAS head.
 * Rolling back to an older reader/writer after new writes is unsafe without a separately planned
 * migration. Actual activation time is retained independently of sequence and never synthesized.
 * Operation receipts have no automatic expiry or key reuse; inquiry never mutates any registry row.</p>
 */
public class DynamoPipelineReleaseRegistry implements PipelineReleaseRegistry {

    @Override
    public boolean supportsCurrentActivationObservation() { return true; }

    @Override
    public Uni<CurrentActivationObservation> currentActivationObservation(String tenant, String pipeline) {
        return blocking(() -> {
            var head = readActivationHead(tenant, pipeline);
            if (head.isEmpty()) {
                return new CurrentActivationObservation(1, tenant, pipeline, legacyLatestActivation(tenant, pipeline).isPresent()
                    ? CurrentActivationObservation.State.LEGACY_UNKNOWN : CurrentActivationObservation.State.NONE, Optional.empty());
            }
            validateHead(head, tenant, pipeline);
            var release = getReleaseRecord(tenant, pipeline, stringValue(head, RELEASE_VERSION), Optional.empty())
                .orElseThrow(() -> new IllegalStateException("Current activation lacks immutable registered Release"));
            return new CurrentActivationObservation(1, tenant, pipeline, CurrentActivationObservation.State.IDENTIFIED,
                Optional.of(new CurrentActivationEvent(stringValue(head, ACTIVATION_ID), release.contractVersion(), release.releaseVersion(),
                    PipelineReleaseEvidence.from(release), longValue(head, ACTIVATED_AT_EPOCH_MS))));
        });
    }
    private static final Logger LOG = Logger.getLogger(DynamoPipelineReleaseRegistry.class);

    private static final String REGISTRY_KEY = "registry_key";
    private static final String REGISTRY_SORT = "registry_sort";
    private static final String RECORD_TYPE = "record_type";
    private static final String RECORD_TYPE_RELEASE = "RELEASE";
    private static final String RECORD_TYPE_ACTIVATION = "ACTIVATION";
    private static final String RELEASE_PREFIX = "release:";
    private static final String ACTIVATION_PREFIX = "activation:";
    private static final String HEAD_SORT = "activation-head:v1";
    private static final String HEAD_TYPE = "ACTIVATION_HEAD_V1";
    private static final String OPERATION_TYPE = "ACTIVATION_OPERATION_V1";
    private static final String SEQUENCE = "activation_sequence";
    private static final String RECEIPT_JSON = "receipt_json";
    private static final String ACTIVATION_ID = "activation_id";
    private static final String TENANT_ID = "tenant_id";
    private static final String PIPELINE_ID = "pipeline_id";
    private static final String CONTRACT_VERSION = "contract_version";
    private static final String RELEASE_VERSION = "release_version";
    private static final String PRIMARY_ARTIFACT_ID = "primary_artifact_id";
    private static final String PRIMARY_ARTIFACT_DIGEST = "primary_artifact_digest";
    private static final String PRIMARY_ARTIFACT_URI = "primary_artifact_uri";
    private static final String LEGACY_PRIMARY_ARTIFACT_PATH = "primary_artifact_path";
    private static final String PRIMARY_ARTIFACT_SIZE_BYTES = "primary_artifact_size_bytes";
    private static final String PRIMARY_ARTIFACT_CHECKSUM = "primary_artifact_checksum";
    private static final String DESCRIPTOR_JSON = "descriptor_json";
    private static final String CONTRACT_JSON = "contract_json";
    private static final String CREATED_AT_EPOCH_MS = "created_at_epoch_ms";
    private static final String UPDATED_AT_EPOCH_MS = "updated_at_epoch_ms";
    private static final String ACTIVATED_AT_EPOCH_MS = "activated_at_epoch_ms";

    private final PipelineOrchestratorConfig explicitConfig;
    private volatile PipelineOrchestratorConfig orchestratorConfig;
    private volatile DynamoDbClient client;

    public DynamoPipelineReleaseRegistry() {
        this.explicitConfig = null;
    }

    public DynamoPipelineReleaseRegistry(PipelineOrchestratorConfig orchestratorConfig) {
        this.explicitConfig = orchestratorConfig;
        this.orchestratorConfig = orchestratorConfig;
    }

    DynamoPipelineReleaseRegistry(DynamoDbClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this.explicitConfig = orchestratorConfig;
        this.orchestratorConfig = orchestratorConfig;
        this.client = client;
    }

    @Override
    public Uni<PipelineReleaseRecord> register(PipelineReleaseRecord record) {
        return blocking(() -> registerBlocking(record));
    }

    @Override
    public Uni<List<PipelineReleaseRecord>> list(String tenantId, String pipelineId) {
        return blocking(() -> listBlocking(tenantId, pipelineId));
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> get(String tenantId, String pipelineId, String releaseVersion) {
        return blocking(() -> getBlocking(tenantId, pipelineId, releaseVersion));
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> active(String tenantId, String pipelineId) {
        return blocking(() -> activeBlocking(tenantId, pipelineId));
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> activate(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        long nowEpochMs) {
        return blocking(() -> activateBlocking(tenantId, pipelineId, releaseVersion, nowEpochMs));
    }

    @Override
    public boolean supportsActivationOperations() {
        return true;
    }

    @Override
    public Uni<ActivationOperationReceipt> activateOnce(ActivationOperationCommand command) {
        return blocking(() -> {
            PipelineReleaseRecord release = command.release();
            Optional<ActivationOperationReceipt> existing = operationBlocking(
                release.tenantId(), release.pipelineId(), command.operationKey());
            if (existing.isPresent()) {
                existing.get().requireIntent(command);
                return existing.get();
            }
            PipelineReleaseRecord registered = getReleaseRecord(release.tenantId(), release.pipelineId(),
                release.releaseVersion(), Optional.empty()).orElseThrow(() ->
                    new IllegalStateException("Activation requires a registered Release"));
            if (!PipelineReleaseRecordMetadata.sameImmutableMetadata(registered, release)) {
                throw new IllegalStateException("Activation requires matching immutable registered metadata");
            }
            ActivationOperationReceipt receipt = new ActivationOperationReceipt(1, command.operationKey(),
                UUID.randomUUID().toString(), release.tenantId(), release.pipelineId(), release.contractVersion(),
                release.releaseVersion(), PipelineReleaseEvidence.from(registered), command.activatedAtEpochMs());
            return writeActivation(registered, command.activatedAtEpochMs(), Optional.of(receipt)).orElseThrow();
        });
    }

    @Override
    public Uni<Optional<ActivationOperationReceipt>> getActivationOperation(
        String tenantId, String pipelineId, String operationKey) {
        return blocking(() -> operationBlocking(tenantId, pipelineId, operationKey));
    }

    @PreDestroy
    void closeClient() {
        DynamoDbClient active = client;
        if (active == null) {
            return;
        }
        synchronized (this) {
            active = client;
            if (active == null) {
                return;
            }
            try {
                active.close();
            } catch (Exception e) {
                LOG.debug("Failed closing DynamoDB client during shutdown.", e);
            } finally {
                client = null;
            }
        }
    }

    private PipelineReleaseRecord registerBlocking(PipelineReleaseRecord record) {
        Optional<PipelineReleaseRecord> existing = getBlocking(
            record.tenantId(),
            record.pipelineId(),
            record.releaseVersion());
        if (existing.isPresent()) {
            PipelineReleaseRecord current = existing.get();
            if (!PipelineReleaseRecordMetadata.sameImmutableMetadata(current, record)) {
                throw new IllegalStateException("Release version is already registered with different metadata");
            }
            return current;
        }
        try {
            dynamoClient().putItem(PutItemRequest.builder()
                .tableName(releaseTable())
                .item(toReleaseItem(record))
                .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT))
                .build());
            return record;
        } catch (ConditionalCheckFailedException ignored) {
            PipelineReleaseRecord current = getBlocking(record.tenantId(), record.pipelineId(), record.releaseVersion())
                .orElseThrow(() -> new IllegalStateException("Release registration lost race but no record was found"));
            if (!PipelineReleaseRecordMetadata.sameImmutableMetadata(current, record)) {
                throw new IllegalStateException("Release version is already registered with different metadata");
            }
            return current;
        }
    }

    private List<PipelineReleaseRecord> listBlocking(String tenantId, String pipelineId) {
        Optional<ActivationEvent> active = latestActivation(tenantId, pipelineId);
        return queryRecords(tenantId, pipelineId, RELEASE_PREFIX, true).items().stream()
            .filter(item -> RECORD_TYPE_RELEASE.equals(stringValue(item, RECORD_TYPE)))
            .map(item -> toReleaseRecord(item, active))
            .sorted(Comparator.comparingLong(PipelineReleaseRecord::createdAtEpochMs))
            .toList();
    }

    private Optional<PipelineReleaseRecord> getBlocking(String tenantId, String pipelineId, String releaseVersion) {
        return getReleaseRecord(tenantId, pipelineId, releaseVersion, latestActivation(tenantId, pipelineId));
    }

    private Optional<PipelineReleaseRecord> getReleaseRecord(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        Optional<ActivationEvent> active) {
        Map<String, AttributeValue> item = dynamoClient().getItem(GetItemRequest.builder()
            .tableName(releaseTable())
            .key(key(tenantId, pipelineId, releaseSort(releaseVersion)))
            .consistentRead(true)
            .build()).item();
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        if (!tenantId.equals(stringValue(item, TENANT_ID)) || !pipelineId.equals(stringValue(item, PIPELINE_ID))
            || !releaseVersion.equals(stringValue(item, RELEASE_VERSION))) {
            throw new IllegalStateException("Registered Release scope mismatch");
        }
        return Optional.of(toReleaseRecord(item, active));
    }

    private Optional<PipelineReleaseRecord> activeBlocking(String tenantId, String pipelineId) {
        Optional<ActivationEvent> activation = latestActivation(tenantId, pipelineId);
        if (activation.isEmpty()) {
            return Optional.empty();
        }
        return getReleaseRecord(tenantId, pipelineId, activation.get().releaseVersion(), activation)
            .map(record -> record.withStatus(PipelineReleaseStatus.ACTIVE, activation.get().activatedAtEpochMs()));
    }

    private Optional<PipelineReleaseRecord> activateBlocking(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        long nowEpochMs) {
        Optional<PipelineReleaseRecord> existing = getReleaseRecord(tenantId, pipelineId, releaseVersion, Optional.empty());
        if (existing.isEmpty()) {
            return Optional.empty();
        }
        writeActivation(existing.get(), nowEpochMs, Optional.empty());
        return Optional.of(existing.get().withStatus(PipelineReleaseStatus.ACTIVE, nowEpochMs));
    }

    private Optional<ActivationEvent> latestActivation(String tenantId, String pipelineId) {
        Map<String, AttributeValue> head = readActivationHead(tenantId, pipelineId);
        if (!head.isEmpty()) {
            validateHead(head, tenantId, pipelineId);
            return Optional.of(new ActivationEvent(stringValue(head, RELEASE_VERSION), longValue(head, ACTIVATED_AT_EPOCH_MS)));
        }
        return legacyLatestActivation(tenantId, pipelineId);
    }

    private Optional<ActivationEvent> legacyLatestActivation(String tenantId, String pipelineId) {
        return queryRecords(tenantId, pipelineId, ACTIVATION_PREFIX, false).items().stream()
            .filter(item -> RECORD_TYPE_ACTIVATION.equals(stringValue(item, RECORD_TYPE)))
            .findFirst()
            .map(item -> new ActivationEvent(
                stringValue(item, RELEASE_VERSION),
                longValue(item, ACTIVATED_AT_EPOCH_MS)));
    }

    private Optional<ActivationOperationReceipt> writeActivation(PipelineReleaseRecord release, long now,
        Optional<ActivationOperationReceipt> receipt) {
        String tenant = release.tenantId();
        String pipeline = release.pipelineId();
        ActivationEvent event = new ActivationEvent(release.releaseVersion(), now);
        String activationId = receipt.map(ActivationOperationReceipt::activationId).orElseGet(() -> UUID.randomUUID().toString());
        for (int attempt = 0; attempt < 32; attempt++) {
            Map<String, AttributeValue> previous = readActivationHead(tenant, pipeline);
            long sequence = 1;
            if (!previous.isEmpty()) {
                validateHead(previous, tenant, pipeline);
                sequence = Math.addExact(longValue(previous, SEQUENCE), 1);
            } else {
                // Retained legacy history is read-only. Older writers must be quiesced before first new write.
                legacyLatestActivation(tenant, pipeline);
            }
            Map<String, AttributeValue> nextHead = new HashMap<>(toActivationItem(tenant, pipeline, event));
            nextHead.put(REGISTRY_SORT, avS(HEAD_SORT));
            nextHead.put(RECORD_TYPE, avS(HEAD_TYPE));
            nextHead.put(SEQUENCE, avN(sequence));
            nextHead.put(ACTIVATION_ID, avS(activationId));
            Map<String, AttributeValue> orderedEvent = new HashMap<>(toActivationItem(tenant, pipeline, event));
            orderedEvent.put(REGISTRY_SORT, avS("activation-v2:" + String.format("%019d", sequence)));
            orderedEvent.put(SEQUENCE, avN(sequence));
            orderedEvent.put(ACTIVATION_ID, avS(activationId));
            List<TransactWriteItem> writes = new ArrayList<>();
            writes.add(TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(releaseTable())
                .key(key(tenant, pipeline, releaseSort(release.releaseVersion())))
                .conditionExpression("attribute_exists(#pk) AND attribute_exists(#sk) AND #descriptor = :descriptor AND #contract = :contract AND #digest = :digest AND (#uri = :uri OR primary_artifact_path = :uri) AND #size = :size AND #checksum = :checksum AND #artifact = :artifact AND #contractVersion = :contractVersion")
                .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT,
                    "#descriptor", DESCRIPTOR_JSON, "#contract", CONTRACT_JSON, "#digest", PRIMARY_ARTIFACT_DIGEST,
                    "#uri", PRIMARY_ARTIFACT_URI, "#size", PRIMARY_ARTIFACT_SIZE_BYTES, "#checksum", PRIMARY_ARTIFACT_CHECKSUM,
                    "#artifact", PRIMARY_ARTIFACT_ID, "#contractVersion", CONTRACT_VERSION))
                .expressionAttributeValues(Map.of(":descriptor", avS(toJson(release.descriptor())),
                    ":contract", avS(toJson(release.contract())), ":digest", avS(release.primaryArtifactDigest()),
                    ":uri", avS(release.primaryArtifactUri()), ":size", avN(release.primaryArtifactSizeBytes()),
                    ":checksum", avS(release.primaryArtifactChecksum()), ":artifact", avS(release.primaryArtifactId()),
                    ":contractVersion", avS(release.contractVersion())))
                .build()).build());
            writes.add(absentPut(orderedEvent));
            Put.Builder headPut = Put.builder().tableName(releaseTable()).item(nextHead);
            if (previous.isEmpty()) {
                headPut.conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                    .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT));
            } else {
                headPut.conditionExpression("#sequence = :previous AND #type = :type")
                    .expressionAttributeNames(Map.of("#sequence", SEQUENCE, "#type", RECORD_TYPE))
                    .expressionAttributeValues(Map.of(":previous", avN(sequence - 1), ":type", avS(HEAD_TYPE)));
            }
            writes.add(TransactWriteItem.builder().put(headPut.build()).build());
            receipt.ifPresent(value -> writes.add(absentPut(Map.of(
                REGISTRY_KEY, avS(partitionKey(tenant, pipeline)), REGISTRY_SORT, avS(operationSort(tenant, pipeline, value.operationKey())),
                RECORD_TYPE, avS(OPERATION_TYPE), RECEIPT_JSON, avS(toJson(value))))));
            try {
                dynamoClient().transactWriteItems(TransactWriteItemsRequest.builder().transactItems(writes).build());
                return receipt;
            } catch (TransactionCanceledException conflict) {
                if (receipt.isPresent()) {
                    Optional<ActivationOperationReceipt> winner = operationBlocking(tenant, pipeline, receipt.get().operationKey());
                    if (winner.isPresent()) {
                        winner.get().requireIntent(new ActivationOperationCommand(receipt.get().operationKey(), release, now));
                        return winner;
                    }
                }
                Map<String, AttributeValue> current = readItem(tenant, pipeline, HEAD_SORT);
                if (current.equals(previous)) {
                    throw conflict;
                }
            }
        }
        throw new IllegalStateException("Activation head remained contended; no effect inferred");
    }

    private TransactWriteItem absentPut(Map<String, AttributeValue> item) {
        return TransactWriteItem.builder().put(Put.builder().tableName(releaseTable()).item(item)
            .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
            .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT)).build()).build();
    }

    private Map<String, AttributeValue> readItem(String tenant, String pipeline, String sort) {
        return dynamoClient().getItem(GetItemRequest.builder().tableName(releaseTable())
            .key(key(tenant, pipeline, sort)).consistentRead(true).build()).item();
    }

    private Map<String, AttributeValue> readActivationHead(String tenant, String pipeline) {
        Map<String, AttributeValue> head = readItem(tenant, pipeline, HEAD_SORT);
        if (head.isEmpty() && (hasNamespaceEvidence(tenant, pipeline, "activation-v2:")
            || hasNamespaceEvidence(tenant, pipeline, "activation-operation:v1:"))) {
            // A legitimate concurrent first commit may have appeared after the initial absent read.
            head = readItem(tenant, pipeline, HEAD_SORT);
            if (head.isEmpty()) {
                throw new IllegalStateException("Retained activation evidence exists without its ordered head");
            }
        }
        return head;
    }

    private boolean hasNamespaceEvidence(String tenant, String pipeline, String prefix) {
        return !dynamoClient().query(QueryRequest.builder().tableName(releaseTable())
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :prefix)")
            .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT))
            .expressionAttributeValues(Map.of(":pk", avS(partitionKey(tenant, pipeline)), ":prefix", avS(prefix)))
            .consistentRead(true).limit(1).build()).items().isEmpty();
    }

    private Optional<ActivationOperationReceipt> operationBlocking(String tenant, String pipeline, String operationKey) {
        Map<String, AttributeValue> item = readItem(tenant, pipeline, operationSort(tenant, pipeline, operationKey));
        if (item.isEmpty()) {
            return Optional.empty();
        }
        if (!OPERATION_TYPE.equals(stringValue(item, RECORD_TYPE))) {
            throw new IllegalStateException("Unknown activation operation record type");
        }
        ActivationOperationReceipt receipt = fromJson(stringValue(item, RECEIPT_JSON), ActivationOperationReceipt.class);
        if (!receipt.tenantId().equals(tenant) || !receipt.pipelineId().equals(pipeline) || !receipt.operationKey().equals(operationKey)) {
            throw new IllegalStateException("Activation operation scope mismatch");
        }
        return Optional.of(receipt);
    }

    private static String operationSort(String tenant, String pipeline, String operationKey) {
        if (operationKey == null || operationKey.isBlank()) {
            throw new IllegalArgumentException("operationKey must not be blank");
        }
        try {
            byte[] bytes = PipelineJson.mapper().writeValueAsBytes(List.of(tenant, pipeline, operationKey));
            return "activation-operation:v1:" + java.util.HexFormat.of().formatHex(
                java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception failure) {
            throw new IllegalArgumentException("Invalid activation operation scope", failure);
        }
    }

    private void validateHead(Map<String, AttributeValue> head, String tenant, String pipeline) {
        if (!HEAD_TYPE.equals(stringValue(head, RECORD_TYPE)) || !tenant.equals(stringValue(head, TENANT_ID))
            || !pipeline.equals(stringValue(head, PIPELINE_ID)) || longValue(head, SEQUENCE) <= 0) {
            throw new IllegalStateException("Unknown or corrupt activation head");
        }
        Map<String, AttributeValue> event = readItem(tenant, pipeline,
            "activation-v2:" + String.format("%019d", longValue(head, SEQUENCE)));
        if (event.isEmpty() || !RECORD_TYPE_ACTIVATION.equals(stringValue(event, RECORD_TYPE))
            || !tenant.equals(stringValue(event, TENANT_ID)) || !pipeline.equals(stringValue(event, PIPELINE_ID))
            || !stringValue(head, RELEASE_VERSION).equals(stringValue(event, RELEASE_VERSION))
            || !stringValue(head, ACTIVATION_ID).equals(stringValue(event, ACTIVATION_ID))
            || longValue(head, ACTIVATED_AT_EPOCH_MS) != longValue(event, ACTIVATED_AT_EPOCH_MS)
            || longValue(head, SEQUENCE) != longValue(event, SEQUENCE)) {
            throw new IllegalStateException("Activation head lacks matching immutable event");
        }
    }

    private QueryResponse queryRecords(
        String tenantId,
        String pipelineId,
        String sortPrefix,
        boolean ascending) {
        return dynamoClient().query(QueryRequest.builder()
            .tableName(releaseTable())
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :prefix)")
            .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT))
            .expressionAttributeValues(Map.of(
                ":pk", avS(partitionKey(tenantId, pipelineId)),
                ":prefix", avS(sortPrefix)))
            .scanIndexForward(ascending)
            .consistentRead(true)
            .build());
    }

    private PipelineReleaseRecord toReleaseRecord(
        Map<String, AttributeValue> item,
        Optional<ActivationEvent> active) {
        String releaseVersion = stringValue(item, RELEASE_VERSION);
        long activatedAt = active
            .filter(event -> event.releaseVersion().equals(releaseVersion))
            .map(ActivationEvent::activatedAtEpochMs)
            .orElse(0L);
        PipelineReleaseStatus status = activatedAt > 0L
            ? PipelineReleaseStatus.ACTIVE
            : PipelineReleaseStatus.REGISTERED;
        return new PipelineReleaseRecord(
            stringValue(item, TENANT_ID),
            stringValue(item, PIPELINE_ID),
            stringValue(item, CONTRACT_VERSION),
            releaseVersion,
            status,
            fromJson(stringValue(item, DESCRIPTOR_JSON), PipelineReleaseDescriptor.class),
            stringValue(item, PRIMARY_ARTIFACT_ID),
            stringValue(item, PRIMARY_ARTIFACT_DIGEST),
            artifactUriValue(item),
            longValue(item, PRIMARY_ARTIFACT_SIZE_BYTES),
            stringValue(item, PRIMARY_ARTIFACT_CHECKSUM),
            fromJson(stringValue(item, CONTRACT_JSON), PipelineContractDescriptor.class),
            longValue(item, CREATED_AT_EPOCH_MS),
            longValue(item, UPDATED_AT_EPOCH_MS),
            activatedAt);
    }

    private Map<String, AttributeValue> toReleaseItem(PipelineReleaseRecord record) {
        return Map.ofEntries(
            Map.entry(REGISTRY_KEY, avS(partitionKey(record.tenantId(), record.pipelineId()))),
            Map.entry(REGISTRY_SORT, avS(releaseSort(record.releaseVersion()))),
            Map.entry(RECORD_TYPE, avS(RECORD_TYPE_RELEASE)),
            Map.entry(TENANT_ID, avS(record.tenantId())),
            Map.entry(PIPELINE_ID, avS(record.pipelineId())),
            Map.entry(CONTRACT_VERSION, avS(record.contractVersion())),
            Map.entry(RELEASE_VERSION, avS(record.releaseVersion())),
            Map.entry(PRIMARY_ARTIFACT_ID, avS(record.primaryArtifactId())),
            Map.entry(PRIMARY_ARTIFACT_DIGEST, avS(record.primaryArtifactDigest())),
            Map.entry(PRIMARY_ARTIFACT_URI, avS(record.primaryArtifactUri())),
            Map.entry(PRIMARY_ARTIFACT_SIZE_BYTES, avN(record.primaryArtifactSizeBytes())),
            Map.entry(PRIMARY_ARTIFACT_CHECKSUM, avS(record.primaryArtifactChecksum())),
            Map.entry(DESCRIPTOR_JSON, avS(toJson(record.descriptor()))),
            Map.entry(CONTRACT_JSON, avS(toJson(record.contract()))),
            Map.entry(CREATED_AT_EPOCH_MS, avN(record.createdAtEpochMs())),
            Map.entry(UPDATED_AT_EPOCH_MS, avN(record.updatedAtEpochMs())),
            Map.entry(ACTIVATED_AT_EPOCH_MS, avN(record.activatedAtEpochMs())));
    }

    private Map<String, AttributeValue> toActivationItem(
        String tenantId,
        String pipelineId,
        ActivationEvent event) {
        return Map.ofEntries(
            Map.entry(REGISTRY_KEY, avS(partitionKey(tenantId, pipelineId))),
            Map.entry(REGISTRY_SORT, avS(activationSort(event))),
            Map.entry(RECORD_TYPE, avS(RECORD_TYPE_ACTIVATION)),
            Map.entry(TENANT_ID, avS(tenantId)),
            Map.entry(PIPELINE_ID, avS(pipelineId)),
            Map.entry(RELEASE_VERSION, avS(event.releaseVersion())),
            Map.entry(ACTIVATED_AT_EPOCH_MS, avN(event.activatedAtEpochMs())));
    }

    private DynamoDbClient dynamoClient() {
        DynamoDbClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            active = client;
            if (active == null) {
                active = newClient(config());
                client = active;
            }
            return active;
        }
    }

    private PipelineOrchestratorConfig config() {
        PipelineOrchestratorConfig active = orchestratorConfig;
        if (active != null) {
            return active;
        }
        if (explicitConfig != null) {
            return explicitConfig;
        }
        throw new IllegalStateException("Dynamo release registry requires PipelineOrchestratorConfig");
    }

    private String releaseTable() {
        PipelineOrchestratorConfig.DynamoConfig dynamo = config().dynamo();
        if (dynamo == null || dynamo.releaseTable() == null || dynamo.releaseTable().isBlank()) {
            throw new IllegalStateException("pipeline.orchestrator.dynamo.release-table must not be blank");
        }
        return dynamo.releaseTable();
    }

    private static DynamoDbClient newClient(PipelineOrchestratorConfig config) {
        PipelineOrchestratorConfig.DynamoConfig dynamo = config.dynamo();
        if (dynamo == null) {
            throw new IllegalStateException("Dynamo release registry requires pipeline.orchestrator.dynamo.* configuration");
        }
        var builder = DynamoDbClient.builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder());
        dynamo.region().filter(value -> !value.isBlank())
            .map(Region::of)
            .ifPresent(builder::region);
        dynamo.endpointOverride().filter(value -> !value.isBlank())
            .map(URI::create)
            .ifPresent(builder::endpointOverride);
        return builder.build();
    }

    private static Map<String, AttributeValue> key(String tenantId, String pipelineId, String sortKey) {
        return Map.of(
            REGISTRY_KEY, avS(partitionKey(tenantId, pipelineId)),
            REGISTRY_SORT, avS(sortKey));
    }

    private static String partitionKey(String tenantId, String pipelineId) {
        return tenantId + "#" + pipelineId;
    }

    private static String releaseSort(String releaseVersion) {
        return RELEASE_PREFIX + releaseVersion;
    }

    private static String activationSort(ActivationEvent event) {
        return ACTIVATION_PREFIX + String.format("%019d", event.activatedAtEpochMs())
            + ":" + event.releaseVersion();
    }

    private static AttributeValue avS(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String stringValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.s() == null) {
            throw new IllegalStateException("Dynamo release registry record is missing string attribute " + name);
        }
        return value.s();
    }

    private static String artifactUriValue(Map<String, AttributeValue> item) {
        AttributeValue current = item.get(PRIMARY_ARTIFACT_URI);
        if (current != null && current.s() != null && !current.s().isBlank()) {
            return current.s();
        }
        AttributeValue legacy = item.get(LEGACY_PRIMARY_ARTIFACT_PATH);
        if (legacy != null && legacy.s() != null && !legacy.s().isBlank()) {
            return legacy.s();
        }
        throw new IllegalStateException(
            "Dynamo release registry record is missing string attribute " + PRIMARY_ARTIFACT_URI);
    }

    private static long longValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.n() == null) {
            throw new IllegalStateException("Dynamo release registry record is missing numeric attribute " + name);
        }
        return Long.parseLong(value.n());
    }

    private static String toJson(Object value) {
        try {
            return PipelineJson.mapper().writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing release registry value", e);
        }
    }

    private static <T> T fromJson(String value, Class<T> type) {
        try {
            return PipelineJson.mapper().readValue(value, type);
        } catch (Exception e) {
            throw new IllegalStateException("Failed deserializing release registry value " + type.getName(), e);
        }
    }

    private static <T> Uni<T> blocking(java.util.function.Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private record ActivationEvent(String releaseVersion, long activatedAtEpochMs) {
        private ActivationEvent {
            if (releaseVersion == null || releaseVersion.isBlank()) {
                throw new IllegalArgumentException("releaseVersion must not be blank");
            }
            if (activatedAtEpochMs <= 0) {
                throw new IllegalArgumentException("activatedAtEpochMs must be positive");
            }
        }
    }
}
