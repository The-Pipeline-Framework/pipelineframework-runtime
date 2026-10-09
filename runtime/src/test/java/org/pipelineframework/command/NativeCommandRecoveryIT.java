package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.pipelineframework.connector.*;
import org.pipelineframework.execution.*;
import org.pipelineframework.orchestrator.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/** Native durable effects plus an independently retained external-provider receipt boundary. */
@Testcontainers(disabledWithoutDocker = true)
class NativeCommandRecoveryIT {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
        .withServices("dynamodb");
    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final String TENANT = "tenant-one";
    private DynamoDbClient dynamo;
    private String table;
    private Fixture fixture;

    @BeforeEach
    void setup() {
        dynamo = DynamoDbClient.builder().endpointOverride(LOCALSTACK.getEndpoint()).region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
            .build();
        table = "command-recovery-" + UUID.randomUUID();
        dynamo.createTable(CreateTableRequest.builder().tableName(table)
            .attributeDefinitions(AttributeDefinition.builder().attributeName("command_key").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("revision").attributeType(ScalarAttributeType.N).build())
            .keySchema(KeySchemaElement.builder().attributeName("command_key").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("revision").keyType(KeyType.RANGE).build())
            .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()).build());
        dynamo.waiter().waitUntilTableExists(request -> request.tableName(table));
        fixture = new Fixture();
        PipelineExecutionContextHolder.set(context("release-one"));
    }

    @AfterEach
    void close() {
        PipelineExecutionContextHolder.clear();
        if (dynamo != null) dynamo.close();
    }

    @Test
    void authoritativeSuccessRecoversTypedOutputWithoutRepeatingEffectAndLateAckConverges() throws Exception {
        String id = "lost-ack";
        CompletableFuture<Result> original = start(support(store(), false), id, descriptor("archive"));
        fixture.applied(id).get(15, TimeUnit.SECONDS);
        CommandEffectRecord before = effect(id);
        List<Map<String, AttributeValue>> historyBefore = history(id);
        Map<String, AttributeValue> receiptBefore = external(id);
        assertEquals(CommandEffectStatus.DISPATCHING, before.status());
        assertFalse(original.isDone());
        assertEquals(2, historyBefore.size());
        assertEquals("APPLIED", receiptBefore.get("external_state").s());
        assertEquals(id, receiptBefore.get("command").s());
        assertEquals("input", receiptBefore.get("input").s());
        assertEquals(before.currentAttempt().attemptId(), receiptBefore.get("attempt").s());
        assertEquals(before.currentAttempt().occurrenceId(), receiptBefore.get("occurrence").s());
        assertEquals(1, fixture.entries(id).get());

        assertEquals(new Result("provider-result"), execute(support(store(), false), id, descriptor("archive")));
        CommandEffectRecord settled = effect(id);
        assertEquals(CommandEffectStatus.SUCCEEDED, settled.status());
        assertEquals(before.currentAttempt().attemptId(), settled.currentAttempt().attemptId());
        assertEquals(before.currentAttempt().occurrenceId(), settled.currentAttempt().occurrenceId());
        assertEquals(1, settled.attempts().size());
        assertEquals(Optional.of(new Result("provider-result")), settled.currentAttempt().output());
        assertTrue(settled.currentAttempt().reconciliationReceipt().isPresent());
        assertEquals(receiptBefore, external(id));
        assertEquals(historyBefore, history(id).subList(0, historyBefore.size()));
        assertEquals(3, history(id).size());
        assertEquals(1, fixture.entries(id).get());

        fixture.ack(id).complete(success(id, new Result("provider-result")));
        assertEquals(new Result("provider-result"), original.get(15, TimeUnit.SECONDS));
        assertEquals(settled, effect(id));
        assertEquals(3, history(id).size());
        assertEquals(1, fixture.entries(id).get());
        assertEquals(new Result("provider-result"), execute(new CommandStepSupport(new ConnectorRegistry(List.of()), List.of(store()), config()), id, descriptor("archive")));
        assertEquals(settled, effect(id));
        assertEquals(1, fixture.entries(id).get());
    }

    @Test
    void authoritativeReceiptSettlesNativeAmbiguousOutcomeWithoutRedispatch() throws Exception {
        String id = "ambiguous-receipt";
        var original = start(support(store(), false), id, descriptor("archive"));
        fixture.applied(id).get(15, TimeUnit.SECONDS);
        var dispatched = effect(id);
        var receipt = external(id);
        assertEquals("APPLIED", receipt.get("external_state").s());
        assertEquals(dispatched.currentAttempt().attemptId(), receipt.get("attempt").s());
        assertEquals(dispatched.currentAttempt().occurrenceId(), receipt.get("occurrence").s());
        assertEquals(1, fixture.entries(id).get());
        fixture.ack(id).complete(new CommandOutcome.Ambiguous<>("provider-acknowledgement-uncertain", List.of()));
        var rejected = assertThrows(ExecutionException.class, () -> original.get(15, TimeUnit.SECONDS));
        assertInstanceOf(CommandOutcomeException.class, rejected.getCause());
        var ambiguous = effect(id);
        var rows = history(id);
        assertEquals(CommandEffectStatus.AMBIGUOUS, ambiguous.status());
        assertEquals(CommandEffectStatus.AMBIGUOUS, ambiguous.currentAttempt().status());
        assertEquals(dispatched.currentAttempt().attemptId(), ambiguous.currentAttempt().attemptId());
        assertEquals(dispatched.currentAttempt().occurrenceId(), ambiguous.currentAttempt().occurrenceId());
        assertEquals(3, rows.size());
        assertEquals(receipt, external(id));

        assertEquals(new Result("provider-result"), execute(support(store(), false), id, descriptor("archive")));
        var settled = effect(id);
        assertEquals(CommandEffectStatus.SUCCEEDED, settled.status());
        assertEquals(ambiguous.currentAttempt().attemptId(), settled.currentAttempt().attemptId());
        assertEquals(ambiguous.currentAttempt().occurrenceId(), settled.currentAttempt().occurrenceId());
        assertEquals(1, settled.attempts().size());
        assertEquals(Optional.of(new Result("provider-result")), settled.currentAttempt().output());
        assertTrue(settled.currentAttempt().reconciliationReceipt().isPresent());
        assertEquals(rows, history(id).subList(0, rows.size()));
        assertEquals(4, history(id).size());
        assertEquals(receipt, external(id));
        assertEquals(1, fixture.entries(id).get());
    }

    @Test
    void unknownAndChangedOriginalBindingsDoNotAlterHistoryOrReceipt() throws Exception {
        String id = "unknown";
        start(support(store(), false), id, descriptor("archive"));
        fixture.applied(id).get(15, TimeUnit.SECONDS);
        var before = effect(id);
        var rows = history(id);
        var receipt = external(id);
        fixture.unresolved = true;
        assertThrows(CommandInProgressException.class, () -> execute(support(store(), false), id, descriptor("archive")));
        fixture.unresolved = false;
        fixture.account = "account-two";
        assertThrows(CommandEffectStoreException.class, () -> execute(support(store(), false), id, descriptor("archive")));
        fixture.account = "account-one";
        assertThrows(CommandEffectStoreException.class, () -> execute(support(store(), false), id, descriptor("other-resource")));
        PipelineExecutionContextHolder.set(context("release-two"));
        assertThrows(CommandEffectStoreException.class, () -> execute(support(store(), false), id, descriptor("archive")));
        PipelineExecutionContextHolder.set(context("release-one"));
        assertThrows(CommandEffectStoreException.class, () -> support(store(), false).<String, Result>execute(
            descriptor("archive"), (ignored, input) -> id, "changed-input").await().atMost(TIMEOUT));
        assertEquals(before, effect(id));
        assertEquals(rows, history(id));
        assertEquals(receipt, external(id));
    }

    @Test
    void providerReceiptConflictAndUnboundReservationCannotBePromoted() throws Exception {
        String id = "stale-receipt";
        start(support(store(), false), id, descriptor("archive"));
        fixture.applied(id).get(15, TimeUnit.SECONDS);
        var before = effect(id);
        var rows = history(id);
        var receipt = external(id);
        fixture.receiptConflict = true;
        assertThrows(CommandInProgressException.class, () -> execute(support(store(), false), id, descriptor("archive")));
        fixture.receiptConflict = false;
        assertEquals(before, effect(id));
        assertEquals(rows, history(id));
        assertEquals(receipt, external(id));

        String unbound = "unbound";
        fixture.targetSupported = false;
        start(support(store(), false), unbound, descriptor("archive"));
        fixture.applied(unbound).get(15, TimeUnit.SECONDS);
        var old = effect(unbound);
        var oldRows = history(unbound);
        var oldReceipt = external(unbound);
        assertTrue(old.currentAttempt().recoveryBinding().isEmpty());
        fixture.targetSupported = true;
        assertThrows(CommandInProgressException.class, () -> execute(support(store(), false), unbound, descriptor("archive")));
        assertEquals(old, effect(unbound));
        assertEquals(oldRows, history(unbound));
        assertEquals(oldReceipt, external(unbound));
    }

    @Test
    void concurrentReconcilersAppendOneConsistentSettlement() throws Exception {
        String id = "concurrent";
        start(support(store(), false), id, descriptor("archive"));
        fixture.applied(id).get(15, TimeUnit.SECONDS);
        var before = effect(id);
        var receipt = external(id);
        fixture.inquiryGate = new CompletableFuture<>();
        fixture.inquiries = new CountDownLatch(2);
        var first = start(support(store(), false), id, descriptor("archive"));
        var second = start(support(store(), false), id, descriptor("archive"));
        assertTrue(fixture.inquiries.await(15, TimeUnit.SECONDS));
        fixture.inquiryGate.complete(null);
        assertEquals(new Result("provider-result"), first.get(15, TimeUnit.SECONDS));
        assertEquals(new Result("provider-result"), second.get(15, TimeUnit.SECONDS));
        assertEquals(3, history(id).size());
        assertEquals(1, effect(id).attempts().size());
        assertEquals(before.currentAttempt().attemptId(), effect(id).currentAttempt().attemptId());
        assertEquals(receipt, external(id));
    }

    @Test
    void originalAckWinningBeforeInquirySettlementIsNotOverwritten() throws Exception {
        String id = "ack-winner";
        var original = start(support(store(), false), id, descriptor("archive"));
        fixture.applied(id).get(15, TimeUnit.SECONDS);
        fixture.inquiryGate = new CompletableFuture<>();
        fixture.inquiries = new CountDownLatch(1);
        var recovery = start(support(store(), false), id, descriptor("archive"));
        assertTrue(fixture.inquiries.await(15, TimeUnit.SECONDS));
        fixture.ack(id).complete(success(id, new Result("provider-result")));
        assertEquals(new Result("provider-result"), original.get(15, TimeUnit.SECONDS));
        var winner = effect(id);
        var rows = history(id);
        fixture.inquiryGate.complete(null);
        assertEquals(new Result("provider-result"), recovery.get(15, TimeUnit.SECONDS));
        assertEquals(winner, effect(id));
        assertEquals(rows, history(id));
        assertTrue(winner.currentAttempt().reconciliationReceipt().isEmpty());
    }

    @Test
    void conflictingLateAckAndLateFailureCannotOverwriteSettlement() throws Exception {
        for (boolean failure : List.of(false, true)) {
            String id = failure ? "late-failure" : "late-conflict";
            var original = start(support(store(), false), id, descriptor("archive"));
            fixture.applied(id).get(15, TimeUnit.SECONDS);
            execute(support(store(), false), id, descriptor("archive"));
            var settled = effect(id);
            var rows = history(id);
            var receipt = external(id);
            if (failure) fixture.ack(id).completeExceptionally(new IllegalStateException("late transport failure"));
            else fixture.ack(id).complete(success(id, new Result("different-output")));
            ExecutionException rejected = assertThrows(ExecutionException.class, () -> original.get(15, TimeUnit.SECONDS));
            if (failure) {
                var composite = assertInstanceOf(io.smallrye.mutiny.CompositeException.class, rejected.getCause());
                assertTrue(composite.getCauses().stream().anyMatch(cause -> cause.getClass().getName()
                    .equals("org.pipelineframework.command.CommandRecoveryConflictException")));
            } else {
                assertEquals("org.pipelineframework.command.CommandRecoveryConflictException", rejected.getCause().getClass().getName());
            }
            assertEquals(settled, effect(id));
            assertEquals(rows, history(id));
            assertEquals(receipt, external(id));
        }
    }

    @Test
    void recoveryPendingClaimFencesPausedOriginalOwnerBeforeAnyExternalEffect() throws Exception {
        String id = "reserved";
        var paused = new PausedClaimStore();
        var original = start(support(paused, true), id, descriptor("archive"));
        assertTrue(paused.reserved.await(15, TimeUnit.SECONDS));
        var pending = effect(id);
        assertEquals(CommandEffectStatus.PENDING, pending.status());
        assertTrue(external(id).isEmpty());
        assertEquals(0, fixture.entries(id).get());
        assertEquals(new Result("provider-result"), execute(support(store(), true), id, descriptor("archive")));
        var winner = effect(id);
        var rows = history(id);
        var receipt = external(id);
        assertEquals(1, fixture.entries(id).get());
        paused.release.complete(null);
        assertThrows(ExecutionException.class, () -> original.get(15, TimeUnit.SECONDS));
        assertEquals(winner, effect(id));
        assertEquals(rows, history(id));
        assertEquals(receipt, external(id));
        assertEquals(1, fixture.entries(id).get());
        assertEquals(1, winner.attempts().size());
        assertEquals(pending.currentAttempt().attemptId(), winner.currentAttempt().attemptId());
        assertEquals(3, rows.size());
    }

    @Test
    void claimedDispatchWithoutProviderReceiptDoesNotAuthorizeRedispatch() throws Exception {
        String id = "claimed-before-send";
        var paused = new PausedClaimStore();
        var original = start(support(paused, true), id, descriptor("archive"));
        assertTrue(paused.reserved.await(15, TimeUnit.SECONDS));
        var binding = effect(id).currentAttempt().recoveryBinding().orElseThrow();
        // Model the durable claim boundary followed by owner loss before contacting the provider.
        store().claimPendingDispatch(binding, System.currentTimeMillis()).await().atMost(TIMEOUT);
        var retained = effect(id);
        var rows = history(id);
        assertEquals(CommandEffectStatus.DISPATCHING, retained.status());
        assertTrue(external(id).isEmpty());
        assertThrows(CommandInProgressException.class, () -> execute(support(store(), true), id, descriptor("archive")));
        assertEquals(retained, effect(id));
        assertEquals(rows, history(id));
        assertTrue(external(id).isEmpty());
        paused.release.complete(null);
        assertThrows(ExecutionException.class, () -> original.get(15, TimeUnit.SECONDS));
        assertEquals(retained, effect(id));
        assertEquals(rows, history(id));
        assertTrue(external(id).isEmpty());
    }

    @Test
    void originalDispatchRetainsTypedRetryableFailureIdentity() throws Exception {
        assertRetryableFailureIdentity(false, false);
    }

    @Test
    void originalDispatchRetainsTransportFailureIdentity() throws Exception {
        assertRetryableFailureIdentity(false, true);
    }

    @Test
    void recoveredPendingDispatchRetainsTypedRetryableFailureIdentity() throws Exception {
        assertRetryableFailureIdentity(true, false);
    }

    @Test
    void recoveredPendingDispatchRetainsTransportFailureIdentity() throws Exception {
        assertRetryableFailureIdentity(true, true);
    }

    private void assertRetryableFailureIdentity(boolean recovered, boolean transport) throws Exception {
        String id = (recovered ? "recovered-" : "original-") + (transport ? "transport" : "typed");
        PausedClaimStore paused = recovered ? new PausedClaimStore() : null;
        CompletableFuture<Result> oldOwner = null;
        CommandEffectRecord reserved = null;
        if (recovered) {
            oldOwner = start(support(paused, false), id, descriptor("archive"));
            assertTrue(paused.reserved.await(15, TimeUnit.SECONDS));
            reserved = effect(id);
            assertEquals(CommandEffectStatus.PENDING, reserved.status());
            assertTrue(external(id).isEmpty());
        }
        var invocation = start(support(store(), false), id, descriptor("archive"));
        fixture.applied(id).get(15, TimeUnit.SECONDS);
        var dispatching = effect(id);
        var originalRows = history(id);
        var receipt = external(id);
        assertEquals(CommandEffectStatus.DISPATCHING, dispatching.status());
        assertEquals(2, originalRows.size());
        assertEquals("APPLIED", receipt.get("external_state").s());
        assertEquals(dispatching.currentAttempt().attemptId(), receipt.get("attempt").s());
        assertEquals(dispatching.currentAttempt().occurrenceId(), receipt.get("occurrence").s());
        if (reserved != null) {
            assertEquals(reserved.currentAttempt().attemptId(), dispatching.currentAttempt().attemptId());
            assertEquals(reserved.currentAttempt().occurrenceId(), dispatching.currentAttempt().occurrenceId());
        }
        if (transport) fixture.ack(id).completeExceptionally(new IllegalStateException("retryable transport failure"));
        else fixture.ack(id).complete(new CommandOutcome.RetryableFailure<>("temporarily-unavailable", List.of()));
        var rejected = assertThrows(ExecutionException.class, () -> invocation.get(15, TimeUnit.SECONDS));
        Throwable failure = rejected.getCause();
        var failed = effect(id);
        var failedRows = history(id);
        assertEquals(CommandEffectStatus.FAILED_RETRYABLE, failed.status());
        assertEquals(CommandEffectStatus.FAILED_RETRYABLE, failed.currentAttempt().status());
        assertEquals(dispatching.currentAttempt().attemptId(), failed.currentAttempt().attemptId());
        assertEquals(dispatching.currentAttempt().occurrenceId(), failed.currentAttempt().occurrenceId());
        assertEquals(1, failed.attempts().size());
        assertTrue(failed.currentAttempt().reconciliationReceipt().isEmpty());
        assertEquals(originalRows, failedRows.subList(0, originalRows.size()));
        assertEquals(3, failedRows.size());
        assertEquals(receipt, external(id));
        if (recovered) {
            paused.release.complete(null);
            CompletableFuture<Result> retainedOwner = oldOwner;
            assertThrows(ExecutionException.class, () -> retainedOwner.get(15, TimeUnit.SECONDS));
            assertEquals(failed, effect(id));
            assertEquals(failedRows, history(id));
            assertEquals(receipt, external(id));
        }
        var marker = CommandRetryableEffectException.find(failure).map(CommandRetryableEffectException::commandId);
        var envelope = TransitionFailureTestAccess.from(failure, 6);
        var decoded = assertInstanceOf(TransitionWorkerFailureException.class, TransitionFailureTestAccess.restore(envelope));
        assertAll(
            () -> assertEquals(Optional.of(id), marker, "native retry marker must retain the exact logical Command"),
            () -> assertEquals(Optional.of(id), envelope.failedCommandId(), "portable envelope must retain the failed Command"),
            () -> assertEquals(Optional.of(id), decoded.failedCommandId(), "restored failure must retain retry identity"),
            () -> assertEquals(6, envelope.failedStepIndex()),
            () -> assertEquals(6, decoded.failedStepIndex()));
    }

    private CompletableFuture<Result> start(CommandStepSupport support, String id, CommandDescriptor descriptor) {
        return support.<String, Result>execute(descriptor, (ignored, input) -> id, "input").subscribeAsCompletionStage().toCompletableFuture();
    }
    private Result execute(CommandStepSupport support, String id, CommandDescriptor descriptor) {
        return support.<String, Result>execute(descriptor, (ignored, input) -> id, "input").await().atMost(TIMEOUT);
    }
    private DynamoCommandEffectStore store() { return new DynamoCommandEffectStore(dynamo, table); }
    private CommandEffectRecord effect(String id) { return store().find(TENANT, id).await().atMost(TIMEOUT).orElseThrow(); }
    private CommandStepSupport support(CommandEffectStore store, boolean acknowledge) {
        return new CommandStepSupport(new ConnectorRegistry(List.of(new Provider(acknowledge))), List.of(store), config());
    }
    private static PipelineOrchestratorConfig config() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        return config;
    }
    private static PipelineExecutionContext context(String release) {
        return new PipelineExecutionContext(TENANT, "execution-one", "pipeline-one", "1", release, 0, Optional.empty(), Optional.empty());
    }
    private static CommandDescriptor descriptor(String target) {
        return CommandDescriptor.nativeCommand("Write", new NativeCommandSelector(new ConnectorOperationIdentity(
            ConnectorProviderId.of("fixture.provider"), "write", ConnectorOperationKind.COMMAND, 1), 1, CommandPolicy.none()),
            String.class.getName(), Result.class.getName(), "test", CommandDuplicatePolicy.RETURN_RECORDED, Map.of("target", target));
    }
    private static ConnectorConfigSchema<Configuration> schema() {
        return ConnectorConfigSchema.record(Configuration.class, "fixture.configuration", 1);
    }
    private CommandRecoveryTarget target() {
        return new CommandRecoveryTarget(fixture.account, new ConnectorConfigurationSnapshot("fixture.provider", 1, "e".repeat(64), List.of()));
    }
    private static CommandOutcome.Succeeded<Result> success(String id, Result result) {
        return new CommandOutcome.Succeeded<>(result, new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false),
            Set.of(), List.of(new CommandReference("receipt", "receipt-" + id, CommandReferencePurpose.RECONCILIATION)));
    }
    private Map<String, AttributeValue> external(String id) {
        return dynamo.getItem(GetItemRequest.builder().tableName(table).consistentRead(true).key(Map.of(
            "command_key", s("external:" + TENANT + ":" + id), "revision", n(0))).build()).item();
    }
    private List<Map<String, AttributeValue>> history(String id) {
        var encoder = Base64.getUrlEncoder().withoutPadding();
        String key = encoder.encodeToString(TENANT.getBytes(java.nio.charset.StandardCharsets.UTF_8)) + "."
            + encoder.encodeToString(id.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return dynamo.query(QueryRequest.builder().tableName(table).consistentRead(true)
            .keyConditionExpression("command_key = :key").expressionAttributeValues(Map.of(":key", s(key))).build()).items();
    }
    private static AttributeValue s(String value) { return AttributeValue.builder().s(value).build(); }
    private static AttributeValue n(long value) { return AttributeValue.builder().n(Long.toString(value)).build(); }

    public record Configuration(String target) {}
    public record Result(String providerId) {}

    private final class PausedClaimStore extends DynamoCommandEffectStore {
        final CountDownLatch reserved = new CountDownLatch(1);
        final CompletableFuture<Void> release = new CompletableFuture<>();
        PausedClaimStore() { super(NativeCommandRecoveryIT.this.dynamo, NativeCommandRecoveryIT.this.table); }
        @Override public io.smallrye.mutiny.Uni<CommandEffectRecord> claimPendingDispatch(CommandRecoveryBinding expected, long now) {
            reserved.countDown();
            return io.smallrye.mutiny.Uni.createFrom().completionStage(release)
                .onItem().transformToUni(ignored -> super.claimPendingDispatch(expected, now));
        }
    }
    private static final class Fixture {
        final Map<String, java.util.concurrent.atomic.AtomicInteger> dispatchEntries = new ConcurrentHashMap<>();
        final Map<String, CompletableFuture<Void>> applied = new ConcurrentHashMap<>();
        final Map<String, CompletableFuture<CommandOutcome<Result>>> acknowledgements = new ConcurrentHashMap<>();
        volatile boolean unresolved;
        volatile boolean receiptConflict;
        volatile boolean targetSupported = true;
        volatile String account = "account-one";
        volatile CompletableFuture<Void> inquiryGate = CompletableFuture.completedFuture(null);
        volatile CountDownLatch inquiries = new CountDownLatch(0);
        CompletableFuture<Void> applied(String id) { return applied.computeIfAbsent(id, ignored -> new CompletableFuture<>()); }
        CompletableFuture<CommandOutcome<Result>> ack(String id) { return acknowledgements.computeIfAbsent(id, ignored -> new CompletableFuture<>()); }
        java.util.concurrent.atomic.AtomicInteger entries(String id) {
            return dispatchEntries.computeIfAbsent(id, ignored -> new java.util.concurrent.atomic.AtomicInteger());
        }
    }
    private final class Provider implements ConnectorProvider<Void> {
        private final Operation operation;
        Provider(boolean acknowledge) { operation = new Operation(acknowledge); }
        public ConnectorProviderId id() { return ConnectorProviderId.of("fixture.provider"); }
        public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
        public Collection<? extends ConnectorOperation> operations() { return List.of(operation); }
    }
    private final class Operation implements CommandOperation<String, Configuration, Result> {
        private final boolean acknowledge;
        Operation(boolean acknowledge) { this.acknowledge = acknowledge; }
        public String id() { return "write"; }
        public Optional<ConnectorConfigSchema<Configuration>> configurationSchema() { return Optional.of(schema()); }
        public CommandCapabilities capabilities() {
            return new CommandCapabilities(true, true, true, CommandExecutionPosture.AUTOMATED,
                CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false, Set.of("receipt"));
        }
        public Optional<CommandRecoveryTarget> recoveryTarget(CommandInvocation<String, Configuration> invocation) {
            return fixture.targetSupported ? Optional.of(target()) : Optional.empty();
        }
        public CompletionStage<CommandOutcome<Result>> dispatch(CommandInvocation<String, Configuration> invocation) {
            var identity = invocation.dispatchIdentity().orElseThrow();
            String id = identity.commandId();
            fixture.entries(id).incrementAndGet();
            var configuration = ConnectorConfigurationSnapshot.from(schema(), new ConnectorConfigurationDocument(Map.of("target", invocation.configuration().target())), false);
            var item = new HashMap<String, AttributeValue>();
            item.put("command_key", s("external:" + invocation.executionContext().tenantId().orElseThrow() + ":" + id));
            item.put("revision", n(0));
            item.put("external_state", s("APPLIED"));
            item.put("tenant", s(invocation.executionContext().tenantId().orElseThrow()));
            item.put("command", s(id));
            item.put("occurrence", s(identity.occurrenceId()));
            item.put("attempt", s(identity.attemptId()));
            item.put("input", s(invocation.input()));
            item.put("configuration_digest", s(configuration.digest()));
            item.put("target", s(target().identity()));
            item.put("target_configuration", s(target().configuration().digest()));
            item.put("output", s("provider-result"));
            dynamo.putItem(PutItemRequest.builder().tableName(table).item(item).conditionExpression("attribute_not_exists(command_key)").build());
            fixture.applied(id).complete(null);
            return acknowledge ? CompletableFuture.completedFuture(success(id, new Result("provider-result"))) : fixture.ack(id);
        }
        public CompletionStage<CommandReconciliationResult<Result>> reconcile(CommandReconciliationInvocation<String, Configuration> invocation) {
            fixture.inquiries.countDown();
            return fixture.inquiryGate.thenApply(ignored -> {
                var binding = invocation.binding();
                var receipt = external(binding.commandId());
                if (fixture.unresolved || receipt.isEmpty()) return new CommandReconciliationResult.Unresolved<Result>("provider-not-found");
                if (fixture.receiptConflict) return new CommandReconciliationResult.Unresolved<Result>("provider-receipt-conflict");
                if (!receipt.get("tenant").s().equals(binding.tenantId())
                    || !receipt.get("command").s().equals(binding.commandId())
                    || !receipt.get("occurrence").s().equals(binding.occurrenceId())
                    || !receipt.get("attempt").s().equals(binding.attemptId())
                    || !receipt.get("input").s().equals(invocation.invocation().input())
                    || !receipt.get("configuration_digest").s().equals(binding.operationConfiguration().digest())
                    || !receipt.get("target").s().equals(binding.target().identity())
                    || !receipt.get("target_configuration").s().equals(binding.target().configuration().digest())) {
                    return new CommandReconciliationResult.Unresolved<Result>("provider-receipt-conflict");
                }
                var outcome = success(binding.commandId(), new Result(receipt.get("output").s()));
                return new CommandReconciliationResult.ConfirmedSucceeded<Result>(binding, outcome, outcome.references().getFirst());
            });
        }
    }
}
