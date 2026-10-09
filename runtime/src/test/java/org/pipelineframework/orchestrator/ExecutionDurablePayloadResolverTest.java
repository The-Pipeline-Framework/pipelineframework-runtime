package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;

import io.smallrye.mutiny.Uni;

class ExecutionDurablePayloadResolverTest {

    @Test
    void terminalSingleResultsUsePinnedOutputForScalarAndListAtEveryCursor() {
        ExecutionDurablePayloadResolver resolver = resolver();
        PaymentOutput output = new PaymentOutput("payment-1", "approved");
        for (int cursor : List.of(0, 1, 2, 4, 5, 8)) {
            ExecutionRecord<Object, Object> execution = execution(cursor);
            String scalar = resolver.encode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, output);
            String collection = resolver.encode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, List.of(output));
            assertTrue(scalar.contains("\"canonicalTypeId\":\"PaymentOutput\""));
            assertTrue(collection.contains("\"canonicalTypeId\":\"List<PaymentOutput>\""));
            assertEquals(output, resolver.decode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, scalar));
            assertEquals(List.of(output), resolver.decode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, collection));
            String materialized = resolver.encode(execution(cursor, ExecutionResultShape.MATERIALIZED_MULTI),
                ExecutionDurablePayloadResolver.Slot.RESULT, List.of(output));
            assertEquals(List.of(output), resolver.decode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, materialized));
        }
    }

    @Test
    void legacySucceededResultsUsePinnedTerminalOutputAtInRangeCursors() {
        for (int cursor : List.of(0, 2, 4)) {
            ExecutionRecord<Object, Object> execution = execution(cursor);
            when(execution.status()).thenReturn(ExecutionStatus.SUCCEEDED);
            assertEquals(new PaymentOutput("payment-1", "approved"), resolver().decodeLegacy(execution,
                ExecutionDurablePayloadResolver.Slot.RESULT, "{\"id\":\"payment-1\",\"status\":\"approved\"}"));
        }
    }

    @Test
    void historicalTypedResultsKeepTheirStoredIdentityAndTerminalEncodingRejectsIntermediateValues() {
        ResolverFixture fixture = resolverFixture();
        ExecutionDurablePayloadResolver resolver = fixture.resolver();
        PaymentStatus historical = new PaymentStatus("payment-1", "approved");
        String stored = resolver.encode(execution(2), "PaymentStatus", historical);
        assertEquals(historical, resolver.decode(execution(0), ExecutionDurablePayloadResolver.Slot.RESULT, stored));
        assertThrows(IllegalStateException.class, () -> resolver.encode(execution(1),
            ExecutionDurablePayloadResolver.Slot.RESULT, new PaymentRecord("payment-1")));
        assertThrows(IllegalStateException.class, () -> resolver.encode(execution(4),
            ExecutionDurablePayloadResolver.Slot.RESULT, List.of(historical)));
        assertTrue(stored.contains("\"catalogFingerprint\":\"catalog\""));
        assertThrows(IllegalStateException.class, () -> resolver.decode(execution(0), ExecutionDurablePayloadResolver.Slot.RESULT,
            stored.replace("\"catalogFingerprint\":\"catalog\"", "\"catalogFingerprint\":\"wrong-catalog\"")));
        ExecutionRecord<Object, Object> unavailable = execution(4);
        when(unavailable.releaseVersion()).thenReturn("unavailable-release");
        when(fixture.registry().get("tenant", "payments", "unavailable-release"))
            .thenReturn(Uni.createFrom().item(Optional.empty()));
        assertThrows(IllegalStateException.class, () -> resolver.encode(unavailable,
            ExecutionDurablePayloadResolver.Slot.RESULT, new PaymentOutput("payment-1", "approved")));
    }

    @Test
    void restoresCanonicalInputAndItemizedChildResultsFromAPinnedRelease() {
        ExecutionDurablePayloadResolver resolver = resolver();
        ExecutionRecord<Object, Object> inputExecution = execution(0);
        ExecutionRecord<Object, Object> resultExecution = execution(5, ExecutionResultShape.MATERIALIZED_MULTI);
        CsvPaymentsInputFile input = new CsvPaymentsInputFile("/app/test-e2e/payments.csv");
        List<PaymentOutput> children = List.of(new PaymentOutput("payment-1", "approved"));

        String encodedInput = resolver.encode(inputExecution, ExecutionDurablePayloadResolver.Slot.INPUT, input);
        String encodedChildren = resolver.encode(resultExecution, ExecutionDurablePayloadResolver.Slot.RESULT, children);

        assertEquals(input, assertInstanceOf(CsvPaymentsInputFile.class,
            resolver.decode(inputExecution, ExecutionDurablePayloadResolver.Slot.INPUT, encodedInput)));
        Object restored = resolver.decode(resultExecution, ExecutionDurablePayloadResolver.Slot.RESULT, encodedChildren);
        List<?> restoredChildren = assertInstanceOf(List.class, restored);
        assertEquals(children, restoredChildren);
        assertInstanceOf(PaymentOutput.class, restoredChildren.getFirst());
    }

    @Test
    void restoresTypedEntryInputAfterTheExecutionHasAdvancedToAnAwaitContinuation() {
        ExecutionDurablePayloadResolver resolver = resolver();
        CsvPaymentsInputFile original = new CsvPaymentsInputFile("/app/test-e2e/payments.csv");

        String encoded = resolver.encode(execution(0), ExecutionDurablePayloadResolver.Slot.INPUT, original);

        assertEquals(original, assertInstanceOf(CsvPaymentsInputFile.class,
            resolver.decode(execution(2), ExecutionDurablePayloadResolver.Slot.INPUT, encoded)));
    }

    @Test
    void restoresTypedContinuationInputFromItsStoredIdentity() {
        ExecutionDurablePayloadResolver resolver = resolver();
        PaymentStatus continuation = new PaymentStatus("payment-1", "approved");

        String encoded = resolver.encode(execution(2), ExecutionDurablePayloadResolver.Slot.CONTINUATION_INPUT, continuation);

        assertEquals(continuation, assertInstanceOf(PaymentStatus.class,
            resolver.decode(execution(2), ExecutionDurablePayloadResolver.Slot.INPUT, encoded)));
    }

    @Test
    void encodesABranchContinuationUsingTheReceivingStepInputNotThePreviousBranchOutput() {
        ExecutionDurablePayloadResolver resolver = resolver();
        PaymentStatus continuation = new PaymentStatus("payment-1", "approved");

        String encoded = resolver.encode(execution(4), ExecutionDurablePayloadResolver.Slot.CONTINUATION_INPUT, continuation);

        assertTrue(encoded.contains("\"canonicalTypeId\":\"PaymentStatus\""));
        assertEquals(continuation, assertInstanceOf(PaymentStatus.class,
            resolver.decode(execution(4), ExecutionDurablePayloadResolver.Slot.INPUT, encoded)));
    }

    @Test
    void encodesAnOutOfRangeGeneratedContinuationCursorAsTheTerminalSemanticOutput() {
        ExecutionDurablePayloadResolver resolver = resolver();
        List<PaymentOutput> outputs = List.of(new PaymentOutput("payment-1", "approved"));

        String encoded = resolver.encode(execution(8), ExecutionDurablePayloadResolver.Slot.CONTINUATION_INPUT, outputs);

        assertTrue(encoded.contains("\"canonicalTypeId\":\"List<PaymentOutput>\""));
        List<?> restored = assertInstanceOf(List.class,
            resolver.decode(execution(8), ExecutionDurablePayloadResolver.Slot.INPUT, encoded));
        assertInstanceOf(PaymentOutput.class, restored.getFirst());
    }

    @Test
    void rejectsCatalogTypeThatIsNotAnAllowedExecutionInputOccupant() {
        ExecutionDurablePayloadResolver resolver = resolver();
        String encoded = "{\"canonicalTypeId\":\"Unrelated\",\"typeExpressionFingerprint\":\"unrelated-fingerprint\","
            + "\"catalogFingerprint\":\"catalog\",\"encoding\":\"application/tpf-canonical+json\",\"encodingVersion\":1,\"payload\":\"e30=\"}";

        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> resolver.decode(execution(2), ExecutionDurablePayloadResolver.Slot.INPUT, encoded));

        assertTrue(failure.getCause().getMessage().contains("not permitted for INPUT"));
    }

    @Test
    void refusesTypedExecutionPayloadMismatchWithoutMapFallback() {
        ExecutionDurablePayloadResolver resolver = resolver();
        String invalid = "{\"canonicalTypeId\":\"PaymentOutput\",\"typeExpressionFingerprint\":\"wrong\","
            + "\"catalogFingerprint\":\"catalog\",\"encoding\":\"application/tpf-canonical+json\",\"encodingVersion\":1,\"payload\":\"e30=\"}";

        assertThrows(IllegalStateException.class,
            () -> resolver.decode(execution(1), ExecutionDurablePayloadResolver.Slot.RESULT, invalid));
    }

    @Test
    void legacyExecutionPayloadRestoresThroughThePinnedBinding() {
        PaymentOutput expected = new PaymentOutput("payment-1", "approved");

        Object restored = resolver().decodeLegacy(execution(5), ExecutionDurablePayloadResolver.Slot.RESULT,
            "{\"_tpf_java_class\":\"untrusted.LegacyOutput\",\"_tpf_payload\":{\"id\":\"payment-1\",\"status\":\"approved\"}}");

        assertEquals(expected, assertInstanceOf(PaymentOutput.class, restored));
    }

    @Test
    void legacyInputStillUsesTheExplicitCompatibilityExpression() {
        CsvPaymentsInputFile expected = new CsvPaymentsInputFile("/app/test-e2e/payments.csv");

        Object restored = resolver().decodeLegacy(execution(0), ExecutionDurablePayloadResolver.Slot.INPUT,
            "{\"_tpf_payload\":{\"path\":\"/app/test-e2e/payments.csv\"}}");

        assertEquals(expected, assertInstanceOf(CsvPaymentsInputFile.class, restored));
    }

    @Test
    void cachesThePinnedReleaseAndCompiledPlanAcrossWarmCodecCalls() {
        ResolverFixture fixture = resolverFixture();
        ExecutionRecord<Object, Object> execution = execution(0);
        CsvPaymentsInputFile input = new CsvPaymentsInputFile("/app/test-e2e/payments.csv");

        fixture.resolver().encode(execution, ExecutionDurablePayloadResolver.Slot.INPUT, input);
        fixture.resolver().encode(execution, ExecutionDurablePayloadResolver.Slot.INPUT, input);

        verify(fixture.registry(), times(1)).get("tenant", "payments", "release");
    }

    @Test
    void leavesSchemaV1ExecutionsOnTheirLegacyPayloadFormat() {
        PipelineReleaseRegistry registry = mock(PipelineReleaseRegistry.class);
        PipelineReleaseRecord release = mock(PipelineReleaseRecord.class);
        when(release.contract()).thenReturn(new PipelineContractDescriptor(
            1, "payments", "2", "contract", null, null, null, false, null,
            List.of(new PipelineBundleStepDescriptor(0, "input", "object", "ONE_TO_ONE",
                PaymentRecord.class.getName(), PaymentOutput.class.getName(), null, null, null)),
            PipelineBundleCapabilities.defaults()));
        when(registry.get("tenant", "payments", "release")).thenReturn(Uni.createFrom().item(Optional.of(release)));
        ExecutionDurablePayloadResolver resolver = new ExecutionDurablePayloadResolver();
        resolver.releaseRegistry = registry;
        resolver.codec = new JsonDurablePayloadCodec();

        assertFalse(resolver.supportsTypedPayloads(execution(0)));
    }

    private static ExecutionDurablePayloadResolver resolver() {
        return resolverFixture().resolver();
    }

    private static ResolverFixture resolverFixture() {
        PipelineReleaseRegistry registry = mock(PipelineReleaseRegistry.class);
        PipelineContractDescriptor contract = new PipelineContractDescriptor(2, "payments", "3", "contract", null, null,
            null, false, null, List.of(
                new PipelineBundleStepDescriptor(0, "input", "object", "ONE_TO_ONE", CsvPaymentsInputFile.class.getName(), PaymentRecord.class.getName(), null, null, null),
                new PipelineBundleStepDescriptor(1, "await", "await", "ONE_TO_ONE", PaymentRecord.class.getName(), PaymentStatus.class.getName(), null, null, null),
                new PipelineBundleStepDescriptor(2, "terminal", "terminal", "ONE_TO_ONE", PaymentStatus.class.getName(), PaymentOutput.class.getName(), null, null, null),
                new PipelineBundleStepDescriptor(3, "approved", "branch", "ONE_TO_ONE", PaymentStatus.class.getName(), ApprovedPaymentOutput.class.getName(), null, null, null),
                new PipelineBundleStepDescriptor(4, "unapproved", "branch", "ONE_TO_ONE", PaymentStatus.class.getName(), PaymentOutput.class.getName(), null, null, null)),
            PipelineBundleCapabilities.defaults(), Map.of(
                "CsvPaymentsInputFile", binding(CsvPaymentsInputFile.class, "input-file-fingerprint"),
                "PaymentRecord", binding(PaymentRecord.class, "record-fingerprint"),
                "PaymentStatus", binding(PaymentStatus.class, "status-fingerprint"),
                "ApprovedPaymentOutput", binding(ApprovedPaymentOutput.class, "approved-output-fingerprint"),
                "PaymentOutput", binding(PaymentOutput.class, "output-fingerprint"),
                "Unrelated", binding(Unrelated.class, "unrelated-fingerprint")), "catalog");
        PipelineReleaseRecord release = mock(PipelineReleaseRecord.class);
        when(release.contract()).thenReturn(contract);
        when(registry.get("tenant", "payments", "release")).thenReturn(Uni.createFrom().item(Optional.of(release)));
        ExecutionDurablePayloadResolver resolver = new ExecutionDurablePayloadResolver();
        resolver.releaseRegistry = registry;
        resolver.codec = new JsonDurablePayloadCodec();
        return new ResolverFixture(resolver, registry);
    }

    @SuppressWarnings("unchecked")
    private static ExecutionRecord<Object, Object> execution(int stepIndex) {
        return execution(stepIndex, ExecutionResultShape.SINGLE);
    }

    @SuppressWarnings("unchecked")
    private static ExecutionRecord<Object, Object> execution(int stepIndex, ExecutionResultShape resultShape) {
        ExecutionRecord<Object, Object> execution = mock(ExecutionRecord.class);
        when(execution.tenantId()).thenReturn("tenant");
        when(execution.pipelineId()).thenReturn("payments");
        when(execution.contractVersion()).thenReturn("3");
        when(execution.releaseVersion()).thenReturn("release");
        when(execution.executionId()).thenReturn("execution");
        when(execution.currentStepIndex()).thenReturn(stepIndex);
        when(execution.resultShape()).thenReturn(resultShape);
        return execution;
    }

    private static Map<String, Object> binding(Class<?> type, String fingerprint) {
        return Map.of("runtimeClass", type.getName(), "definitionFingerprint", fingerprint);
    }

    private record CsvPaymentsInputFile(String path) { }
    private record PaymentRecord(String id) { }
    private record PaymentStatus(String id, String status) { }
    private record ApprovedPaymentOutput(String id, String status) { }
    private record PaymentOutput(String id, String status) { }
    private record Unrelated(String value) { }

    private record ResolverFixture(ExecutionDurablePayloadResolver resolver, PipelineReleaseRegistry registry) {
    }
}
