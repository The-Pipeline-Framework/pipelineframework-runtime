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
        when(release.tenantId()).thenReturn("tenant");
        when(release.pipelineId()).thenReturn("payments");
        when(release.contractVersion()).thenReturn("3");
        when(release.releaseVersion()).thenReturn("release");
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

    @Test
    void serializedModePreservesCanonicalBytesAndTypedReaderCompatibility() {
        ExecutionDurablePayloadResolver typed = resolver();
        var execution = execution(0);
        String original = typed.encode(execution, ExecutionDurablePayloadResolver.Slot.RESULT,
            new PaymentOutput("one", "approved"));
        ExecutionDurablePayloadResolver serialized = serializedResolver();
        TypedDurablePayload envelope = assertInstanceOf(TypedDurablePayload.class,
            serialized.decode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, original));
        String stored = serialized.encode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, envelope);
        assertEquals(original, stored);
        assertEquals(new PaymentOutput("one", "approved"),
            typed.decode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, stored));
    }

    @Test
    void serializedResultItemsPreserveCardinalityAndScalarCanonicalIdentity() throws Exception {
        var single = execution(0, ExecutionResultShape.SINGLE);
        var multi = execution(0, ExecutionResultShape.MATERIALIZED_MULTI);
        var typed = resolver();
        var serialized = serializedResolver();
        var first = new PaymentOutput("one", "approved");
        var second = new PaymentOutput("two", "approved");
        var slot = ExecutionDurablePayloadResolver.Slot.RESULT;
        String scalar = typed.encode(single, slot, first);
        String oneItem = typed.encode(single, slot, List.of(first));
        String twoItems = typed.encode(multi, slot, List.of(first, second));
        String empty = typed.encode(single, slot, List.of());
        assertEquals(List.of(), serialized.decodeResultItems(single, empty));
        var one = assertInstanceOf(List.class, serialized.decodeResultItems(single, oneItem));
        assertEquals(1, one.size());
        assertEquals(scalar, org.pipelineframework.config.pipeline.PipelineJson.mapper().writeValueAsString(one.getFirst()));
        var many = assertInstanceOf(List.class, serialized.decodeResultItems(multi, twoItems));
        assertEquals(2, many.size());
        assertEquals(typed.encode(multi, slot, second),
            org.pipelineframework.config.pipeline.PipelineJson.mapper().writeValueAsString(many.get(1)));
        assertThrows(IllegalStateException.class, () -> serialized.decodeResultItems(single, twoItems));
        assertThrows(IllegalStateException.class, () -> serialized.decodeResultItems(multi, scalar));
        assertEquals(List.of(first), typed.decodeResultItems(single, oneItem));
    }

    @Test
    void serializedReadDoesNotResolveUnavailableApplicationBinding() throws Exception {
        var fixture = resolverFixture();
        var execution = execution(0);
        String stored = fixture.resolver().encode(execution, ExecutionDurablePayloadResolver.Slot.RESULT,
            new PaymentOutput("one", "approved"));
        var release = fixture.registry().get("tenant", "payments", "release").await().indefinitely().orElseThrow();
        var mapper = org.pipelineframework.config.pipeline.PipelineJson.mapper();
        var unavailable = mapper.readValue(mapper.writeValueAsString(release.contract())
            .replace(PaymentOutput.class.getName(), "unavailable.application.Output"), PipelineContractDescriptor.class);
        when(release.contract()).thenReturn(unavailable);
        ExecutionDurablePayloadResolver serialized = serializedResolver();
        serialized.releaseRegistry = fixture.registry();
        assertInstanceOf(TypedDurablePayload.class,
            serialized.decode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, stored));
        ExecutionDurablePayloadResolver freshTyped = resolver();
        freshTyped.releaseRegistry = fixture.registry();
        assertThrows(IllegalStateException.class,
            () -> freshTyped.decode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, stored));
    }

    @Test
    void serializedModeRejectsCorruptionAndWrongWriteBoundary() throws Exception {
        var execution = execution(0);
        ExecutionDurablePayloadResolver resolver = serializedResolver();
        for (TypedDurablePayload payload : List.of(
            new TypedDurablePayload("PaymentOutput", "wrong", "catalog",
                JsonDurablePayloadCodec.ENCODING, 1, "{}".getBytes()),
            new TypedDurablePayload("PaymentOutput", "output-fingerprint", "catalog",
                JsonDurablePayloadCodec.ENCODING, 1, "invalid".getBytes()),
            new TypedDurablePayload("PaymentOutput", "output-fingerprint", "catalog",
                JsonDurablePayloadCodec.ENCODING, 1, "{} {}".getBytes()),
            new TypedDurablePayload("PaymentOutput", "output-fingerprint", "catalog",
                JsonDurablePayloadCodec.ENCODING, 1, "[]".getBytes()),
            new TypedDurablePayload("PaymentOutput", "output-fingerprint", "catalog",
                JsonDurablePayloadCodec.ENCODING, 1, "{}".getBytes()))) {
            String stored = org.pipelineframework.config.pipeline.PipelineJson.mapper().writeValueAsString(payload);
            assertThrows(IllegalStateException.class,
                () -> resolver.decode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, stored));
        }
        TypedDurablePayload input = new TypedDurablePayload("PaymentRecord", "record-fingerprint", "catalog",
            JsonDurablePayloadCodec.ENCODING, 1, "{}".getBytes());
        assertThrows(IllegalArgumentException.class,
            () -> resolver.encode(execution, ExecutionDurablePayloadResolver.Slot.RESULT, input));
    }

    @Test
    void serializedInitialJsonUsesPinnedSchemaWithoutApplicationObjects() {
        var serialized = serializedResolver();
        var execution = execution(0);
        String stored = serialized.encode(execution, ExecutionDurablePayloadResolver.Slot.INPUT,
            Map.of("path", "/input.csv"));
        assertEquals(new CsvPaymentsInputFile("/input.csv"),
            resolver().decode(execution, ExecutionDurablePayloadResolver.Slot.INPUT, stored));
        assertThrows(IllegalArgumentException.class, () -> serialized.encode(execution,
            ExecutionDurablePayloadResolver.Slot.INPUT, Map.of("wrong", "field")));
        assertThrows(IllegalArgumentException.class, () -> serialized.encode(execution,
            ExecutionDurablePayloadResolver.Slot.INPUT, List.of(Map.of("path", "/input.csv"))));
    }

    @Test
    void serializedMultiInputRetainsEmptyAndPopulatedStreamFraming() {
        var serialized = serializedResolver();
        var execution = execution(0);
        when(execution.inputPayload()).thenReturn(new ExecutionInputSnapshot(ExecutionInputShape.MULTI, List.of()));
        for (var values : List.of(List.of(), List.of(Map.of("path", "/input.csv")))) {
            String stored = serialized.encode(execution, ExecutionDurablePayloadResolver.Slot.INPUT, values);
            var envelope = assertInstanceOf(TypedDurablePayload.class,
                serialized.decode(execution, ExecutionDurablePayloadResolver.Slot.INPUT, stored));
            assertEquals("List<CsvPaymentsInputFile>", envelope.canonicalTypeId());
            assertEquals(values.size(), assertInstanceOf(List.class,
                resolver().decode(execution, ExecutionDurablePayloadResolver.Slot.INPUT, stored)).size());
        }
    }

    @Test
    void serializedContinuationUsesItsCanonicalIdentityAtPhysicalCursor() {
        var execution = execution(99);
        var typed = resolver();
        var serialized = serializedResolver();
        String source = typed.encode(execution, "PaymentStatus", new PaymentStatus("one", "approved"));
        var envelope = assertInstanceOf(TypedDurablePayload.class,
            serialized.decode(execution, ExecutionDurablePayloadResolver.Slot.CONTINUATION_INPUT, source));
        assertEquals(source, serialized.encode(execution,
            ExecutionDurablePayloadResolver.Slot.CONTINUATION_INPUT, envelope));
        String combined = serialized.encode(execution, "PaymentStatus", List.of(envelope, envelope));
        assertEquals(List.of(new PaymentStatus("one", "approved"), new PaymentStatus("one", "approved")),
            typed.decode(execution, ExecutionDurablePayloadResolver.Slot.CONTINUATION_INPUT, combined));
    }

    @Test
    void resolvedReleaseMustMatchEveryPinnedIdentityField() {
        for (String field : List.of("tenant", "pipeline", "contract", "release")) {
            ResolverFixture fixture = resolverFixture();
            PipelineReleaseRecord release = fixture.registry().get("tenant", "payments", "release")
                .await().indefinitely().orElseThrow();
            switch (field) {
                case "tenant" -> when(release.tenantId()).thenReturn("other");
                case "pipeline" -> when(release.pipelineId()).thenReturn("other");
                case "contract" -> when(release.contractVersion()).thenReturn("other");
                case "release" -> when(release.releaseVersion()).thenReturn("other");
            }
            assertThrows(IllegalStateException.class,
                () -> fixture.resolver().supportsTypedPayloads(execution(0)));
        }
    }

    private static ExecutionDurablePayloadResolver serializedResolver() {
        ExecutionDurablePayloadResolver resolver = resolver();
        resolver.orchestratorConfig = mock(PipelineOrchestratorConfig.class);
        var controlPlane = mock(PipelineOrchestratorConfig.ControlPlaneConfig.class);
        when(resolver.orchestratorConfig.controlPlane()).thenReturn(controlPlane);
        when(controlPlane.payloadMaterialization()).thenReturn(
            PipelineOrchestratorConfig.ControlPlaneConfig.PayloadMaterialization.SERIALIZED);
        return resolver;
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
        when(release.tenantId()).thenReturn("tenant");
        when(release.pipelineId()).thenReturn("payments");
        when(release.contractVersion()).thenReturn("3");
        when(release.releaseVersion()).thenReturn("release");
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
        return Map.of("runtimeClass", type.getName(), "definitionFingerprint", fingerprint,
            "definition", Map.of("kind", "record", "id", type.getSimpleName(), "fields",
                java.util.Arrays.stream(type.getRecordComponents()).map(component -> Map.of(
                    "name", component.getName(), "presence", "REQUIRED", "nullability", "NON_NULL",
                    "type", Map.of("kind", "scalar", "id", "string"))).toList()));
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
