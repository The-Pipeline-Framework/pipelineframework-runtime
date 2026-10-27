package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import java.math.BigDecimal;
import java.time.Instant;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.*;

class CommandRecoveryRecordCodecTest {
    private final CommandEffectRecordCodec codec = new CommandEffectRecordCodec();

    @Test
    void legacyUserPayloadRecoveryNamesAreNotAttemptMetadata() throws Exception {
        var payload = Map.of("recoveryBinding", "business-value", "reconciliationReceipt", "business-receipt");
        var record = new CommandEffectRecord("tenant", "execution", "step", "command", "id", CommandEffectStatus.SUCCEEDED,
            payload, payload, null, null, Optional.empty(), List.of(), 1L, 1L);
        for (int version : List.of(1, 2)) {
            var root = (ObjectNode) PipelineJson.mapper().readTree(codec.encode(record, Map.class.getName(), Map.class.getName()));
            root.put("schemaVersion", version);
            for (var attempt : root.withArray("attempts")) {
                ((ObjectNode) attempt).remove(List.of("recoveryBinding", "reconciliationReceipt"));
                if (version == 1) ((ObjectNode) attempt).remove(List.of("occurrenceId", "purpose", "output", "reason"));
            }
            assertEquals(payload, codec.decode(root.toString()).record().input());
            assertEquals(payload, codec.decode(root.toString()).record().output());
        }
    }

    @Test
    void roundTripsOriginalTypedBindingAndReconciledReceipt() {
        CommandEffectRecord settled = settled();
        String encoded = codec.encode(settled, String.class.getName(), Result.class.getName());
        assertTrue(encoded.contains("\"schemaVersion\":3"));
        assertEquals(settled, codec.decode(encoded).record());
        assertInstanceOf(Result.class, codec.decode(encoded).record().output());
        assertEquals(settled.currentAttempt().recoveryBinding(), codec.decode(encoded).record().currentAttempt().recoveryBinding());
        assertEquals(settled.currentAttempt().reconciliationReceipt(), codec.decode(encoded).record().currentAttempt().reconciliationReceipt());
    }

    @Test
    void rejectsMismatchedTypedInputOutputConfigurationAndAttemptBinding() throws Exception {
        String encoded = codec.encode(settled(), String.class.getName(), Result.class.getName());
        for (String mutation : List.of("input", "output", "attempt-output", "configuration", "attempt")) {
            ObjectNode root = (ObjectNode) PipelineJson.mapper().readTree(encoded);
            ObjectNode attempt = (ObjectNode) root.withArray("attempts").get(0);
            switch (mutation) {
                case "input" -> ((ObjectNode) root.get("input")).put("value", "changed");
                case "output" -> ((ObjectNode) root.get("output").get("value")).put("value", "changed");
                case "attempt-output" -> ((ObjectNode) attempt.get("output").get("value")).put("value", "changed");
                case "configuration" -> ((ObjectNode) attempt.get("recoveryBinding").get("operationConfiguration")).put("digest", "d".repeat(64));
                case "attempt" -> attempt.put("attemptId", "old-attempt");
                default -> throw new AssertionError(mutation);
            }
            assertThrows(CommandEffectStoreException.class, () -> codec.decode(root.toString()), mutation);
        }
    }

    @Test
    void v2RemainsUnboundAndCannotAcquireNewMetadataByChangingVersion() throws Exception {
        CommandEffectRecord old = pending();
        ObjectNode oldRoot = (ObjectNode) PipelineJson.mapper().readTree(codec.encode(old, String.class.getName(), Result.class.getName()));
        oldRoot.put("schemaVersion", 2);
        for (var attempt : oldRoot.withArray("attempts")) {
            ((ObjectNode) attempt).remove(List.of("recoveryBinding", "reconciliationReceipt"));
        }
        CommandEffectRecord decoded = codec.decode(oldRoot.toString()).record();
        assertEquals(old, decoded);
        assertTrue(decoded.currentAttempt().recoveryBinding().isEmpty());
        assertTrue(decoded.currentAttempt().reconciliationReceipt().isEmpty());
        assertThrows(IllegalStateException.class, () -> decoded.claimPendingDispatch(binding(), 2L));
        ObjectNode downgrade = (ObjectNode) PipelineJson.mapper().readTree(codec.encode(settled(), String.class.getName(), Result.class.getName()));
        downgrade.put("schemaVersion", 2);
        assertThrows(CommandEffectStoreException.class, () -> codec.decode(downgrade.toString()));
    }

    @Test
    void digestIsTypedAndCanonicalRatherThanMapIterationOrderOrToString() {
        Map<String, String> first = new LinkedHashMap<>();
        first.put("b", "two"); first.put("a", "one");
        Map<String, String> second = new LinkedHashMap<>();
        second.put("a", "one"); second.put("b", "two");
        assertEquals(codec.digest(first, Map.class.getName()), codec.digest(second, Map.class.getName()));
        assertNotEquals(codec.digest(first, Map.class.getName()), codec.digest(first, Object.class.getName()));
        assertThrows(CommandEffectStoreException.class, () -> codec.digest("input", "missing.Type"));
        assertThrows(CommandEffectStoreException.class, () -> codec.digest("input", Result.class.getName()));
    }

    @Test
    void storedCommandMustMatchOriginalNativeBindingOrOperation() {
        CommandEffectRecord bound = pending().bindRecovery(binding());
        CommandEffectRecord wrong = new CommandEffectRecord(bound.tenantId(), bound.executionId(), bound.stepId(), "other-command",
            bound.commandId(), bound.status(), bound.input(), bound.output(), bound.errorClass(), bound.errorMessage(), bound.outcome(),
            bound.attempts(), bound.createdAtEpochMs(), bound.updatedAtEpochMs());
        assertThrows(CommandEffectStoreException.class, () -> codec.encode(wrong, String.class.getName(), Result.class.getName()));
    }

    @Test
    void roundTripsNestedNanosecondInstantsAndExactDecimalsWithRecoveryBindingAndReceipt() {
        var value = preciseValue();
        var binding = binding(value, PreciseValue.class.getName(), PreciseValue.class.getName());
        var pending = pending(value).bindRecovery(binding);
        var restored = codec.decode(codec.encode(pending, PreciseValue.class.getName(), PreciseValue.class.getName())).record();
        assertEquals(pending, restored);
        assertEquals(binding.inputDigest(), codec.digest(restored.input(), PreciseValue.class.getName()));

        var reference = new CommandReference("receipt", "precise-receipt", CommandReferencePurpose.RECONCILIATION);
        var outcome = new CommandOutcomeSnapshot(binding.operationIdentity(), 1, binding.operationConfiguration(),
            CommandEffectStatus.SUCCEEDED, "succeeded", Set.of(), CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false, List.of(reference));
        var settled = pending.claimPendingDispatch(binding, 2L).reconcileSucceeded(binding, CommandEffectStatus.DISPATCHING,
            value, outcome, new CommandReconciliationReceipt(reference, codec.digest(value, PreciseValue.class.getName()), 3L), 3L);
        assertEquals(settled, codec.decode(codec.encode(settled, PreciseValue.class.getName(), PreciseValue.class.getName())).record());
    }

    @Test
    void exactDecimalParsingStillRejectsOneNanosecondAndSmallDecimalInputChanges() throws Exception {
        var value = preciseValue();
        var binding = binding(value, PreciseValue.class.getName(), PreciseValue.class.getName());
        String encoded = codec.encode(pending(value).bindRecovery(binding), PreciseValue.class.getName(), PreciseValue.class.getName());
        for (String mutation : List.of("nanosecond", "decimal")) {
            ObjectNode root = (ObjectNode) PipelineJson.mapper().reader()
                .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).readTree(encoded);
            ObjectNode input = (ObjectNode) root.path("input").path("value");
            if (mutation.equals("nanosecond")) {
                ObjectNode nested = (ObjectNode) input.path("evidence");
                nested.put("observedAt", nested.path("observedAt").decimalValue().add(new BigDecimal("0.000000001")));
            } else {
                input.put("amount", input.path("amount").decimalValue().add(new BigDecimal("0.000000000000000000001")));
            }
            assertThrows(CommandEffectStoreException.class, () -> codec.decode(root.toString()), mutation);
        }
    }

    @Test
    void preservesFloatingPointTypesSignedZeroAndExponentForms() {
        var floating = new FloatingValue(-0.0d, -0.0f, 1.0d, 1.0E20d);
        assertBoundInputRoundTrip(floating, FloatingValue.class.getName());
        var map = new LinkedHashMap<String, Object>();
        map.put("negativeZero", -0.0d);
        map.put("whole", 1.0d);
        map.put("fraction", 0.25d);
        map.put("exponent", 1.0E20d);
        assertBoundInputRoundTrip(map, Map.class.getName());
        assertBoundInputRoundTrip(-0.0d, Double.class.getName());
        assertBoundInputRoundTrip(-0.0f, Float.class.getName());
    }

    @Test
    void rejectsDuplicateTypedValueMembersBeforePointerSelection() {
        String encoded = codec.encode(pending().bindRecovery(binding()), String.class.getName(), Result.class.getName());
        String duplicateValue = encoded.replace("\"value\":\"input\"", "\"value\":\"input\",\"value\":\"changed\"");
        assertNotEquals(encoded, duplicateValue);
        assertThrows(CommandEffectStoreException.class, () -> codec.decode(duplicateValue));
        String duplicateInput = encoded.replace("\"input\":", "\"input\":null,\"input\":");
        assertNotEquals(encoded, duplicateInput);
        assertThrows(CommandEffectStoreException.class, () -> codec.decode(duplicateInput));
    }

    private void assertBoundInputRoundTrip(Object value, String declaredType) {
        var binding = binding(value, declaredType, Result.class.getName());
        var pending = pending(value).bindRecovery(binding);
        var restored = codec.decode(codec.encode(pending, declaredType, Result.class.getName())).record();
        assertEquals(pending, restored);
        assertEquals(binding.inputDigest(), codec.digest(restored.input(), declaredType));
    }

    private static PreciseValue preciseValue() {
        return new PreciseValue(new Evidence(Instant.parse("2026-10-23T10:31:45.161393892Z")),
            Instant.parse("2026-10-22T13:36:45.981744123Z"), new BigDecimal("1234567890.123456789012345678901"));
    }

    private CommandEffectRecord settled() {
        var binding = binding();
        var reference = new CommandReference("receipt", "receipt-one", CommandReferencePurpose.RECONCILIATION);
        var output = new Result("confirmed");
        var outcome = new CommandOutcomeSnapshot(binding.operationIdentity(), 1, binding.operationConfiguration(),
            CommandEffectStatus.SUCCEEDED, "succeeded", Set.of(), CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false, List.of(reference));
        return pending().bindRecovery(binding).claimPendingDispatch(binding, 2L).reconcileSucceeded(binding, CommandEffectStatus.DISPATCHING,
            output, outcome, new CommandReconciliationReceipt(reference, codec.digest(output, Result.class.getName()), 3L), 3L);
    }

    private CommandRecoveryBinding binding() {
        return binding("input", String.class.getName(), Result.class.getName());
    }

    private CommandRecoveryBinding binding(Object input, String inputType, String outputType) {
        var configuration = new ConnectorConfigurationSnapshot("configuration", 1, "b".repeat(64), List.of());
        return new CommandRecoveryBinding("tenant", "command", "occurrence", "attempt", "execution", "pipeline", "1", "release", "step",
            new ConnectorOperationIdentity(ConnectorProviderId.of("provider"), "write", ConnectorOperationKind.COMMAND, 1), 1,
            ConnectorBindingName.of("configured"), inputType, outputType, codec.digest(input, inputType),
            configuration, new CommandRecoveryTarget("account-one", configuration));
    }

    private static CommandEffectRecord pending() {
        return pending("input");
    }

    private static CommandEffectRecord pending(Object input) {
        return new CommandEffectRecord("tenant", "execution", "step", "native-binding:configured/write", "command", CommandEffectStatus.PENDING,
            input, null, null, null, Optional.empty(), List.of(new CommandEffectAttemptRecord("attempt", "occurrence", 1, "execution",
                CommandAttemptPurpose.INITIAL, CommandEffectStatus.PENDING, Optional.empty(), null, null, Optional.empty(), Optional.empty(), 1L, 1L)), 1L, 1L);
    }

    record FloatingValue(double negativeDouble, float negativeFloat, double whole, double exponent) {}
    record Evidence(Instant observedAt) {}
    record PreciseValue(Evidence evidence, Instant requestedAt, BigDecimal amount) {}
    record Result(String value) {}
}
