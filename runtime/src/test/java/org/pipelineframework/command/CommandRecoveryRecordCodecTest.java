package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
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
        var configuration = new ConnectorConfigurationSnapshot("configuration", 1, "b".repeat(64), List.of());
        return new CommandRecoveryBinding("tenant", "command", "occurrence", "attempt", "execution", "pipeline", "1", "release", "step",
            new ConnectorOperationIdentity(ConnectorProviderId.of("provider"), "write", ConnectorOperationKind.COMMAND, 1), 1,
            ConnectorBindingName.of("configured"), String.class.getName(), Result.class.getName(), codec.digest("input", String.class.getName()),
            configuration, new CommandRecoveryTarget("account-one", configuration));
    }

    private static CommandEffectRecord pending() {
        return new CommandEffectRecord("tenant", "execution", "step", "native-binding:configured/write", "command", CommandEffectStatus.PENDING,
            "input", null, null, null, Optional.empty(), List.of(new CommandEffectAttemptRecord("attempt", "occurrence", 1, "execution",
                CommandAttemptPurpose.INITIAL, CommandEffectStatus.PENDING, Optional.empty(), null, null, Optional.empty(), Optional.empty(), 1L, 1L)), 1L, 1L);
    }

    record Result(String value) {}
}
