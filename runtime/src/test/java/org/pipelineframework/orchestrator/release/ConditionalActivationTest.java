package org.pipelineframework.orchestrator.release;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConditionalActivationTest {
    @Test void retryPreservesReceiptButNewStaleOperationCannotOverwriteInterveningActivation() {
        var registry = new InMemoryPipelineReleaseRegistry();
        var release = CurrentActivationObservationTest.release(PipelineReleaseStatus.REGISTERED);
        registry.register(release).await().indefinitely();
        var none = registry.currentActivationObservation("tenant","pipeline").await().indefinitely();
        var firstCommand = new ActivationOperationCommand("first",release,1000);
        var first = registry.activateOnceIfCurrent(firstCommand, none).await().indefinitely();
        var firstHead = registry.currentActivationObservation("tenant","pipeline").await().indefinitely();
        var second = registry.activateOnce(new ActivationOperationCommand("second",release,1000)).await().indefinitely();
        assertNotEquals(first.activationId(),second.activationId());
        assertEquals(first,registry.activateOnceIfCurrent(firstCommand,none).await().indefinitely());
        assertThrows(IllegalStateException.class, () -> registry.activateOnceIfCurrent(
            new ActivationOperationCommand("stale-rollback",release,1000),firstHead).await().indefinitely());
        assertEquals(second.activationId(),registry.currentActivationObservation("tenant","pipeline").await().indefinitely().identifiedEvent().orElseThrow().activationId());
        assertTrue(registry.getActivationOperation("tenant","pipeline","stale-rollback").await().indefinitely().isEmpty());
    }

    @Test void operationKeyAlsoPinsExpectationAndUnknownOrForeignScopeCannotAuthorize() {
        var registry = new InMemoryPipelineReleaseRegistry();
        var release = CurrentActivationObservationTest.release(PipelineReleaseStatus.REGISTERED);
        registry.register(release).await().indefinitely();
        var none = registry.currentActivationObservation("tenant","pipeline").await().indefinitely();
        var command = new ActivationOperationCommand("first",release,1000);
        registry.activateOnceIfCurrent(command,none).await().indefinitely();
        var current = registry.currentActivationObservation("tenant","pipeline").await().indefinitely();
        assertThrows(IllegalStateException.class, () -> registry.activateOnceIfCurrent(command,current).await().indefinitely());
        assertThrows(IllegalStateException.class, () -> registry.activateOnce(command).await().indefinitely());
        var foreign = new CurrentActivationObservation(1,"other","pipeline",CurrentActivationObservation.State.NONE,Optional.empty());
        assertThrows(IllegalArgumentException.class, () -> registry.activateOnceIfCurrent(command,foreign).await().indefinitely());
        var unknown = new CurrentActivationObservation(1,"tenant","pipeline",CurrentActivationObservation.State.LEGACY_UNKNOWN,Optional.empty());
        assertThrows(IllegalArgumentException.class, () -> registry.activateOnceIfCurrent(command,unknown).await().indefinitely());
    }

    @Test void forgedAbsenceCannotReplaceLegacyUnknown() {
        var registry = new InMemoryPipelineReleaseRegistry();
        var release = CurrentActivationObservationTest.release(PipelineReleaseStatus.ACTIVE);
        registry.register(release).await().indefinitely();
        var none = new CurrentActivationObservation(1,"tenant","pipeline",CurrentActivationObservation.State.NONE,Optional.empty());
        assertThrows(IllegalStateException.class, () -> registry.activateOnceIfCurrent(new ActivationOperationCommand("first",release,1000),none).await().indefinitely());
        assertEquals(CurrentActivationObservation.State.LEGACY_UNKNOWN,registry.currentActivationObservation("tenant","pipeline").await().indefinitely().state());
    }
}
