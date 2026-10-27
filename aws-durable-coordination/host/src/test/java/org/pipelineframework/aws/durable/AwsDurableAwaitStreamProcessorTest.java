package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import org.junit.jupiter.api.Test;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableCallbackRegistration;
import org.pipelineframework.aws.durable.model.AwsDurableDriverCheckpoint;
import org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint;

class AwsDurableAwaitStreamProcessorTest {
    private static final String AWAIT_STREAM = "arn:aws:dynamodb:eu-west-1:123456789012:table/await/stream/2026-10-12T00:00:00.000";
    private static final String BINDING_STREAM = "arn:aws:dynamodb:eu-west-1:123456789012:table/bindings/stream/2026-10-12T00:00:00.000";

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"CALLBACK_ACCEPTED", "DRIVER_RESUMED", "CALLBACK_LATENCY", "REPAIR_FIRST_OBSERVED"})
    void mechanicalObservationFactsAreAcknowledgedWithoutEnteringSemanticAuthority(String recordType) {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var actions = mock(AwsDurableActionInvoker.class);
        var wakeups = mock(AwsDurableWakeupService.class);
        var processor = new AwsDurableAwaitStreamProcessor(bindings, actions, wakeups, AWAIT_STREAM, BINDING_STREAM,
            ignored -> { throw new AssertionError("timing facts are mechanical, not semantic rows"); });

        assertThat(processor.process(record(BINDING_STREAM, Map.of("record_type", string(recordType)))))
            .isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        org.mockito.Mockito.verifyNoInteractions(bindings, actions, wakeups);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"missing-callback", "invalid-generation", "missing-source-record"})
    void malformedMechanicalEvidenceDoesNotRetireRequiredRegistrationRepair(String fault) {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var actions = mock(AwsDurableActionInvoker.class);
        var wakeups = mock(AwsDurableWakeupService.class);
        var image = new HashMap<>(registrationImage(registration()));
        if ("missing-callback".equals(fault)) {
            image.remove("provider_callback_id");
        } else if ("invalid-generation".equals(fault)) {
            image.put("generation", new AttributeValue().withN("not-a-number"));
        } else {
            image.clear();
        }
        var damaged = record(BINDING_STREAM, image);
        damaged.getDynamodb().setSequenceNumber("retry-this-record");
        var unrelated = record(BINDING_STREAM, Map.of("record_type", string("GENERATION_CLAIM")));
        unrelated.getDynamodb().setSequenceNumber("already-classified");
        var event = new DynamodbEvent();
        event.setRecords(List.of(damaged, unrelated));
        var processor = new AwsDurableAwaitStreamProcessor(bindings, actions, wakeups, AWAIT_STREAM, BINDING_STREAM,
            ignored -> { throw new AssertionError("mechanical rows cannot supply semantic authority"); });

        assertThat(new AwsDurableAwaitStreamBatchAdapter().handle(event, processor::process).getBatchItemFailures())
            .extracting(item -> item.getItemIdentifier()).containsExactly("retry-this-record");
        org.mockito.Mockito.verifyNoInteractions(bindings, actions, wakeups);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void registrationStreamRetainsCompletedAwaitRepairEvenWhenTheBindingIsFenced(boolean superseded) {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableActionInvoker actions = mock(AwsDurableActionInvoker.class);
        AwsDurableWakeupService wakeups = mock(AwsDurableWakeupService.class);
        AwsDurableCallbackRegistration registration = registration();
        AwsDurableAwaitCheckpoint semantic = new AwsDurableAwaitCheckpoint(
            "tenant", "execution", "interaction", "correlation", "unit", "step", "COMPLETED",
            "pipeline", "contract", "release");
        when(actions.invoke(org.mockito.ArgumentMatchers.any()))
            .thenReturn(AwsDurableActionResponse.awaitCheckpoint(semantic));
        if (superseded) {
            org.mockito.Mockito.doThrow(new AwsDurableStaleGenerationException())
                .when(bindings).bind(org.mockito.ArgumentMatchers.any());
        }
        when(wakeups.wake(org.mockito.ArgumentMatchers.any()))
            .thenReturn(AwsDurableWakeupDisposition.RETRY);

        AwsDurableWakeupDisposition disposition = new AwsDurableAwaitStreamProcessor(bindings, actions, wakeups, AWAIT_STREAM, BINDING_STREAM,
            ignored -> { throw new AssertionError("mechanical records must not enter the semantic decoder"); })
            .process(record(BINDING_STREAM, registrationImage(registration)));

        assertThat(disposition).isEqualTo(AwsDurableWakeupDisposition.RETRY);
        verify(bindings).bind(registration.bind(new org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity(
            "tenant", "execution", "interaction", "correlation", 2)));
        verify(wakeups).wake(new org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity(
            "tenant", "execution", "interaction", "correlation", 2));
    }

    @Test
    void secondRegistrationReadsItsPinnedCompletedInteractionWithoutListingHistory() {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var wakeups = mock(AwsDurableWakeupService.class);
        var second = new org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity(
            "tenant", "execution", "interaction-2", "correlation-2", 2, 2);
        var original = registration();
        var registration = new AwsDurableCallbackRegistration(original.checkpoint(), original.providerExecutionName(),
            original.providerExecutionArn(), "second-callback", 100, 1000, Optional.of(second));
        var semantic = new AwsDurableAwaitCheckpoint("tenant", "execution", "interaction-2", "correlation-2", "unit-2", "step-2",
            "COMPLETED", "pipeline", "contract", "release");
        AwsDurableActionInvoker actions = request -> {
            assertThat(request).isEqualTo(org.pipelineframework.aws.durable.model.AwsDurableActionRequest.awaitCheckpoint(second));
            return AwsDurableActionResponse.awaitCheckpoint(semantic);
        };
        when(wakeups.wake(second)).thenReturn(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        var processor = new AwsDurableAwaitStreamProcessor(bindings, actions, wakeups, AWAIT_STREAM, BINDING_STREAM,
            ignored -> { throw new AssertionError("mechanical registration is not a semantic row"); });
        assertThat(processor.process(record(BINDING_STREAM, registrationImage(registration))))
            .isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        verify(bindings).bind(registration.bind(second));
        verify(wakeups).wake(second);
    }

    @Test
    void lateFirstCompletionCannotBindOrWakeTheSecondCallback() {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var wakeups = mock(AwsDurableWakeupService.class);
        var original = registration();
        var second = new org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity(
            "tenant", "execution", "interaction-2", "correlation-2", 2, 2);
        when(bindings.findLatestRegistration("tenant", "execution")).thenReturn(Optional.of(new AwsDurableCallbackRegistration(
            original.checkpoint(), original.providerExecutionName(), original.providerExecutionArn(), "second-callback", 100, 1000,
            Optional.of(second))));
        var processor = new AwsDurableAwaitStreamProcessor(bindings, mock(AwsDurableActionInvoker.class), wakeups,
            AWAIT_STREAM, BINDING_STREAM, ignored -> Optional.of(semantic("COMPLETED")));
        assertThat(processor.process(record(AWAIT_STREAM, Map.of()))).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        verify(bindings, org.mockito.Mockito.never()).bind(org.mockito.ArgumentMatchers.any());
        org.mockito.Mockito.verifyNoInteractions(wakeups);
    }

    @Test
    void awaitRecordRetriesUntilRegistrationExists() {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var processor = new AwsDurableAwaitStreamProcessor(bindings, mock(AwsDurableActionInvoker.class),
            mock(AwsDurableWakeupService.class), AWAIT_STREAM, BINDING_STREAM,
            ignored -> Optional.of(semantic("COMPLETED")));
        assertThat(processor.process(record(AWAIT_STREAM, Map.of()))).isEqualTo(AwsDurableWakeupDisposition.RETRY);
    }

    @Test
    void terminalExecutionWithoutRegistrationDoesNotBlockTheSharedStream() {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var actions = mock(AwsDurableActionInvoker.class);
        var wakeups = mock(AwsDurableWakeupService.class);
        var processor = new AwsDurableAwaitStreamProcessor(bindings, actions, wakeups, AWAIT_STREAM, BINDING_STREAM,
            ignored -> Optional.of(semantic("COMPLETED")), ignored -> Optional.empty(), ignored -> true);

        assertThat(processor.process(record(AWAIT_STREAM, Map.of())))
            .isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        org.mockito.Mockito.verifyNoInteractions(actions, wakeups);
        verify(bindings, org.mockito.Mockito.never()).bind(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void lateRegistrationStillRepairsCompletionAfterTerminalRecordWasAcknowledged() {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var actions = mock(AwsDurableActionInvoker.class);
        var wakeups = mock(AwsDurableWakeupService.class);
        when(actions.invoke(org.mockito.ArgumentMatchers.any()))
            .thenReturn(AwsDurableActionResponse.awaitCheckpoint(semantic("COMPLETED")));
        when(wakeups.wake(org.mockito.ArgumentMatchers.any()))
            .thenReturn(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        var processor = new AwsDurableAwaitStreamProcessor(bindings, actions, wakeups, AWAIT_STREAM, BINDING_STREAM,
            ignored -> Optional.of(semantic("COMPLETED")), ignored -> Optional.empty(), ignored -> true);

        assertThat(processor.process(record(AWAIT_STREAM, Map.of())))
            .isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        assertThat(processor.process(record(BINDING_STREAM, registrationImage(registration()))))
            .isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        verify(wakeups).wake(registration().awaitIdentity().orElseThrow());
    }

    @Test
    void registeredCompletionNeverUsesTheTerminalWithoutRegistrationShortcut() {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var wakeups = mock(AwsDurableWakeupService.class);
        when(bindings.findLatestRegistration("tenant", "execution")).thenReturn(Optional.of(registration()));
        when(wakeups.wake(org.mockito.ArgumentMatchers.any()))
            .thenReturn(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        var processor = new AwsDurableAwaitStreamProcessor(bindings, mock(AwsDurableActionInvoker.class), wakeups,
            AWAIT_STREAM, BINDING_STREAM, ignored -> Optional.of(semantic("COMPLETED")), ignored -> Optional.empty(),
            ignored -> { throw new AssertionError("registered callback must retain normal fencing and delivery"); });

        assertThat(processor.process(record(AWAIT_STREAM, Map.of())))
            .isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        verify(wakeups).wake(registration().awaitIdentity().orElseThrow());
    }

    @Test
    void unavailableTerminalObservationRetainsTheOriginalStreamSequence() {
        var processor = new AwsDurableAwaitStreamProcessor(mock(AwsDurableCallbackBindingRepository.class),
            mock(AwsDurableActionInvoker.class), mock(AwsDurableWakeupService.class), AWAIT_STREAM, BINDING_STREAM,
            ignored -> Optional.of(semantic("COMPLETED")), ignored -> Optional.empty(),
            ignored -> { throw new IllegalStateException("semantic store unavailable"); });
        var event = new DynamodbEvent();
        var record = record(AWAIT_STREAM, Map.of());
        record.getDynamodb().setSequenceNumber("retain-this-sequence");
        event.setRecords(List.of(record));

        assertThat(new AwsDurableAwaitStreamBatchAdapter().handle(event, processor::process).getBatchItemFailures())
            .extracting(failure -> failure.getItemIdentifier()).containsExactly("retain-this-sequence");
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"WAITING,COMPLETED", "COMPLETED,WAITING"})
    void semanticObservationOverridesStaleImagesAndMechanicalLookingFields(String actualStatus, String imageStatus) {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var actions = mock(AwsDurableActionInvoker.class);
        var wakeups = mock(AwsDurableWakeupService.class);
        when(bindings.findLatestRegistration("tenant", "execution")).thenReturn(Optional.of(registration()));
        when(wakeups.wake(org.mockito.ArgumentMatchers.any())).thenReturn(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        var processor = new AwsDurableAwaitStreamProcessor(bindings, actions, wakeups, AWAIT_STREAM, BINDING_STREAM,
            ignored -> Optional.of(semantic(actualStatus)));
        var image = new HashMap<>(registrationImage(registration()));
        image.put("status", string(imageStatus));
        image.put("execution_id", string("stale-execution"));
        image.put("correlation_id", string("stale-correlation"));

        assertThat(processor.process(record(AWAIT_STREAM, image))).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        var identity = new org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity(
            "tenant", "execution", "interaction", "correlation", 2);
        verify(bindings).bind(registration().bind(identity));
        org.mockito.Mockito.verifyNoInteractions(actions);
        if ("COMPLETED".equals(actualStatus)) {
            verify(wakeups).wake(identity);
        } else {
            org.mockito.Mockito.verifyNoInteractions(wakeups);
        }
    }

    @Test
    void missingSemanticObservationKeepsTheRecordRetryable() {
        var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var processor = new AwsDurableAwaitStreamProcessor(bindings, mock(AwsDurableActionInvoker.class),
            mock(AwsDurableWakeupService.class), AWAIT_STREAM, BINDING_STREAM, ignored -> Optional.empty());
        assertThat(processor.process(record(AWAIT_STREAM, Map.of()))).isEqualTo(AwsDurableWakeupDisposition.RETRY);
        org.mockito.Mockito.verifyNoInteractions(bindings);
    }

    @Test
    void unexpectedStreamIsReportedAsABatchFailure() {
        var processor = new AwsDurableAwaitStreamProcessor(mock(AwsDurableCallbackBindingRepository.class),
            mock(AwsDurableActionInvoker.class), mock(AwsDurableWakeupService.class), AWAIT_STREAM, BINDING_STREAM,
            ignored -> { throw new AssertionError("unexpected source must not be decoded"); });
        var record = record(AWAIT_STREAM + "-other", Map.of());
        record.getDynamodb().setSequenceNumber("unexpected");
        var event = new DynamodbEvent();
        event.setRecords(List.of(record));
        assertThat(new AwsDurableAwaitStreamBatchAdapter().handle(event, processor::process).getBatchItemFailures())
            .extracting(item -> item.getItemIdentifier()).containsExactly("unexpected");
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {AWAIT_STREAM, BINDING_STREAM})
    void removalDoesNotTurnExpiryIntoSemanticCompletion(String source) {
        var processor = new AwsDurableAwaitStreamProcessor(mock(AwsDurableCallbackBindingRepository.class),
            mock(AwsDurableActionInvoker.class), mock(AwsDurableWakeupService.class), AWAIT_STREAM, BINDING_STREAM,
            ignored -> { throw new AssertionError("deletion is not a completion signal"); });
        var record = record(source, Map.of());
        record.setEventName("REMOVE");
        assertThat(processor.process(record)).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
    }

    @Test
    void nonRegistrationMechanicalRecordsNeverEnterTheSemanticPath() {
        var processor = new AwsDurableAwaitStreamProcessor(mock(AwsDurableCallbackBindingRepository.class),
            mock(AwsDurableActionInvoker.class), mock(AwsDurableWakeupService.class), AWAIT_STREAM, BINDING_STREAM,
            ignored -> { throw new AssertionError("mechanical evidence must stay mechanical"); });
        assertThat(processor.process(record(BINDING_STREAM, Map.of("record_type", string("GENERATION_CLAIM")))))
            .isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
    }

    private static AwsDurableAwaitCheckpoint semantic(String status) {
        return new AwsDurableAwaitCheckpoint("tenant", "execution", "interaction", "correlation", "unit", "step",
            status, "pipeline", "contract", "release");
    }

    private static AwsDurableCallbackRegistration registration() {
        return new AwsDurableCallbackRegistration(
            new AwsDurableDriverCheckpoint(
                new AwsDurableExecutionCheckpoint("tenant", "execution", "pipeline", "contract", "release"), 2),
            "durable-name", "durable-arn", "callback", 1_000L, 9_999L, Optional.of(new org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity(
                "tenant", "execution", "interaction", "correlation", 2)));
    }

    private static Map<String, AttributeValue> registrationImage(AwsDurableCallbackRegistration registration) {
        Map<String, AttributeValue> image = new HashMap<>();
        image.put("record_type", string("REGISTRATION"));
        image.put("tenant_id", string(registration.checkpoint().tenantId()));
        image.put("execution_id", string(registration.checkpoint().executionId()));
        image.put("pipeline_id", string(registration.checkpoint().pipelineId()));
        image.put("contract_version", string(registration.checkpoint().contractVersion()));
        image.put("release_version", string(registration.checkpoint().releaseVersion()));
        image.put("generation", number(registration.checkpoint().generation()));
        image.put("provider_execution_name", string(registration.providerExecutionName()));
        image.put("provider_execution_arn", string(registration.providerExecutionArn()));
        image.put("provider_callback_id", string(registration.providerCallbackId()));
        image.put("created_at_epoch_ms", number(registration.createdAtEpochMs()));
        image.put("expires_at_epoch_s", number(registration.expiresAtEpochS()));
        registration.awaitIdentity().ifPresent(identity -> {
            image.put("interaction_id", string(identity.interactionId()));
            image.put("correlation_id", string(identity.correlationId()));
            image.put("occurrence", number(identity.occurrence()));
        });
        return image;
    }

    private static DynamodbEvent.DynamodbStreamRecord record(String source, Map<String, AttributeValue> image) {
        DynamodbEvent.DynamodbStreamRecord record = new DynamodbEvent.DynamodbStreamRecord();
        record.setEventSourceARN(source);
        record.setEventName("MODIFY");
        StreamRecord stream = new StreamRecord();
        stream.setNewImage(image);
        record.setDynamodb(stream);
        return record;
    }

    private static AttributeValue string(String value) {
        return new AttributeValue().withS(value);
    }

    private static AttributeValue number(long value) {
        return new AttributeValue().withN(Long.toString(value));
    }
}
