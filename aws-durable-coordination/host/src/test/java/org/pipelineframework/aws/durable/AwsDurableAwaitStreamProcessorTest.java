package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Test
    void registrationStreamRepairsCompletedAwaitThatArrivedFirst() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableActionInvoker actions = mock(AwsDurableActionInvoker.class);
        AwsDurableWakeupService wakeups = mock(AwsDurableWakeupService.class);
        AwsDurableCallbackRegistration registration = registration();
        AwsDurableAwaitCheckpoint semantic = new AwsDurableAwaitCheckpoint(
            "tenant", "execution", "interaction", "correlation", "unit", "step", "COMPLETED",
            "pipeline", "contract", "release");
        when(actions.invoke(org.mockito.ArgumentMatchers.any()))
            .thenReturn(AwsDurableActionResponse.awaitCheckpoints(List.of(semantic)));
        when(wakeups.wake(org.mockito.ArgumentMatchers.any()))
            .thenReturn(AwsDurableWakeupDisposition.ACKNOWLEDGE);

        AwsDurableWakeupDisposition disposition = new AwsDurableAwaitStreamProcessor(bindings, actions, wakeups)
            .process(record(registrationImage(registration)));

        assertThat(disposition).isEqualTo(AwsDurableWakeupDisposition.ACKNOWLEDGE);
        verify(bindings).bind(registration.bind(new org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity(
            "tenant", "execution", "interaction", "correlation", 2)));
        verify(wakeups).wake(new org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity(
            "tenant", "execution", "interaction", "correlation", 2));
    }

    @Test
    void awaitRecordRetriesUntilRegistrationExists() {
        AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
        AwsDurableAwaitStreamProcessor processor = new AwsDurableAwaitStreamProcessor(
            bindings, mock(AwsDurableActionInvoker.class), mock(AwsDurableWakeupService.class));

        Map<String, AttributeValue> image = new HashMap<>();
        image.put("tenant_id", string("tenant"));
        image.put("execution_id", string("execution"));
        image.put("interaction_id", string("interaction"));
        image.put("correlation_id", string("correlation"));
        image.put("status", string("COMPLETED"));

        assertThat(processor.process(record(image))).isEqualTo(AwsDurableWakeupDisposition.RETRY);
    }

    private static AwsDurableCallbackRegistration registration() {
        return new AwsDurableCallbackRegistration(
            new AwsDurableDriverCheckpoint(
                new AwsDurableExecutionCheckpoint("tenant", "execution", "pipeline", "contract", "release"), 2),
            "durable-name", "durable-arn", "callback", 1_000L, 9_999L);
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
        return image;
    }

    private static DynamodbEvent.DynamodbStreamRecord record(Map<String, AttributeValue> image) {
        DynamodbEvent.DynamodbStreamRecord record = new DynamodbEvent.DynamodbStreamRecord();
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
