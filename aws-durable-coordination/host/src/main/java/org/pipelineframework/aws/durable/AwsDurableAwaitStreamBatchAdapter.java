package org.pipelineframework.aws.durable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.StreamsEventResponse;
import org.pipelineframework.aws.durable.model.AwsDurableAwaitIdentity;

/** Partial-batch adapter for Await Stream records; schema decoding remains in generated application glue. */
public final class AwsDurableAwaitStreamBatchAdapter {
    public StreamsEventResponse handle(
        DynamodbEvent event,
        Function<DynamodbEvent.DynamodbStreamRecord, Optional<AwsDurableAwaitIdentity>> identityDecoder,
        Function<AwsDurableAwaitIdentity, AwsDurableWakeupDisposition> wakeup
    ) {
        return handle(event, record -> identityDecoder.apply(record)
            .map(wakeup)
            .orElse(AwsDurableWakeupDisposition.ACKNOWLEDGE));
    }

    /** Handles one bounded record action and reports only retryable records to Lambda. */
    public StreamsEventResponse handle(
        DynamodbEvent event,
        Function<DynamodbEvent.DynamodbStreamRecord, AwsDurableWakeupDisposition> action
    ) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(action, "action");
        List<StreamsEventResponse.BatchItemFailure> failures = new ArrayList<>();
        for (DynamodbEvent.DynamodbStreamRecord record : event.getRecords()) {
            String itemIdentifier = Optional.ofNullable(record.getDynamodb())
                .map(com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord::getSequenceNumber)
                .filter(value -> !value.isBlank())
                .orElse("");
            try {
                if (action.apply(record) == AwsDurableWakeupDisposition.RETRY) {
                    failures.add(new StreamsEventResponse.BatchItemFailure(itemIdentifier));
                }
            } catch (RuntimeException failure) {
                failures.add(new StreamsEventResponse.BatchItemFailure(itemIdentifier));
            }
        }
        return new StreamsEventResponse(List.copyOf(failures));
    }
}
