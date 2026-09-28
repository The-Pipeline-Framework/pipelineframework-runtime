package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;

import software.amazon.awssdk.services.sqs.model.Message;

/**
 * Receipt-independent SQS message input for a single coordinator action.
 */
public record SqsInboundMessage(Optional<String> messageId, Optional<String> body) {

    public SqsInboundMessage {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(body, "body");
    }

    public static SqsInboundMessage from(Message message) {
        Objects.requireNonNull(message, "message");
        return new SqsInboundMessage(
            Optional.ofNullable(message.messageId()),
            Optional.ofNullable(message.body()));
    }
}
