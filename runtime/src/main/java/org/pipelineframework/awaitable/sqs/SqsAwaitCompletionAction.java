package org.pipelineframework.awaitable.sqs;

import java.time.Duration;
import java.util.Objects;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCompletionAdmissionFailures;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitTelemetry;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.SqsInboundMessage;
import org.pipelineframework.orchestrator.SqsMessageDisposition;

/**
 * Admits one SQS await-completion message without receiving or acknowledging it.
 */
@ApplicationScoped
public class SqsAwaitCompletionAction {

    private static final Logger LOG = Logger.getLogger(SqsAwaitCompletionAction.class);

    @Inject
    PipelineExecutionService executionService;

    @Inject
    AwaitTelemetry awaitTelemetry = AwaitTelemetry.disabled();

    public SqsAwaitCompletionAction() {
    }

    SqsAwaitCompletionAction(
        PipelineExecutionService executionService,
        AwaitTelemetry awaitTelemetry
    ) {
        this.executionService = executionService;
        this.awaitTelemetry = awaitTelemetry;
    }

    public Uni<SqsMessageDisposition> handle(SqsInboundMessage message, Duration completionTimeout) {
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(completionTimeout, "completionTimeout");
        if (message.body().isEmpty()) {
            LOG.warnf(
                "Leaving SQS await completion message with null body for queue redrive id=%s",
                message.messageId().orElse("<missing>"));
            return Uni.createFrom().item(SqsMessageDisposition.RETRY);
        }

        SqsAwaitCompletionEnvelope envelope;
        try {
            envelope = PipelineJson.mapper().readValue(message.body().orElseThrow(), SqsAwaitCompletionEnvelope.class);
        } catch (Exception e) {
            LOG.warnf(
                e,
                "Leaving malformed SQS await completion message for queue redrive id=%s",
                message.messageId().orElse("<missing>"));
            return Uni.createFrom().item(SqsMessageDisposition.RETRY);
        }

        return executionService.completeAwaitInteraction(new AwaitCompletionCommand(
                envelope.tenantId(),
                envelope.interactionId(),
                envelope.correlationId(),
                envelope.resumeToken(),
                envelope.idempotencyKey(),
                envelope.responsePayload(),
                envelope.actor(),
                System.currentTimeMillis()))
            .ifNoItem().after(completionTimeout).fail()
            .replaceWith(SqsMessageDisposition.ACKNOWLEDGE)
            .onFailure().recoverWithItem(failure -> dispositionForFailure(envelope, failure));
    }

    private SqsMessageDisposition dispositionForFailure(
        SqsAwaitCompletionEnvelope envelope,
        Throwable failure
    ) {
        if (AwaitCompletionAdmissionFailures.isDeterministic(failure)) {
            String reason = AwaitCompletionAdmissionFailures.reason(failure);
            awaitTelemetry.recordDroppedCompletion("sqs", reason);
            LOG.warnf(failure, "Dropping deterministic SQS await completion message: reason=%s", reason);
            return SqsMessageDisposition.ACKNOWLEDGE;
        }
        LOG.errorf(failure, "Failed admitting SQS await completion interactionId=%s correlationId=%s",
            envelope.interactionId(),
            envelope.correlationId());
        return SqsMessageDisposition.RETRY;
    }
}
