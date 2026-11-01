package org.pipelineframework.orchestrator;

import java.time.Duration;
import java.util.Objects;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Processes one SQS work-item message without receiving or acknowledging it.
 */
@ApplicationScoped
public class SqsWorkItemAction {

    private static final Logger LOG = Logger.getLogger(SqsWorkItemAction.class);
    private static final Duration PROCESS_TIMEOUT = Duration.ofMinutes(5);

    @Inject
    PipelineExecutionService pipelineExecutionService;

    private Duration processTimeout = PROCESS_TIMEOUT;

    public SqsWorkItemAction() {
    }

    SqsWorkItemAction(PipelineExecutionService pipelineExecutionService) {
        this.pipelineExecutionService = pipelineExecutionService;
    }

    SqsWorkItemAction(PipelineExecutionService pipelineExecutionService, Duration processTimeout) {
        this.pipelineExecutionService = pipelineExecutionService;
        this.processTimeout = Objects.requireNonNull(processTimeout, "processTimeout");
    }

    public Uni<SqsMessageDisposition> handle(SqsInboundMessage message) {
        Objects.requireNonNull(message, "message");
        if (message.body().isEmpty()) {
            LOG.warnf("Dropping SQS work message with null body id=%s", message.messageId().orElse("<missing>"));
            return Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE);
        }

        ExecutionWorkItem workItem;
        try {
            workItem = PipelineJson.mapper().readValue(message.body().orElseThrow(), ExecutionWorkItem.class);
        } catch (Exception e) {
            LOG.warnf(e, "Dropping malformed SQS work message id=%s", message.messageId().orElse("<missing>"));
            return Uni.createFrom().item(SqsMessageDisposition.ACKNOWLEDGE);
        }

        return pipelineExecutionService.processExecutionWorkItem(workItem)
            .ifNoItem().after(processTimeout).fail()
            .replaceWith(SqsMessageDisposition.ACKNOWLEDGE)
            .onFailure().invoke(failure -> LOG.errorf(
                failure,
                "Failed processing SQS work item executionId=%s",
                workItem.executionId()))
            .onFailure().recoverWithItem(SqsMessageDisposition.RETRY);
    }
}
