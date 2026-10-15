package org.pipelineframework.aws.durable;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import org.pipelineframework.orchestrator.SqsMessageDisposition;

/** Drives bounded semantic due work independently of suspended provider executions. */
@Named("aws-durable-due-work")
@ApplicationScoped
public final class AwsDurableDueWorkHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private final PipelineControlPlane controlPlane;
    private final Clock clock;

    @Inject
    public AwsDurableDueWorkHandler(PipelineControlPlane controlPlane) {
        this(controlPlane, Clock.systemUTC());
    }

    AwsDurableDueWorkHandler(PipelineControlPlane controlPlane, Clock clock) {
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        // A tick is a hint, not a persisted deadline or authority supplied by its payload.
        return new AwsDurableSqsBatchAdapter().handle(event, message ->
            controlPlane.sweepOnce(clock.millis()).replaceWith(SqsMessageDisposition.ACKNOWLEDGE),
            Duration.ofSeconds(30));
    }
}
