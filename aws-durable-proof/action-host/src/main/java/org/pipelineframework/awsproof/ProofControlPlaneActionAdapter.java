package org.pipelineframework.awsproof;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.aws.durable.AwsDurableActionInvoker;
import org.pipelineframework.aws.durable.AwsDurableCallbackBindingRepository;
import org.pipelineframework.aws.durable.AwsDurableControlPlaneActions;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.aws.durable.model.AwsDurableActionResponse;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Fault instrumentation around production semantic actions; no duplicate action implementation. */
@ApplicationScoped
final class ProofControlPlaneActionAdapter {
    private static final Duration ACTION_TIMEOUT = Duration.ofSeconds(30);
    private final AwsDurableActionInvoker actions;
    private final ProofFaultInjector faults;
    private final ProofReleaseCatalog releaseCatalog;

    @Inject
    ProofControlPlaneActionAdapter(
        PipelineControlPlane controlPlane,
        ObjectMapper mapper,
        DynamoDbClient dynamo,
        ProofFaultInjector faults,
        ProofReleaseCatalog releaseCatalog
    ) {
        this(new AwsDurableControlPlaneActions(
            controlPlane,
            new AwsDurableCallbackBindingRepository(dynamo,
                Optional.ofNullable(System.getenv("TPF_PROOF_BINDING_TABLE"))
                    .filter(value -> !value.isBlank())
                    .orElseThrow(() -> new IllegalStateException("TPF_PROOF_BINDING_TABLE is required"))),
            inputJson -> {
                try {
                    return mapper.readValue(inputJson, ProofPipelineInput.class);
                } catch (JsonProcessingException malformed) {
                    throw new IllegalArgumentException("inputJson must contain a valid proof pipeline input", malformed);
                }
            },
            mapper, ACTION_TIMEOUT, Duration.ofDays(400)), faults, releaseCatalog);
    }

    ProofControlPlaneActionAdapter(
        AwsDurableActionInvoker actions,
        ProofFaultInjector faults,
        ProofReleaseCatalog releaseCatalog
    ) {
        this.actions = Objects.requireNonNull(actions, "actions");
        this.faults = Objects.requireNonNull(faults, "faults");
        this.releaseCatalog = Objects.requireNonNull(releaseCatalog, "releaseCatalog");
    }

    AwsDurableActionResponse handle(AwsDurableActionRequest request) {
        Objects.requireNonNull(request, "request");
        return switch (request.operation()) {
            case SUBMIT -> submit(request);
            case STATUS, RESULT, REDRIVE, SWEEP, QUERY_PENDING_AWAIT, READ_AWAIT_CHECKPOINT,
                READ_EXECUTION_AWAITS -> actions.invoke(request);
            case REGISTER_CALLBACK, BIND_CALLBACK -> throw new IllegalArgumentException(
                request.operation() + " is an AWS Durable host operation");
        };
    }

    private AwsDurableActionResponse submit(AwsDurableActionRequest request) {
        faults.failIfArmed("submit-before-tpf-commit", required(request.idempotencyKey(), "idempotencyKey"));
        releaseCatalog.ensureRegistered(request.tenantId(),
                required(request.pipelineId(), "pipelineId"),
                required(request.contractVersion(), "contractVersion"),
                required(request.releaseVersion(), "releaseVersion"))
            .await().atMost(ACTION_TIMEOUT);
        AwsDurableActionResponse response = actions.invoke(request);
        faults.failIfArmed("submit-after-tpf-commit", response.checkpoint()
            .orElseThrow(() -> new IllegalStateException("TPF submit action returned no checkpoint")).executionId());
        return response;
    }

    private static String required(Optional<String> value, String name) {
        return value.filter(candidate -> !candidate.isBlank())
            .orElseThrow(() -> new IllegalArgumentException(name + " is required"));
    }
}
