package org.pipelineframework.awsproof;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;
import org.pipelineframework.awsproof.model.ProofAwaitIdentity;
import org.pipelineframework.awsproof.model.ProofCallbackBinding;
import org.pipelineframework.awsproof.model.ProofCallbackRegistration;
import org.pipelineframework.awsproof.model.ProofDriverCheckpoint;
import org.pipelineframework.awaitable.AwaitCompletionDescriptorRegistry;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.Event;
import software.amazon.awssdk.services.lambda.model.Execution;
import software.amazon.awssdk.services.lambda.model.ExecutionStatus;
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionHistoryRequest;
import software.amazon.awssdk.services.lambda.model.ListDurableExecutionsByFunctionRequest;

/** Reconstructs disposable callback bindings from active AWS durable state. */
@ApplicationScoped
final class ProofDurableBindingResolver {
    private static final Logger LOG = Logger.getLogger(ProofDurableBindingResolver.class);
    private static final Duration ACTION_TIMEOUT = Duration.ofSeconds(30);

    @Inject
    LambdaClient lambda;

    @Inject
    ObjectMapper mapper;

    @Inject
    PipelineControlPlane controlPlane;

    @Inject
    AwaitCompletionDescriptorRegistry descriptorRegistry;

    @Inject
    ProofAwaitDescriptorFactory descriptorFactory;

    @Inject
    DynamoDbClient dynamo;

    Optional<ProofCallbackBinding> reconstruct(ProofAwaitIdentity identity) {
        return reconstructRegistration(identity.tenantId(), identity.executionId())
            .filter(registration -> registration.checkpoint().generation() == identity.generation())
            .map(registration -> registration.bind(identity));
    }

    Optional<ProofCallbackRegistration> reconstructRegistration(String tenantId, String executionId) {
        return runningExecutions().stream()
            .map(this::registration)
            .flatMap(Optional::stream)
            .filter(registration -> registration.checkpoint().tenantId().equals(tenantId)
                && registration.checkpoint().executionId().equals(executionId))
            .findFirst();
    }

    List<ProofCallbackBinding> reconstructOpenBindings() {
        return reconstructOpenBindings(runningExecutions());
    }

    List<ProofCallbackBinding> reconstructOpenBindings(List<Execution> executions) {
        List<ProofCallbackBinding> recovered = new ArrayList<>();
        for (Execution execution : executions) {
            try {
                reconstruct(execution).ifPresent(recovered::add);
            } catch (RuntimeException failure) {
                LOG.warnf(failure,
                    "Open-binding reconstruction failed for execution=%s",
                    execution.durableExecutionArn());
            }
        }
        return List.copyOf(recovered);
    }

    private Optional<ProofCallbackBinding> reconstruct(Execution execution) {
        Optional<ProofCallbackRegistration> registration = registration(execution);
        if (registration.isEmpty()) {
            return Optional.empty();
        }
        descriptorRegistry.register(descriptorFactory.create());
        ProofDriverCheckpoint checkpoint = registration.orElseThrow().checkpoint();
        Optional<ProofAwaitIdentity> identity = controlPlane.queryPendingAwaitInteractions(
                checkpoint.tenantId(), "", "", "", 100)
            .await().atMost(ACTION_TIMEOUT).stream()
            .filter(record -> checkpoint.executionId().equals(record.executionId()))
            .findFirst()
            .map(record -> new ProofAwaitIdentity(
                record.tenantId(),
                record.executionId(),
                record.interactionId(),
                record.correlationId(),
                checkpoint.generation()));
        if (identity.isEmpty()) {
            identity = findSemanticAwait(checkpoint);
        }
        return identity.map(registration.orElseThrow()::bind);
    }

    private Optional<ProofAwaitIdentity> findSemanticAwait(ProofDriverCheckpoint checkpoint) {
        return findSemanticAwait(
            checkpoint,
            requiredEnvironment("PIPELINE_ORCHESTRATOR_DYNAMO_AWAIT_INTERACTION_TABLE"));
    }

    Optional<ProofAwaitIdentity> findSemanticAwait(
        ProofDriverCheckpoint checkpoint,
        String tableName
    ) {
        Map<String, AttributeValue> startKey = Map.of();
        do {
            QueryRequest.Builder request = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("tenant_id = :tenant")
                .filterExpression("execution_id = :execution")
                .expressionAttributeValues(Map.of(
                    ":tenant", AttributeValue.fromS(checkpoint.tenantId()),
                    ":execution", AttributeValue.fromS(checkpoint.executionId())))
                .consistentRead(true);
            if (!startKey.isEmpty()) {
                request.exclusiveStartKey(startKey);
            }
            var response = dynamo.query(request.build());
            Optional<ProofAwaitIdentity> identity = response.items().stream().findFirst()
                .map(item -> new ProofAwaitIdentity(
                    attribute(item, "tenant_id"),
                    attribute(item, "execution_id"),
                    attribute(item, "interaction_id"),
                    attribute(item, "correlation_id"),
                    checkpoint.generation()));
            if (identity.isPresent()) {
                return identity;
            }
            startKey = response.lastEvaluatedKey();
        } while (startKey != null && !startKey.isEmpty());
        return Optional.empty();
    }

    private static String attribute(java.util.Map<String, AttributeValue> item, String name) {
        return Optional.ofNullable(item.get(name))
            .map(AttributeValue::s)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalStateException("Await attribute is missing: " + name));
    }

    private Optional<ProofCallbackRegistration> registration(Execution execution) {
        List<Event> events = history(execution.durableExecutionArn());
        Optional<ProofDriverCheckpoint> checkpoint = ProofDurableHistory
            .successfulStepPayload(events, "submit-tpf-execution")
            .or(() -> ProofDurableHistory.successfulStepPayload(events, "resume-tpf-execution"))
            .flatMap(this::parseCheckpoint);
        Optional<Event> callback = ProofDurableHistory.openCallback(events, "await-completion-callback");
        if (checkpoint.isEmpty() || callback.isEmpty()) {
            return Optional.empty();
        }
        long now = Instant.now().toEpochMilli();
        return Optional.of(new ProofCallbackRegistration(
            checkpoint.orElseThrow(),
            execution.durableExecutionName(),
            execution.durableExecutionArn(),
            callback.orElseThrow().callbackStartedDetails().callbackId(),
            now,
            Instant.ofEpochMilli(now).plus(400, ChronoUnit.DAYS).getEpochSecond()));
    }

    private List<Execution> runningExecutions() {
        List<Execution> executions = new ArrayList<>();
        String marker = "";
        do {
            var request = ListDurableExecutionsByFunctionRequest.builder()
                .functionName(requiredEnvironment("TPF_PROOF_DURABLE_FUNCTION"))
                .statuses(ExecutionStatus.RUNNING)
                .maxItems(100);
            if (!marker.isBlank()) {
                request.marker(marker);
            }
            var response = lambda.listDurableExecutionsByFunction(request.build());
            executions.addAll(response.durableExecutions());
            marker = Optional.ofNullable(response.nextMarker()).orElse("");
        } while (!marker.isBlank());
        return List.copyOf(executions);
    }

    private List<Event> history(String executionArn) {
        List<Event> events = new ArrayList<>();
        String marker = "";
        do {
            var request = GetDurableExecutionHistoryRequest.builder()
                .durableExecutionArn(executionArn)
                .includeExecutionData(true)
                .maxItems(1000);
            if (!marker.isBlank()) {
                request.marker(marker);
            }
            var response = lambda.getDurableExecutionHistory(request.build());
            events.addAll(response.events());
            marker = Optional.ofNullable(response.nextMarker()).orElse("");
        } while (!marker.isBlank());
        return List.copyOf(events);
    }

    private Optional<ProofDriverCheckpoint> parseCheckpoint(String json) {
        if (json == null || json.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(mapper.readValue(json, ProofDriverCheckpoint.class));
        } catch (JsonProcessingException malformed) {
            return Optional.empty();
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required");
        }
        return value;
    }
}
