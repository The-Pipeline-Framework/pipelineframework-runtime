package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.aws.durable.model.*;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import software.amazon.lambda.durable.DurableContext;
import software.amazon.lambda.durable.StepContext;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

/** Production actions, driver, repository and repair; only provider/storage transports are substituted. */
class AwsDurableAdmissionRecoveryTest {
    private static final String ARN = "arn:aws:lambda:eu-west-1:123456789012:function:driver:7/durable-execution/original/id";
    private static final AwsDurableExecutionInput INPUT = new AwsDurableExecutionInput("tenant-a", "stable-key",
        "pipeline-a", "contract-a", "release-a", "{}", Optional.empty(), 1);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void providerLossAfterAdmissionBeforeExecutionLocatorConvergesWithoutResubmission(boolean loseSubmitAcknowledgement) {
        var fixture = new Fixture();
        var context = fixture.context();
        if (loseSubmitAcknowledgement) fixture.loseSubmitAcknowledgement = true;
        else doThrow(new IllegalStateException("injected loss before execution locator"))
            .when(context).step(eq("register-execution-recovery"), eq(Boolean.class),
                org.mockito.ArgumentMatchers.<Function<StepContext, Boolean>>any());
        assertThatThrownBy(() -> new AwsDurableCoordinatorHandler(fixture.actions).handleRequest(INPUT, context))
            .hasMessageContaining("injected loss");
        assertThat(fixture.admissions.get()).isEqualTo(1);
        assertThat(fixture.repository.findRecoveryByProviderExecutionArn(ARN)).isEmpty();
        assertThat(fixture.repository.findAdmissionByProviderExecutionArn(ARN)).isPresent();
        for (int retry = 0; retry < 2; retry++) assertThat(fixture.reconciler.reconcile(ARN)).isTrue();
        verify(fixture.pinned, times(2)).start(argThat(input -> input.resumeExecutionId().equals(Optional.of("execution-a"))
            && input.generation() == 2 && input.releaseVersion().equals("release-a")));
        assertThat(fixture.admissions.get()).isEqualTo(1);
        verify(fixture.controlPlane, never()).redriveExecution(anyString(), anyString(), anyLong(), anyBoolean(), anyString());
    }

    @Test
    void retainedLocatorBeforeAdmissionDoesNotResubmitAndLateAdmissionCanStillRecover() {
        var fixture = new Fixture();
        fixture.actions.invoke(AwsDurableActionRequest.registerAdmission(INPUT, "provider-original", ARN));
        for (int retry = 0; retry < 2; retry++) {
            assertThatThrownBy(() -> fixture.reconciler.reconcile(ARN)).hasMessageContaining("retry without resubmission");
        }
        assertThat(fixture.admissions.get()).isZero();
        verifyNoInteractions(fixture.pinned);
        fixture.actions.invoke(AwsDurableActionRequest.submit(INPUT));
        assertThat(fixture.reconciler.reconcile(ARN)).isTrue();
        assertThat(fixture.admissions.get()).isEqualTo(1);
    }

    @Test
    void clientKeyCollisionWithAnotherReleaseCannotBeAdopted() {
        var fixture = new Fixture();
        fixture.actions.invoke(AwsDurableActionRequest.registerAdmission(INPUT, "provider-original", ARN));
        fixture.semantic.set(Optional.of(new ExecutionSemanticCheckpoint("tenant-a", "foreign-execution", "pipeline-a",
            "contract-a", "other-release", ExecutionStatus.RUNNING)));
        assertThatThrownBy(() -> fixture.reconciler.reconcile(ARN)).hasMessageContaining("pipeline and release");
        verifyNoInteractions(fixture.pinned);
        assertThat(fixture.admissions.get()).isZero();
    }

    private static final class Fixture {
        private final AtomicInteger admissions = new AtomicInteger();
        private final AtomicReference<Optional<ExecutionSemanticCheckpoint>> semantic = new AtomicReference<>(Optional.empty());
        private final PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
        private final AwsDurableExecutionStarter pinned = mock(AwsDurableExecutionStarter.class);
        private final AwsDurableCallbackBindingRepository repository;
        private final AwsDurableControlPlaneActions actions;
        private final AwsDurableTerminalReconciler reconciler;
        private boolean loseSubmitAcknowledgement;

        private Fixture() {
            when(controlPlane.executePipelineAsync(any(), eq("tenant-a"), eq("stable-key"), eq(false), eq("pipeline-a"),
                eq("contract-a"), eq("release-a"))).thenAnswer(call -> {
                admissions.incrementAndGet();
                semantic.set(Optional.of(new ExecutionSemanticCheckpoint("tenant-a", "execution-a", "pipeline-a", "contract-a",
                    "release-a", ExecutionStatus.RUNNING)));
                if (loseSubmitAcknowledgement) return Uni.createFrom().failure(new IllegalStateException("injected loss of submit acknowledgement"));
                return Uni.createFrom().item(new RunAsyncAcceptedDto("execution-a", false, "/pipeline/executions/execution-a", 1L));
            });
            when(controlPlane.getExecutionSemanticCheckpointByClientKey("tenant-a", "stable-key"))
                .thenAnswer(call -> Uni.createFrom().item(semantic.get()));
            when(controlPlane.getExecutionSemanticCheckpoint("tenant-a", "execution-a"))
                .thenAnswer(call -> Uni.createFrom().item(semantic.get().orElseThrow()));
            // No transaction guarantees are inferred from this storage transport fake. Generation races have a separate DynamoDB test.
            var rows = new LinkedHashMap<Map<String, AttributeValue>, Map<String, AttributeValue>>();
            var dynamo = mock(DynamoDbClient.class);
            when(dynamo.putItem(any(PutItemRequest.class))).thenAnswer(call -> {
                PutItemRequest request = call.getArgument(0);
                rows.put(key(request.item()), request.item());
                return PutItemResponse.builder().build();
            });
            when(dynamo.transactWriteItems(any(TransactWriteItemsRequest.class))).thenAnswer(call -> {
                TransactWriteItemsRequest request = call.getArgument(0);
                request.transactItems().stream().filter(item -> item.put() != null)
                    .forEach(item -> rows.put(key(item.put().item()), item.put().item()));
                return TransactWriteItemsResponse.builder().build();
            });
            when(dynamo.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
                GetItemRequest request = call.getArgument(0);
                return GetItemResponse.builder().item(rows.getOrDefault(request.key(), Map.of())).build();
            });
            when(dynamo.query(any(QueryRequest.class))).thenAnswer(call -> {
                QueryRequest request = call.getArgument(0);
                String prefix = request.expressionAttributeValues().values().stream().map(AttributeValue::s)
                    .filter(value -> value.endsWith("#")).findFirst().orElseThrow();
                var matches = rows.values().stream().filter(row -> request.expressionAttributeValues().get(":arn")
                    .equals(row.get("provider_execution_arn")) && row.get("sk").s().startsWith(prefix)).toList();
                return QueryResponse.builder().items(matches).build();
            });
            repository = new AwsDurableCallbackBindingRepository(dynamo, "mechanical-table");
            actions = new AwsDurableControlPlaneActions(controlPlane, repository, input -> input, new ObjectMapper(),
                Duration.ofSeconds(1), Duration.ofDays(1));
            var starter = mock(AwsDurableExecutionStarter.class);
            when(starter.pinnedToExecution(ARN)).thenReturn(pinned);
            when(pinned.start(any())).thenReturn(new AwsDurableStartResponse("replacement", 202));
            reconciler = new AwsDurableTerminalReconciler(repository, controlPlane, starter,
                ignored -> AwsDurableProviderExecutionState.MISSING, Duration.ofSeconds(1));
        }

        private DurableContext context() {
            var context = mock(DurableContext.class);
            when(context.getExecutionArn()).thenReturn(ARN);
            doAnswer(call -> {
                Function<StepContext, Object> action = call.getArgument(2);
                return action.apply(mock(StepContext.class));
            }).when(context).step(anyString(), any(Class.class),
                org.mockito.ArgumentMatchers.<Function<StepContext, Object>>any());
            return context;
        }

        private static Map<String, AttributeValue> key(Map<String, AttributeValue> row) {
            return Map.of("pk", row.get("pk"), "sk", row.get("sk"));
        }
    }
}
