package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Optional;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.*;
import org.pipelineframework.aws.durable.model.*;
import org.pipelineframework.orchestrator.*;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

class AwsDurableHumanInteractionTest {
    private static final ObjectMapper JSON = new ObjectMapper().registerModule(new Jdk8Module());
    private static final AwsDurableExecutionCheckpoint PIN = new AwsDurableExecutionCheckpoint("tenant", "execution", "pipeline", "contract", "A1");
    private static final AwsDurableAwaitIdentity IDENTITY = new AwsDurableAwaitIdentity("tenant", "execution", "interaction", "correlation", 8);
    private static final ExecutionSemanticCheckpoint SEMANTIC = new ExecutionSemanticCheckpoint("tenant", "execution", "pipeline", "contract", "A1", ExecutionStatus.WAITING_EXTERNAL);
    private static final TypedDurablePayload CANONICAL = new TypedDurablePayload("Pending", "type-fingerprint", "catalog", JsonDurablePayloadCodec.ENCODING, 1, "{}".getBytes());

    @Test void exactReadRoundTripsCanonicalViewAndAbsenceWithoutOtherEffects() throws Exception {
        var cp = mock(PipelineControlPlane.class); var bindings = mock(AwsDurableCallbackBindingRepository.class);
        var actions = new AwsDurableControlPlaneActions(cp, bindings, input -> input, JSON, Duration.ofSeconds(2), Duration.ofDays(1));
        var view = view("A1");
        when(cp.getAwaitInteractionView(SEMANTIC, "interaction")).thenReturn(Uni.createFrom().item(Optional.of(view)), Uni.createFrom().item(Optional.empty()));
        var lambda = mock(LambdaClient.class);
        when(lambda.invoke(any(InvokeRequest.class))).thenAnswer(call -> {
            InvokeRequest wire = call.getArgument(0);
            var decoded = JSON.readValue(wire.payload().asUtf8String(), AwsDurableActionRequest.class);
            return InvokeResponse.builder().payload(SdkBytes.fromUtf8String(JSON.writeValueAsString(actions.invoke(decoded)))).build();
        });
        var invoker = new LambdaAwsDurableActionInvoker(lambda, "action:7");
        var result = JSON.readValue(invoker.invoke(AwsDurableActionRequest.readAwaitInteraction(PIN, IDENTITY)).resultJson().orElseThrow(), AwaitInteractionView.class);
        assertThat(result.checkpoint()).isEqualTo(view.checkpoint()); assertThat(result.completionPayloadTypeId()).isEqualTo("Decision");
        assertThat(result.requestPayload().payload()).isEqualTo(CANONICAL.payload());
        assertThat(invoker.invoke(AwsDurableActionRequest.readAwaitInteraction(PIN, IDENTITY)).resultJson()).contains("null");
        verify(cp, times(2)).getAwaitInteractionView(SEMANTIC, "interaction"); verifyNoMoreInteractions(cp); verifyNoInteractions(bindings);
    }

    @Test void adapterRejectsForeignReadEvidenceBeforeSerialization() {
        var cp = mock(PipelineControlPlane.class);
        when(cp.getAwaitInteractionView(SEMANTIC, "interaction")).thenReturn(Uni.createFrom().item(Optional.of(view("A2"))));
        var actions = new AwsDurableControlPlaneActions(cp, mock(AwsDurableCallbackBindingRepository.class), input -> input, JSON, Duration.ofSeconds(2), Duration.ofDays(1));
        assertThatThrownBy(() -> actions.invoke(AwsDurableActionRequest.readAwaitInteraction(PIN, IDENTITY)))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("requested execution and Release");
    }

    @Test void explicitHumanGatewayReturnsOnlyNativeSafeReceiptAndNeverCallsLegacyCompletion() throws Exception {
        var service = mock(PipelineExecutionService.class);
        var record = mock(AwaitInteractionRecord.class);
        when(record.interactionId()).thenReturn("interaction"); when(record.executionId()).thenReturn("execution");
        when(record.stepId()).thenReturn("review"); when(record.status()).thenReturn(AwaitInteractionStatus.COMPLETED);
        when(service.completeHumanAwaitInteraction(any(), any())).thenReturn(Uni.createFrom().item(new AwaitCompletionResult(record, false)));
        var gateway = new AwsDurableActionGatewayHandler(); gateway.mapper = JSON; gateway.executionService = service;
        var input = new AwsDurableHumanCompletionRequest("decision-key", CANONICAL, "trusted-reviewer");
        var request = AwsDurableActionRequest.completeHumanAwait(PIN, IDENTITY, JSON.writeValueAsString(input));
        var decoded = JSON.readValue(JSON.writeValueAsString(request), AwsDurableActionRequest.class);
        var response = gateway.handleRequest(decoded, null);
        var receipt = JSON.readTree(response.resultJson().orElseThrow());
        assertThat(receipt.size()).isEqualTo(5); assertThat(receipt.get("duplicate").asBoolean()).isFalse();
        assertThat(receipt.get("status").asText()).isEqualTo("COMPLETED");
        var command = org.mockito.ArgumentCaptor.forClass(AwaitCompletionCommand.class);
        verify(service).completeHumanAwaitInteraction(eq(SEMANTIC), command.capture()); verifyNoMoreInteractions(service);
        assertThat(command.getValue().resumeToken()).isNull(); assertThat(command.getValue().interactionId()).isEqualTo("interaction");
        assertThat(command.getValue().responsePayload()).isInstanceOf(TypedDurablePayload.class);
        assertThat(command.getValue().actor()).isEqualTo("trusted-reviewer");
    }

    @ParameterizedTest @ValueSource(strings = {"idempotencyKey", "responsePayload", "actor"})
    void malformedHumanInputFailsBeforeNativeCall(String field) throws Exception {
        var input = JSON.valueToTree(new AwsDurableHumanCompletionRequest("key", CANONICAL, "reviewer"));
        ((com.fasterxml.jackson.databind.node.ObjectNode) input).remove(field);
        var service = mock(PipelineExecutionService.class);
        var gateway = new AwsDurableActionGatewayHandler(); gateway.mapper = JSON; gateway.executionService = service;
        assertThatThrownBy(() -> gateway.handleRequest(AwsDurableActionRequest.completeHumanAwait(PIN, IDENTITY, JSON.writeValueAsString(input)), null))
            .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(service);
    }

    @Test void previousPublishedActionEnumRejectsBothNewOperationsInsteadOfLegacySubmission() throws Exception {
        for (var operation : new AwsDurableOperation[]{AwsDurableOperation.COMPLETE_HUMAN_AWAIT, AwsDurableOperation.READ_AWAIT_INTERACTION}) {
            assertThatThrownBy(() -> JSON.readValue(JSON.writeValueAsString(operation), PreviousOperation.class))
                .isInstanceOf(com.fasterxml.jackson.databind.exc.InvalidFormatException.class);
        }
        assertThat(JSON.readValue("\"SUBMIT\"", PreviousOperation.class)).isEqualTo(PreviousOperation.SUBMIT);
    }

    // Exact enum constants at Runtime main 9ddb5b030a07f1f0e1419b22e56ca5af331423c2 (#128).
    private enum PreviousOperation {
        SUBMIT, STATUS, RESULT, READ_RUNTIME_IDENTITY, CHECK_REGISTERED_WORKER, REDRIVE, SWEEP,
        PROCESS_AWAIT_CONTINUATION, QUERY_PENDING_AWAIT, READ_AWAIT_CHECKPOINT, READ_EXECUTION_AWAITS,
        READ_AWAIT_COMPLETION, QUERY_AWAIT_COMPLETIONS, RECORD_AWAIT_RESUMED, REGISTER_EXECUTION,
        REGISTER_CALLBACK, BIND_CALLBACK, READ_EXECUTION_LOCATOR, TYPED_RESULT, READ_EXECUTION_ADMISSION,
        READ_CURRENT_EXECUTION_LOCATOR
    }

    private static AwaitInteractionView view(String release) {
        return new AwaitInteractionView(new AwaitSemanticCheckpoint("tenant", "execution", "interaction", "correlation", "unit", "review",
            AwaitInteractionStatus.WAITING, "pipeline", "contract", release), CANONICAL, "Decision", 12000, "interaction-api", false);
    }
}
