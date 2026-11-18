package org.pipelineframework.aws.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.aws.durable.model.AwsDurableActionRequest;
import org.pipelineframework.orchestrator.ExecutionClientAdmission;
import org.pipelineframework.orchestrator.ExecutionSemanticCheckpoint;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.PipelineControlPlane;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.InvocationType;

class AwsDurableExecutionAdmissionTest {
    private static final ObjectMapper JSON = new ObjectMapper().registerModule(new Jdk8Module());
    private final PipelineControlPlane controlPlane = mock(PipelineControlPlane.class);
    private final AwsDurableCallbackBindingRepository bindings = mock(AwsDurableCallbackBindingRepository.class);
    private final AwsDurableInputDecoder decoder = mock(AwsDurableInputDecoder.class);
    private final AwsDurableControlPlaneActions actions = new AwsDurableControlPlaneActions(
        controlPlane, bindings, decoder, JSON, Duration.ofSeconds(5), Duration.ofDays(1));

    @Test
    void absentAdmissionRemainsPendingUntilTheAuthoritativeReadObservesIt() throws Exception {
        var admitted = admission("tenant-1", "pipeline-1", "contract-1", "release-1", Optional.of("retained-fingerprint"));
        when(controlPlane.getExecutionAdmissionByClientKey("tenant-1", "client-key", "pipeline-1", "release-1"))
            .thenReturn(Uni.createFrom().item(Optional.empty()), Uni.createFrom().item(Optional.of(admitted)));

        var pending = actions.invoke(request());
        assertThat(pending.resultJson()).contains("null");
        assertThat(pending.checkpoint()).isEmpty();
        assertThat(pending.executionStatus()).isEmpty();
        assertThat(JSON.readValue(actions.invoke(request()).resultJson().orElseThrow(), ExecutionClientAdmission.class))
            .isEqualTo(admitted);

        verifyOnlyAdmissionReads(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retainedAdmissionPreservesTheOriginalIdentityTimestampAndOptionalFingerprint(boolean fingerprintAvailable) throws Exception {
        var admitted = admission("tenant-1", "pipeline-1", "contract-1", "release-1",
            fingerprintAvailable ? Optional.of("original-fingerprint") : Optional.empty());
        when(controlPlane.getExecutionAdmissionByClientKey("tenant-1", "client-key", "pipeline-1", "release-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(admitted)));

        var response = actions.invoke(request());
        assertThat(JSON.readValue(response.resultJson().orElseThrow(), ExecutionClientAdmission.class)).isEqualTo(admitted);
        assertThat(response.bindingCreated()).isFalse();
        assertThat(response.checkpoint()).isEmpty();
        verifyOnlyAdmissionReads(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"tenantId", "pipelineId", "contractVersion", "releaseVersion"})
    void deniesForeignSemanticIdentityBeforeReturningAnyEvidence(String foreignField) {
        var admitted = admission(foreignField.equals("tenantId") ? "foreign" : "tenant-1",
            foreignField.equals("pipelineId") ? "foreign" : "pipeline-1",
            foreignField.equals("contractVersion") ? "foreign" : "contract-1",
            foreignField.equals("releaseVersion") ? "foreign" : "release-1", Optional.of("private-fingerprint"));
        when(controlPlane.getExecutionAdmissionByClientKey("tenant-1", "client-key", "pipeline-1", "release-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(admitted)));

        assertThatThrownBy(() -> actions.invoke(request()))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("requested tenant and Release pin")
            .hasMessageNotContaining("foreign").hasMessageNotContaining("private-fingerprint");
        verifyOnlyAdmissionReads(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"tenantId", "idempotencyKey", "pipelineId", "contractVersion", "releaseVersion"})
    void rejectsMissingRequiredWireFieldsBeforeReadingAdmission(String missingField) {
        ObjectNode wire = JSON.valueToTree(request());
        wire.remove(missingField);

        assertThatThrownBy(() -> actions.invoke(JSON.treeToValue(wire, AwsDurableActionRequest.class)))
            .hasStackTraceContaining(missingField);
        verifyNoInteractions(controlPlane, bindings, decoder);
    }

    @ParameterizedTest
    @ValueSource(strings = {"tenantId", "idempotencyKey", "pipelineId", "contractVersion", "releaseVersion"})
    void factoryRejectsBlankRequiredFields(String blankField) {
        assertThatThrownBy(() -> AwsDurableActionRequest.executionAdmission(
            blankField.equals("tenantId") ? " " : "tenant-1",
            blankField.equals("idempotencyKey") ? " " : "client-key",
            blankField.equals("pipelineId") ? " " : "pipeline-1",
            blankField.equals("contractVersion") ? " " : "contract-1",
            blankField.equals("releaseVersion") ? " " : "release-1"))
            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(blankField);
        verifyNoInteractions(controlPlane, bindings, decoder);
    }

    @Test
    void existingLambdaInvokerRoundTripsTheActualRequestAndCanonicalObservation() throws Exception {
        var admitted = admission("tenant-1", "pipeline-1", "contract-1", "release-1", Optional.of("retained-fingerprint"));
        when(controlPlane.getExecutionAdmissionByClientKey("tenant-1", "client-key", "pipeline-1", "release-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(admitted)));
        var lambda = mock(LambdaClient.class);
        when(lambda.invoke(any(InvokeRequest.class))).thenAnswer(call -> {
            InvokeRequest invocation = call.getArgument(0);
            assertThat(invocation.functionName()).isEqualTo("action-function:7");
            assertThat(invocation.invocationType()).isEqualTo(InvocationType.REQUEST_RESPONSE);
            var wire = JSON.readTree(invocation.payload().asUtf8String());
            assertThat(wire.path("operation").asText()).isEqualTo("READ_EXECUTION_ADMISSION");
            assertThat(wire.path("idempotencyKey").asText()).isEqualTo("client-key");
            var decoded = JSON.treeToValue(wire, AwsDurableActionRequest.class);
            assertThat(decoded).isEqualTo(request());
            return InvokeResponse.builder().statusCode(200)
                .payload(SdkBytes.fromUtf8String(JSON.writeValueAsString(actions.invoke(decoded)))).build();
        });

        var response = new LambdaAwsDurableActionInvoker(lambda, "action-function:7").invoke(request());

        assertThat(JSON.readValue(response.resultJson().orElseThrow(), ExecutionClientAdmission.class)).isEqualTo(admitted);
        verify(lambda).invoke(any(InvokeRequest.class));
        verifyNoMoreInteractions(lambda);
        verifyOnlyAdmissionReads(1);
    }

    private void verifyOnlyAdmissionReads(int count) {
        verify(controlPlane, times(count)).getExecutionAdmissionByClientKey("tenant-1", "client-key", "pipeline-1", "release-1");
        verifyNoMoreInteractions(controlPlane);
        verifyNoInteractions(bindings, decoder);
    }

    private static AwsDurableActionRequest request() {
        return AwsDurableActionRequest.executionAdmission("tenant-1", "client-key", "pipeline-1", "contract-1", "release-1");
    }

    private static ExecutionClientAdmission admission(String tenant, String pipeline, String contract, String release,
        Optional<String> fingerprint) {
        return new ExecutionClientAdmission(new ExecutionSemanticCheckpoint(tenant, "original-execution", pipeline,
            contract, release, ExecutionStatus.SUCCEEDED), 1730000000123L, fingerprint);
    }
}
