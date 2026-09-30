package org.pipelineframework.awsproof;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.amazonaws.services.lambda.runtime.Context;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitCompletionDescriptor;
import org.pipelineframework.awaitable.AwaitCompletionDescriptorRegistry;
import org.pipelineframework.awsproof.model.ProofActionRequest;
import org.pipelineframework.awsproof.model.ProofActionResponse;
import org.pipelineframework.awsproof.model.ProofDriverCheckpoint;
import org.pipelineframework.awsproof.model.ProofExecutionCheckpoint;
import org.pipelineframework.awsproof.model.ProofExecutionInput;

class ProofActionGatewayHandlerTest {

    @Test
    void routesTpfAndAwsDurableOperationsAcrossTheHostSeam() {
        ProofControlPlaneActionAdapter controlPlane = mock(ProofControlPlaneActionAdapter.class);
        ProofDurableHostActionAdapter durableHost = mock(ProofDurableHostActionAdapter.class);
        AwaitCompletionDescriptorRegistry descriptors = mock(AwaitCompletionDescriptorRegistry.class);
        ProofAwaitDescriptorFactory descriptorFactory = mock(ProofAwaitDescriptorFactory.class);
        when(descriptorFactory.create()).thenReturn(mock(AwaitCompletionDescriptor.class));
        ProofActionGatewayHandler handler = new ProofActionGatewayHandler();
        handler.controlPlaneActions = controlPlane;
        handler.durableHostActions = durableHost;
        handler.descriptorRegistry = descriptors;
        handler.descriptorFactory = descriptorFactory;

        ProofActionRequest submit = ProofActionRequest.submit(new ProofExecutionInput(
            "tenant", "key", "pipeline", "contract", "release", "{}", Optional.empty(), 1));
        ProofActionResponse submitted = ProofActionResponse.status("QUEUED");
        when(controlPlane.handle(submit)).thenReturn(submitted);

        assertThat(handler.handleRequest(submit, mock(Context.class)))
            .isSameAs(submitted);
        verify(controlPlane).handle(submit);
        verifyNoInteractions(durableHost);

        ProofActionRequest register = ProofActionRequest.register(
            new ProofDriverCheckpoint(
                new ProofExecutionCheckpoint(
                    "tenant", "execution", "pipeline", "contract", "release"),
                1),
            "provider-name",
            "arn:aws:lambda:region:account:function:name:1/durable-execution/id",
            "callback");
        ProofActionResponse registered = ProofActionResponse.bound(true);
        when(durableHost.handle(register)).thenReturn(registered);

        assertThat(handler.handleRequest(register, mock(Context.class)))
            .isSameAs(registered);
        verify(durableHost).handle(register);
    }
}
