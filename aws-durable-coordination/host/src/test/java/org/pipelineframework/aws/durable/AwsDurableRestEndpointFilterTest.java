package org.pipelineframework.aws.durable;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.RestAwaitLifecycleProtocol;
import org.pipelineframework.orchestrator.RestTransitionWorkerProtocol;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AwsDurableRestEndpointFilterTest {
    @Test
    void lifecycleAssemblyAcceptsOnlyTypedWorkerCallbacks() {
        var filter = new AwsDurableRestEndpointFilter();
        filter.lifecycle = true;

        var registration = request("POST", RestAwaitLifecycleProtocol.REGISTRATION_PATH);
        filter.filter(registration);
        verify(registration, never()).abortWith(any());

        var admin = request("POST", "/tpf/admin/tenants/tenant/pipelines/pipeline/workers/register");
        filter.filter(admin);
        verify(admin).abortWith(argThat(response -> response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()));

        var transition = request("POST", RestTransitionWorkerProtocol.EXECUTE_PATH);
        filter.filter(transition);
        verify(transition).abortWith(any());
    }

    @Test
    void workerAssemblyAcceptsProjectionAndRejectsCoordinatorCallbacks() {
        var filter = new AwsDurableRestEndpointFilter();
        filter.worker = true;

        var projection = request("POST", RestTransitionWorkerProtocol.AWAIT_VALUE_OPERATION_PATH);
        filter.filter(projection);
        verify(projection, never()).abortWith(any());

        var callback = request("POST", RestAwaitLifecycleProtocol.REPORT_PATH);
        filter.filter(callback);
        verify(callback).abortWith(any());
    }

    private static ContainerRequestContext request(String method, String path) {
        var request = mock(ContainerRequestContext.class);
        var uri = mock(UriInfo.class);
        when(request.getMethod()).thenReturn(method);
        when(request.getUriInfo()).thenReturn(uri);
        when(uri.getRequestUri()).thenReturn(java.net.URI.create("https://localhost" + path));
        return request;
    }
}
