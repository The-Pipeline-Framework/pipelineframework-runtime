package org.pipelineframework.aws.durable;

import java.util.Optional;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.awaitable.RestAwaitLifecycleProtocol;
import org.pipelineframework.orchestrator.RestTransitionWorkerProtocol;

/** Limits each dedicated Lambda HTTP assembly to its authenticated transport routes. */
@Provider
@PreMatching
@ApplicationScoped
public class AwsDurableRestEndpointFilter implements ContainerRequestFilter {
    private static final Set<String> WORKER_POSTS = Set.of(
        RestTransitionWorkerProtocol.EXECUTE_PATH, RestTransitionWorkerProtocol.AWAIT_VALUE_OPERATION_PATH);
    private static final Set<String> LIFECYCLE_POSTS = Set.of(
        RestAwaitLifecycleProtocol.REGISTRATION_PATH, RestAwaitLifecycleProtocol.DISPATCH_CLAIM_PATH,
        RestAwaitLifecycleProtocol.REPORT_PATH, RestAwaitLifecycleProtocol.UNIT_PREPARE_PATH,
        RestAwaitLifecycleProtocol.UNIT_SEAL_PATH, RestAwaitLifecycleProtocol.UNIT_SUSPENSION_PATH,
        RestAwaitLifecycleProtocol.UNIT_RESUME_PATH);

    @Inject
    @ConfigProperty(name = "tpf.aws.durable.pinned-rest-worker", defaultValue = "false")
    boolean worker;

    @Inject
    @ConfigProperty(name = "tpf.aws.durable.rest-lifecycle-only", defaultValue = "false")
    boolean lifecycle;

    @Override
    public void filter(ContainerRequestContext request) {
        if (!worker && !lifecycle) {
            return;
        }
        String path = "/" + request.getUriInfo().getPath();
        String method = request.getMethod();
        boolean workerRoute = "POST".equals(method) && WORKER_POSTS.contains(path)
            || "GET".equals(method) && RestTransitionWorkerProtocol.CAPABILITIES_PATH.equals(path);
        boolean lifecycleRoute = "POST".equals(method) && LIFECYCLE_POSTS.contains(path);
        if (!(worker && !lifecycle && workerRoute || lifecycle && !worker && lifecycleRoute)) {
            request.abortWith(Response.status(Response.Status.NOT_FOUND).build());
        }
    }
}
