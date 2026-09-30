package org.pipelineframework.awsproof;

import java.time.Duration;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.pipelineframework.orchestrator.PipelineControlPlane;

@Named("proof-await-reconciler")
@ApplicationScoped
public final class ProofAwaitReconcilerHandler implements RequestHandler<Map<String, Object>, Integer> {
    private static final Duration ACTION_TIMEOUT = Duration.ofSeconds(30);

    @Inject
    ProofCallbackBindingRepository bindings;

    @Inject
    ProofWakeupService wakeups;

    @Inject
    ProofDurableBindingResolver resolver;

    @Inject
    ProofDriverRecoveryService recovery;

    @Inject
    PipelineControlPlane controlPlane;

    @Override
    public Integer handleRequest(Map<String, Object> event, Context context) {
        controlPlane.sweepOnce(System.currentTimeMillis()).await().atMost(ACTION_TIMEOUT);
        int attempted = 1;
        String closedExecutionArn = durableExecutionArn(event);
        if (!closedExecutionArn.isBlank() && recovery.recoverClosed(closedExecutionArn)) {
            attempted++;
        }
        attempted += recovery.recoverClosedBindings();
        for (var binding : bindings.scanOpen(100)) {
            wakeups.wake(binding.awaitIdentity());
            attempted++;
        }
        for (var recovered : resolver.reconstructOpenBindings()) {
            bindings.bind(recovered);
            wakeups.wake(recovered.awaitIdentity());
            attempted++;
        }
        return attempted;
    }

    private static String durableExecutionArn(Map<String, Object> event) {
        Object detail = event == null ? null : event.get("detail");
        if (!(detail instanceof Map<?, ?> values)) {
            return "";
        }
        Object status = values.get("status");
        if (!("FAILED".equals(status) || "STOPPED".equals(status) || "TIMED_OUT".equals(status))) {
            return "";
        }
        Object arn = values.get("durableExecutionArn");
        return arn instanceof String text ? text : "";
    }
}
