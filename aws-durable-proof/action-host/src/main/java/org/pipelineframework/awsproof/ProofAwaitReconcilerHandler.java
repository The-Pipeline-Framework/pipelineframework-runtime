package org.pipelineframework.awsproof;

import java.time.Duration;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.jboss.logging.Logger;
import org.pipelineframework.orchestrator.PipelineControlPlane;

@Named("proof-await-reconciler")
@ApplicationScoped
public final class ProofAwaitReconcilerHandler implements RequestHandler<Map<String, Object>, Integer> {
    private static final Logger LOG = Logger.getLogger(ProofAwaitReconcilerHandler.class);
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
            attempted++;
            try {
                wakeups.wake(binding.awaitIdentity());
            } catch (RuntimeException failure) {
                LOG.warnf(failure,
                    "Reconciler wake-up failed for execution=%s generation=%d",
                    binding.awaitIdentity().executionId(), binding.awaitIdentity().generation());
            }
        }
        for (var recovered : resolver.reconstructOpenBindings()) {
            attempted++;
            try {
                bindings.bind(recovered);
                wakeups.wake(recovered.awaitIdentity());
            } catch (RuntimeException failure) {
                LOG.warnf(failure,
                    "Reconciler reconstruction failed for execution=%s generation=%d",
                    recovered.awaitIdentity().executionId(), recovered.awaitIdentity().generation());
            }
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
