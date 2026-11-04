package org.pipelineframework.aws.durable;

import static org.junit.jupiter.api.Assertions.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.quarkus.test.QuarkusExtensionTest;
import jakarta.inject.Inject;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitResumeTokenService;
import org.pipelineframework.aws.durable.fixture.*;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Coherent typed runtime fixture and real native authorities; not compiler or restart-durability evidence. */
class AwsDurableWorkItemAwaitTest {
    private static final PipelineContractDescriptor CONTRACT = new PipelineContractDescriptor(1,
        "org.pipelineframework.aws.durable.fixture", "native-scalar-contract-v1", "a".repeat(64), "COMPUTE", "LOCAL",
        "native-await-test", false, "monolith", List.of(
            new PipelineBundleStepDescriptor(0, "WaitForScalarAnswer", "command", "ONE_TO_ONE", Request.class.getName(),
                Prepared.class.getName(), WaitService.class.getName(), WaitService.class.getName(),
                Map.of("mode", "CONNECTOR_CALLBACK", "operationOutputTypeId", Pending.class.getName(), "completionPayloadTypeId", Answer.class.getName(),
                    "finalOutputTypeId", Prepared.class.getName())),
            new PipelineBundleStepDescriptor(1, "FinishScalarResult", "internal", "ONE_TO_ONE", Prepared.class.getName(),
                Result.class.getName(), FinishService.class.getName(), FinishService.class.getName(), Map.of())),
        PipelineBundleCapabilities.defaults());
    @RegisterExtension static final QuarkusExtensionTest APPLICATION = new QuarkusExtensionTest()
        .withApplicationRoot(archive -> archive.addPackages(true, WaitService.class.getPackage())
            .addClasses(AwsDurableWorkItemHandler.class, AwsDurableSqsBatchAdapter.class)
            .addAsResource(new StringAsset(json(CONTRACT)), PipelineContractDescriptor.RESOURCE_PATH)
            .addAsResource(new StringAsset(json(Map.of("order", List.of(WaitService.class.getName(), FinishService.class.getName())))), "META-INF/pipeline/order.json"))
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("quarkus.http.host-enabled", "false")
        .overrideConfigKey("pipeline.orchestrator.mode", "QUEUE_ASYNC")
        .overrideConfigKey("pipeline.orchestrator.coordination-host", "NATIVE")
        .overrideConfigKey("pipeline.orchestrator.process-loops-disabled", "true")
        .overrideConfigKey("pipeline.orchestrator.strict-startup", "true")
        .overrideConfigKey("pipeline.orchestrator.idempotency-policy", "CLIENT_KEY_REQUIRED")
        .overrideConfigKey("pipeline.orchestrator.state-provider", "memory")
        .overrideConfigKey("pipeline.orchestrator.dispatcher-provider", "native-await-test")
        .overrideConfigKey("pipeline.orchestrator.dlq-provider", "log")
        .overrideConfigKey("pipeline.orchestrator.pipeline-id", "org.pipelineframework.aws.durable.fixture")
        .overrideConfigKey("pipeline.orchestrator.release-version", "native-await-test-release")
        .overrideConfigKey("pipeline.orchestrator.resume-token-secret", "test-only-native-await-secret")
        .overrideConfigKey("pipeline.command.effect-store.provider", "memory")
        .overrideConfigKey("pipeline.await-admission.store", "in-memory");
    @Inject AwsDurableWorkItemHandler handler;
    @Inject PipelineExecutionService pipeline;
    @Inject PipelineReleaseIdentityResolver identity;
    @Inject CapturedDelivery delivery;
    @Inject AwaitCoordinator awaits;
    @Inject AwaitResumeTokenService tokens;
    @Inject WaitService provider;
    @Inject InMemoryCommandEffectStore effects;

    @Test void returnsWhileScalarCompletionIsPendingThenResumesThroughNativeDelivery() throws Exception {
        assertEquals(CONTRACT.pipelineId(), identity.contract().pipelineId());
        assertEquals(CONTRACT.contractVersion(), identity.contractVersion());
        String tenant = "native-await-test";
        String key = UUID.randomUUID().toString();
        var admitted = pipeline.executePipelineAsync(new Request(key), tenant, key).await().atMost(Duration.ofSeconds(5));
        var first = event(delivery.receive());
        com.amazonaws.services.lambda.runtime.events.SQSBatchResponse acknowledged;
        try {
            acknowledged = assertTimeoutPreemptively(Duration.ofSeconds(3), () -> handler.handleRequest(first, context()));
        } catch (AssertionError timeout) {
            var current = pipeline.getExecutionStatus(tenant, admitted.executionId()).await().atMost(Duration.ofSeconds(3));
            var pending = awaits.findByExecution(tenant, admitted.executionId(), 10).await().atMost(Duration.ofSeconds(3));
            throw new AssertionError("Handler failed to release: mode=" + provider.observedMode() + ", dispatches="
                + provider.dispatches() + ", execution=" + current.status() + ", interactions="
                + pending.stream().map(record -> record.status().name()).toList() + ", executionContext="
                + provider.executionContextPresent() + ", stepFailure=" + provider.observedFailure(), timeout);
        }
        assertTrue(acknowledged.getBatchItemFailures().isEmpty());
        assertEquals(ExecutionStatus.WAITING_EXTERNAL,
            pipeline.getExecutionStatus(tenant, admitted.executionId()).await().atMost(Duration.ofSeconds(3)).status());
        assertEquals(0, delivery.pending());
        var interactions = awaits.findByExecution(tenant, admitted.executionId(), 10).await().atMost(Duration.ofSeconds(3));
        assertEquals(1, interactions.size());
        var interaction = interactions.getFirst();
        assertEquals(new Request(key), interaction.requestPayload());
        assertTrue(interaction.commandAcknowledgement().isEmpty(), "Request-aware fixture does not retain an acknowledgement projection input");
        assertEquals(1, provider.dispatches());
        var effect = effects.find(tenant, "fixture:" + key).await().atMost(Duration.ofSeconds(3)).orElseThrow();
        assertEquals(CommandEffectStatus.SUCCEEDED, effect.status());
        assertEquals(new Pending(key), effect.output(), "Provider acknowledgement must be retained by native Command authority");
        long now = System.currentTimeMillis();
        pipeline.completeAwaitInteraction(new AwaitCompletionCommand(tenant, interaction.interactionId(),
            interaction.correlationId(), tokens.sign(interaction, now), "answer", new Answer("accepted"), "fixture-user", now))
            .await().atMost(Duration.ofSeconds(3));
        assertNotEquals(ExecutionStatus.SUCCEEDED,
            pipeline.getExecutionStatus(tenant, admitted.executionId()).await().atMost(Duration.ofSeconds(3)).status());
        var resumed = handler.handleRequest(event(delivery.receive()), context());
        assertTrue(resumed.getBatchItemFailures().isEmpty());
        assertEquals(ExecutionStatus.SUCCEEDED,
            pipeline.getExecutionStatus(tenant, admitted.executionId()).await().atMost(Duration.ofSeconds(3)).status());
        Result result = pipeline.<Result>getExecutionResult(tenant, admitted.executionId(), Result.class, false)
            .await().atMost(Duration.ofSeconds(3));
        assertEquals(new Result(key, "accepted"), result);
        assertEquals(0, delivery.pending());
        assertEquals(1, provider.dispatches());
    }

    private SQSEvent event(ExecutionWorkItem item) throws Exception {
        var message = new SQSEvent.SQSMessage();
        message.setMessageId(UUID.randomUUID().toString());
        message.setBody(PipelineJson.mapper().writeValueAsString(item));
        var event = new SQSEvent(); event.setRecords(List.of(message)); return event;
    }

    private Context context() {
        return (Context) Proxy.newProxyInstance(Context.class.getClassLoader(), new Class<?>[]{Context.class},
            (proxy, method, args) -> { throw new AssertionError("Unexpected provider Context access"); });
    }

    private static String json(Object value) {
        try { return PipelineJson.mapper().writeValueAsString(value); }
        catch (Exception failure) { throw new IllegalStateException(failure); }
    }
}
