package org.pipelineframework.aws.durable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.PipelineReleaseEntryBinding;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistry;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.GetFunctionConfigurationRequest;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

/** Selects the immutable entry of the execution's Release, never the current activation. */
@ApplicationScoped
public class AwsDurableReleaseRouter {
    @Inject PipelineReleaseRegistry releases;
    @Inject PipelineControlPlane controlPlane;
    @Inject LambdaClient lambda;
    @Inject @ConfigProperty(name = "tpf.aws.durable.entry-id") Optional<String> entryId;
    @Inject @ConfigProperty(name = "tpf.aws.durable.continuation-entry-id") Optional<String> continuationEntryId;
    @Inject @ConfigProperty(name = "tpf.aws.durable.action-function-family") Optional<String> actionFunctionFamily;

    public boolean enabled() { return entryId != null && entryId.filter(value -> !value.isBlank()).isPresent(); }

    public Uni<SqsMessageDisposition> transition(SqsInboundMessage message, Context context,
        Function<SqsInboundMessage, Uni<SqsMessageDisposition>> local) {
        if (!enabled()) return local.apply(message);
        var remaining = remainingBudget(Duration.ofSeconds(300));
        TransitionCommandEnvelope command;
        try {
            var request = PipelineJson.mapper().readValue(message.body().orElse(""), SqsTransitionWorkerRequest.class);
            command = PipelineJson.mapper().readValue(request.commandEnvelope(), TransitionCommandEnvelope.class);
        } catch (Exception malformed) {
            return local.apply(message);
        }
        return controlPlane.getExecutionSemanticCheckpoint(command.tenantId(), command.executionId()).chain(checkpoint -> {
            if (!command.tenantId().equals(checkpoint.tenantId()) || !command.executionId().equals(checkpoint.executionId())
                || !command.pipelineId().equals(checkpoint.pipelineId()) || !command.contractVersion().equals(checkpoint.contractVersion())
                || !command.releaseVersion().equals(checkpoint.releaseVersion())) {
                return Uni.createFrom().failure(new IllegalArgumentException("Transition does not match the semantic execution pin"));
            }
            // The selected original worker authenticates with its own retained signing generation.
            // Routing grants no transition authority and does not publish a semantic response.
            return route(checkpoint, message, context, remaining, local);
        });
    }

    public Uni<SqsMessageDisposition> work(SqsInboundMessage message, Context context,
        Function<SqsInboundMessage, Uni<SqsMessageDisposition>> local) {
        if (!enabled()) return local.apply(message);
        var remaining = remainingBudget(Duration.ofSeconds(300));
        ExecutionWorkItem item;
        try { item = PipelineJson.mapper().readValue(message.body().orElse(""), ExecutionWorkItem.class); }
        catch (Exception malformed) { return local.apply(message); }
        return controlPlane.getExecutionSemanticCheckpoint(item.tenantId(), item.executionId()).chain(checkpoint -> {
            if (!item.tenantId().equals(checkpoint.tenantId()) || !item.executionId().equals(checkpoint.executionId())) {
                return Uni.createFrom().failure(new IllegalArgumentException("Work item does not match the semantic execution pin"));
            }
            return route(checkpoint, message, context, remaining, local);
        });
    }

    public Uni<SqsMessageDisposition> completion(SqsInboundMessage message, Context context,
        Function<SqsInboundMessage, Uni<SqsMessageDisposition>> local) {
        if (!enabled()) return local.apply(message);
        var remaining = remainingBudget(Duration.ofSeconds(30));
        org.pipelineframework.awaitable.sqs.SqsAwaitCompletionEnvelope completion;
        try {
            completion = PipelineJson.mapper().readValue(message.body().orElse(""),
                org.pipelineframework.awaitable.sqs.SqsAwaitCompletionEnvelope.class);
        } catch (Exception malformed) { return local.apply(message); }
        var checkpoint = completion.interactionId() == null
            ? controlPlane.getAwaitSemanticCheckpointByCorrelation(completion.tenantId(), completion.correlationId())
            : controlPlane.getAwaitSemanticCheckpoint(completion.tenantId(), completion.interactionId());
        return checkpoint.chain(found -> {
            var await = found.orElseThrow(() -> new IllegalStateException("Pinned Await checkpoint is unavailable"));
            boolean matchesLocator = completion.interactionId() == null
                ? completion.correlationId().equals(await.correlationId())
                : completion.interactionId().equals(await.interactionId());
            if (!completion.tenantId().equals(await.tenantId()) || !matchesLocator) {
                throw new IllegalStateException("Await checkpoint does not match completion scope");
            }
            // Token, correlation, idempotency and actor admission remain owned by the original runtime.
            return route(new ExecutionSemanticCheckpoint(await.tenantId(), await.executionId(), await.pipelineId(),
                await.contractVersion(), await.releaseVersion(), ExecutionStatus.WAITING_EXTERNAL), message, context, remaining, local);
        });
    }

    public boolean dueEnabled() {
        return (continuationEntryId != null && continuationEntryId.isPresent())
            || (actionFunctionFamily != null && actionFunctionFamily.isPresent());
    }

    public Uni<AwaitItemContinuationResult> dispatchDue(AwaitItemContinuationCommand command) {
        String entry = Optional.ofNullable(continuationEntryId).orElse(Optional.empty())
            .filter(value -> !value.isBlank()).orElseThrow(() -> new IllegalStateException("Continuation entry ID is required"));
        String family = Optional.ofNullable(actionFunctionFamily).orElse(Optional.empty())
            .filter(value -> value.matches("arn:[a-z0-9-]+:lambda:[a-z0-9-]+:[0-9]{12}:function:[A-Za-z0-9_-]+"))
            .orElseThrow(() -> new IllegalStateException("Exact trusted Action function family ARN is required"));
        var remaining = remainingBudget(Duration.ofSeconds(30));
        return controlPlane.getExecutionSemanticCheckpoint(command.tenantId(), command.executionId()).chain(checkpoint -> {
            if (!checkpoint.tenantId().equals(command.tenantId()) || !checkpoint.executionId().equals(command.executionId())) {
                throw new IllegalStateException("Due continuation does not match execution scope");
            }
            return bindingFor(checkpoint, entry).chain(binding -> {
                requireFunctionFamily(binding, family);
                return Uni.createFrom().item(() -> {
                    try {
                        var request = org.pipelineframework.aws.durable.model.AwsDurableActionRequest.itemContinuation(
                            new org.pipelineframework.aws.durable.model.AwsDurableExecutionCheckpoint(checkpoint.tenantId(),
                                checkpoint.executionId(), checkpoint.pipelineId(), checkpoint.contractVersion(), checkpoint.releaseVersion()),
                            PipelineJson.mapper().writeValueAsString(command));
                        var response = PipelineJson.mapper().readValue(invokeRaw(binding, PipelineJson.mapper().writeValueAsString(request), remaining),
                            org.pipelineframework.aws.durable.model.AwsDurableActionResponse.class);
                        var result = PipelineJson.mapper().readValue(response.resultJson().orElseThrow(() ->
                            new IllegalStateException("Continuation response is missing its canonical result")), AwaitItemContinuationResult.class);
                        var observed = result.command();
                        if (!observed.tenantId().equals(command.tenantId()) || !observed.executionId().equals(command.executionId())
                            || !observed.unitId().equals(command.unitId()) || !observed.interactionId().equals(command.interactionId())
                            || observed.itemIndex() != command.itemIndex() || observed.attempt() != command.attempt()) {
                            throw new IllegalStateException("Continuation result does not match the dispatched attempt");
                        }
                        return result;
                    } catch (java.io.IOException invalid) { throw new IllegalStateException("Invalid continuation response", invalid); }
                }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
            });
        });
    }

    private Uni<SqsMessageDisposition> route(ExecutionSemanticCheckpoint checkpoint, SqsInboundMessage message,
        Context context, LongSupplier remaining, Function<SqsInboundMessage, Uni<SqsMessageDisposition>> local) {
        return bindingFor(checkpoint, entryId.orElseThrow()).chain(binding -> {
            String invoked = java.util.Objects.requireNonNull(context, "context").getInvokedFunctionArn();
            requireNumericSameFunction(binding, invoked);
            if (binding.resourceId().equals(invoked)) return local.apply(message);
            return Uni.createFrom().item(() -> invoke(binding, message,
                () -> Math.min(remaining.getAsLong(), context.getRemainingTimeInMillis())))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
        });
    }

    private Uni<PipelineReleaseEntryBinding> bindingFor(ExecutionSemanticCheckpoint checkpoint, String entry) {
        return releases.getEntryBinding(checkpoint.tenantId(), checkpoint.pipelineId(), checkpoint.contractVersion(),
            checkpoint.releaseVersion(), entry).chain(found -> {
                var binding = found.orElseThrow(() -> new IllegalStateException("Pinned Release entry binding is unavailable"));
                if (!binding.key().equals(List.of(checkpoint.tenantId(), checkpoint.pipelineId(), checkpoint.contractVersion(),
                    checkpoint.releaseVersion(), entry))) throw new IllegalStateException("Entry binding does not match semantic execution pin");
                return releases.get(checkpoint.tenantId(), checkpoint.pipelineId(), checkpoint.releaseVersion()).map(release -> {
                    var registered = release.orElseThrow(() -> new IllegalStateException("Pinned Release is unavailable"));
                    binding.requireRelease(registered);
                    var artifact = registered.descriptor().artifacts().stream()
                        .filter(value -> value.artifactId().equals(binding.artifactId())).findFirst().orElseThrow();
                    if (!"lambda-zip".equals(artifact.kind())) throw new IllegalStateException("AWS entry routing requires an exact lambda-zip artifact");
                    String deliveredDigest;
                    try {
                        byte[] bytes = java.util.Base64.getDecoder().decode(binding.codeDigest());
                        if (bytes.length != 32) throw new IllegalArgumentException("not SHA-256");
                        deliveredDigest = "sha256:" + java.util.HexFormat.of().formatHex(bytes);
                    } catch (IllegalArgumentException invalid) {
                        throw new IllegalStateException("AWS entry code digest must be a SHA-256 CodeSha256 value", invalid);
                    }
                    if (!artifact.digest().equals(deliveredDigest)) throw new IllegalStateException("AWS entry code digest does not match the original Release ZIP");
                    return binding;
                });
            });
    }

    private static final String NUMERIC_FUNCTION_ARN = "arn:[a-z0-9-]+:lambda:[a-z0-9-]+:[0-9]{12}:function:[A-Za-z0-9_-]+:[1-9][0-9]*";

    private static void requireNumericSameFunction(PipelineReleaseEntryBinding binding, String invoked) {
        if (invoked == null || !invoked.matches(NUMERIC_FUNCTION_ARN)) {
            throw new IllegalStateException("Release routing requires retained numbered versions of this exact function");
        }
        requireFunctionFamily(binding, invoked.substring(0, invoked.lastIndexOf(':')));
    }

    private static void requireFunctionFamily(PipelineReleaseEntryBinding binding, String family) {
        if (!"aws-lambda".equals(binding.provider()) || !binding.resourceId().matches(NUMERIC_FUNCTION_ARN)
            || !binding.resourceId().substring(0, binding.resourceId().lastIndexOf(':')).equals(family)) {
            throw new IllegalStateException("Release routing requires retained numbered versions of this exact function");
        }
    }

    private SqsMessageDisposition invoke(PipelineReleaseEntryBinding binding, SqsInboundMessage message,
        LongSupplier remaining) {
        try {
            String payload = PipelineJson.mapper().writeValueAsString(Map.of("Records", List.of(Map.of(
                "messageId", message.messageId().orElseThrow(), "body", message.body().orElseThrow()))));
            var batch = PipelineJson.mapper().readValue(invokeRaw(binding, payload, remaining), SQSBatchResponse.class);
            if (batch.getBatchItemFailures() == null) throw new IllegalStateException("Pinned entry response is incomplete");
            return batch.getBatchItemFailures().isEmpty() ? SqsMessageDisposition.ACKNOWLEDGE : SqsMessageDisposition.RETRY;
        } catch (java.io.IOException failure) { throw new IllegalStateException("Pinned entry response is invalid", failure); }
    }

    private String invokeRaw(PipelineReleaseEntryBinding binding, String payload, LongSupplier remaining) {
        var observed = lambda.getFunctionConfiguration(GetFunctionConfigurationRequest.builder().functionName(binding.resourceId())
            .overrideConfiguration(override -> override.apiCallTimeout(callTimeout(remaining, 5_000))).build());
        if (!binding.codeDigest().equals(observed.codeSha256())) throw new IllegalStateException("Pinned entry code digest is unverified");
        long targetMillis = observed.timeout() == null ? Long.MAX_VALUE : Duration.ofSeconds(observed.timeout()).toMillis() + 1_000;
        var response = lambda.invoke(InvokeRequest.builder().functionName(binding.resourceId())
            .invocationType(InvocationType.REQUEST_RESPONSE).payload(SdkBytes.fromUtf8String(payload))
            .overrideConfiguration(override -> override.apiCallTimeout(callTimeout(remaining, targetMillis))).build());
        String version = binding.resourceId().substring(binding.resourceId().lastIndexOf(':') + 1);
        if (response.statusCode() == null || response.statusCode() != 200 || response.functionError() != null
            || !version.equals(response.executedVersion()) || response.payload() == null) {
            throw new IllegalStateException("Pinned entry invocation did not return a verified response");
        }
        return response.payload().asUtf8String();
    }

    private static LongSupplier remainingBudget(Duration limit) {
        long deadline = System.nanoTime() + limit.toNanos();
        return () -> java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
    }

    private static Duration callTimeout(LongSupplier remaining, long limitMillis) {
        // Leave time for the originating handler to return its retry disposition.
        long millis = Math.min(remaining.getAsLong() - 1_000, limitMillis);
        if (millis <= 0) {
            throw new IllegalStateException("Insufficient time to route to the pinned Release entry");
        }
        return Duration.ofMillis(millis);
    }
}
