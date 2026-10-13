package org.pipelineframework.awaitable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Runtime descriptor for one generated await step.
 *
 * @param stepId stable generated step id
 * @param inputType input domain type
 * @param outputType output domain type
 * @param cardinality pipeline cardinality shape
 * @param timeout maximum wait duration
 * @param correlationStrategy strategy used to derive adapter-visible correlation ids
 * @param transportType adapter type
 * @param transportConfig transport-specific adapter config
 * @param idempotencyKeyFields fields used to derive stable idempotency keys
 * @param transportInputType serialization representation used at dispatch
 * @param transportOutputType serialization representation used at completion
 * @param inputToTransport generated conversion applied immediately before dispatch
 * @param outputFromTransport generated conversion applied immediately after transport decoding
 * @param completionProjectorId stable configured identity of the request-aware completion projector
 * @param completionProjector optional pure request-plus-completion projection
 * @param requestAwareCompletion whether completionProjector owns canonical output construction
 */
public record AwaitCompletionDescriptor(
    String stepId,
    String inputType,
    String outputType,
    String cardinality,
    Duration timeout,
    String correlationStrategy,
    String transportType,
    Map<String, Object> transportConfig,
    List<String> idempotencyKeyFields,
    String transportInputType,
    String transportOutputType,
    Function<Object, Object> inputToTransport,
    Function<Object, Object> outputFromTransport,
    String completionProjectorId,
    AwaitCompletionProjector<Object, Object, Object> completionProjector,
    boolean requestAwareCompletion,
    java.util.Optional<ConnectorCallbackSelection> callback,
    Optional<String> commandAcknowledgementType,
    Optional<CommandCompletionProjector<Object, Object, Object, Object>> commandCompletionProjector
) {
    public static final String CONNECTOR_CALLBACK_TRANSPORT = "connector-callback";

    public AwaitCompletionDescriptor(String stepId, String inputType, String outputType, String cardinality,
        Duration timeout, String correlationStrategy, String transportType, Map<String, Object> transportConfig,
        List<String> idempotencyKeyFields, String transportInputType, String transportOutputType,
        Function<Object, Object> inputToTransport, Function<Object, Object> outputFromTransport,
        String completionProjectorId, AwaitCompletionProjector<Object, Object, Object> completionProjector,
        boolean requestAwareCompletion, java.util.Optional<ConnectorCallbackSelection> callback) {
        this(stepId, inputType, outputType, cardinality, timeout, correlationStrategy, transportType,
            transportConfig, idempotencyKeyFields, transportInputType, transportOutputType,
            inputToTransport, outputFromTransport, completionProjectorId, completionProjector,
            requestAwareCompletion, callback, Optional.empty(), Optional.empty());
    }

    public AwaitCompletionDescriptor(String stepId, String inputType, String outputType, String cardinality,
        Duration timeout, String correlationStrategy, String transportType, Map<String, Object> transportConfig,
        List<String> idempotencyKeyFields, String transportInputType, String transportOutputType,
        Function<Object, Object> inputToTransport, Function<Object, Object> outputFromTransport,
        String completionProjectorId, AwaitCompletionProjector<Object, Object, Object> completionProjector,
        boolean requestAwareCompletion, Optional<ConnectorCallbackSelection> callback,
        String commandAcknowledgementType,
        CommandCompletionProjector<Object, Object, Object, Object> commandCompletionProjector) {
        this(stepId, inputType, outputType, cardinality, timeout, correlationStrategy, transportType,
            transportConfig, idempotencyKeyFields, transportInputType, transportOutputType,
            inputToTransport, outputFromTransport, completionProjectorId, completionProjector,
            requestAwareCompletion, callback, Optional.of(commandAcknowledgementType),
            Optional.of(commandCompletionProjector));
    }

    public boolean acknowledgementAwareCommandCompletion() {
        return commandCompletionProjector.isPresent();
    }

    public AwaitCompletionDescriptor(String stepId, String inputType, String outputType, String cardinality,
        Duration timeout, String correlationStrategy, String transportType, Map<String, Object> transportConfig,
        List<String> idempotencyKeyFields, String transportInputType, String transportOutputType,
        Function<Object, Object> inputToTransport, Function<Object, Object> outputFromTransport,
        String completionProjectorId, AwaitCompletionProjector<Object, Object, Object> completionProjector,
        boolean requestAwareCompletion) {
        this(stepId, inputType, outputType, cardinality, timeout, correlationStrategy, transportType, transportConfig,
            idempotencyKeyFields, transportInputType, transportOutputType, inputToTransport, outputFromTransport,
            completionProjectorId, completionProjector, requestAwareCompletion, java.util.Optional.empty());
    }
    public AwaitCompletionDescriptor(
        String stepId,
        String inputType,
        String outputType,
        String cardinality,
        Duration timeout,
        String correlationStrategy,
        String transportType,
        Map<String, Object> transportConfig,
        List<String> idempotencyKeyFields,
        String transportInputType,
        String transportOutputType,
        Function<Object, Object> inputToTransport,
        Function<Object, Object> outputFromTransport
    ) {
        this(stepId, inputType, outputType, cardinality, timeout, correlationStrategy, transportType,
            transportConfig, idempotencyKeyFields, transportInputType, transportOutputType,
            inputToTransport, outputFromTransport, null, defaultCompletionProjector(outputFromTransport), false);
    }

    public AwaitCompletionDescriptor(
        String stepId,
        String inputType,
        String outputType,
        String cardinality,
        Duration timeout,
        String correlationStrategy,
        String transportType,
        Map<String, Object> transportConfig,
        List<String> idempotencyKeyFields
    ) {
        this(stepId, inputType, outputType, cardinality, timeout, correlationStrategy, transportType,
            transportConfig, idempotencyKeyFields, inputType, outputType, Function.identity(), Function.identity());
    }

    public AwaitCompletionDescriptor(
        String stepId,
        String inputType,
        String outputType,
        Duration timeout,
        String correlationStrategy,
        String transportType,
        Map<String, Object> transportConfig,
        List<String> idempotencyKeyFields
    ) {
        this(
            stepId,
            inputType,
            outputType,
            "ONE_TO_ONE",
            timeout,
            correlationStrategy,
            transportType,
            transportConfig,
            idempotencyKeyFields,
            inputType,
            outputType,
            Function.identity(),
            Function.identity());
    }

    public AwaitCompletionDescriptor(
        String stepId,
        String inputType,
        String outputType,
        String cardinality,
        Duration timeout,
        String correlationStrategy,
        String transportType,
        Map<String, Object> transportConfig,
        List<String> idempotencyKeyFields,
        String transportInputType,
        String transportOutputType
    ) {
        this(
            stepId,
            inputType,
            outputType,
            cardinality,
            timeout,
            correlationStrategy,
            transportType,
            transportConfig,
            idempotencyKeyFields,
            transportInputType,
            transportOutputType,
            Function.identity(),
            Function.identity());
    }

    public AwaitCompletionDescriptor {
        callback = java.util.Objects.requireNonNull(callback, "callback");
        commandAcknowledgementType = java.util.Objects.requireNonNull(commandAcknowledgementType,
            "commandAcknowledgementType");
        commandCompletionProjector = java.util.Objects.requireNonNull(commandCompletionProjector,
            "commandCompletionProjector");
        if (stepId == null || stepId.isBlank()) {
            throw new IllegalArgumentException("stepId must not be blank");
        }
        if (inputType == null || inputType.isBlank()) {
            throw new IllegalArgumentException("inputType must not be blank");
        }
        if (outputType == null || outputType.isBlank()) {
            throw new IllegalArgumentException("outputType must not be blank");
        }
        cardinality = cardinality == null || cardinality.isBlank() ? "ONE_TO_ONE" : cardinality.trim();
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        if (callback.isEmpty() && (transportType == null || transportType.isBlank())) {
            throw new IllegalArgumentException("transportType must not be blank");
        }
        correlationStrategy = correlationStrategy == null || correlationStrategy.isBlank()
            ? "interactionId"
            : correlationStrategy;
        if (transportConfig == null) {
            transportConfig = Map.of();
        } else {
            for (Map.Entry<String, Object> entry : transportConfig.entrySet()) {
                if (entry.getKey() == null) {
                    throw new IllegalArgumentException("transportConfig contains null key '<null>'");
                }
                if (entry.getValue() == null) {
                    throw new IllegalArgumentException(
                        "transportConfig value for key '" + entry.getKey() + "' must not be null");
                }
            }
            transportConfig = Map.copyOf(transportConfig);
        }
        idempotencyKeyFields = idempotencyKeyFields == null ? List.of() : List.copyOf(idempotencyKeyFields);
        if (callback.isPresent() && (!(transportType == null || transportType.isBlank()
            || CONNECTOR_CALLBACK_TRANSPORT.equals(transportType)) || !transportConfig.isEmpty()
            || !idempotencyKeyFields.isEmpty() || !"signedResumeToken".equals(correlationStrategy)
            || !"ONE_TO_ONE".equals(cardinality) || !requestAwareCompletion)) {
            throw new IllegalArgumentException("callback completion requires unary signed-token projection without Await transport or idempotency fields");
        }
        if (callback.isPresent()) {
            transportType = CONNECTOR_CALLBACK_TRANSPORT;
        }
        transportInputType = transportInputType == null || transportInputType.isBlank() ? inputType : transportInputType;
        transportOutputType = transportOutputType == null || transportOutputType.isBlank() ? outputType : transportOutputType;
        inputToTransport = inputToTransport == null ? Function.identity() : inputToTransport;
        Function<Object, Object> normalizedOutputFromTransport =
            outputFromTransport == null ? Function.identity() : outputFromTransport;
        outputFromTransport = normalizedOutputFromTransport;
        completionProjectorId = completionProjectorId == null || completionProjectorId.isBlank()
            ? null
            : completionProjectorId.trim();
        if (requestAwareCompletion && completionProjector == null && commandCompletionProjector.isEmpty()) {
            throw new IllegalArgumentException("request-aware completion requires a completion projector");
        }
        if (requestAwareCompletion && completionProjectorId == null) {
            throw new IllegalArgumentException("request-aware completion requires a stable completion projector id");
        }
        if (!requestAwareCompletion) {
            completionProjectorId = null;
        }
        if (commandCompletionProjector.isPresent() && (callback.isEmpty()
            || completionProjector != null || commandAcknowledgementType.isEmpty()
            || commandAcknowledgementType.orElseThrow().isBlank())) {
            throw new IllegalArgumentException("acknowledgement-aware completion requires a callback, Command output type and distinct projector");
        }
        if (commandCompletionProjector.isEmpty() && commandAcknowledgementType.isPresent()) {
            throw new IllegalArgumentException("Command acknowledgement type requires its projector");
        }
        completionProjector = completionProjector == null
            ? defaultCompletionProjector(normalizedOutputFromTransport)
            : completionProjector;
    }

    static AwaitCompletionProjector<Object, Object, Object> defaultCompletionProjector(
        Function<Object, Object> outputFromTransport
    ) {
        Function<Object, Object> converter = outputFromTransport == null ? Function.identity() : outputFromTransport;
        return (request, completion, metadata) -> converter.apply(completion);
    }
}
