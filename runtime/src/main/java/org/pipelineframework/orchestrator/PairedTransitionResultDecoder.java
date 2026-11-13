package org.pipelineframework.orchestrator;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.TransitionResultEnvelope.PortableFailure;

/** Decodes the already selected whole-bundle result profile under the original dispatch. */
final class PairedTransitionResultDecoder {
    private static final ObjectReader JSON = PipelineJson.mapper().reader()
        .with(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
        .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private final NativeWorkerReleaseBinding.Attempt attempt;
    private final List<PipelineBundleStepDescriptor> steps;
    private final PolyglotInt32PayloadBinding payloads;

    PairedTransitionResultDecoder(NativeWorkerReleaseBinding.Attempt attempt) {
        this.attempt = attempt;
        this.steps = attempt.target().coordinatorContract().steps();
        this.payloads = new PolyglotInt32PayloadBinding(attempt.expectedInputTypeId(),
            steps.get(0).outputTypeId(), steps.get(1).outputTypeId(), attempt.expectedOutputTypeId());
    }

    void validateCommand() {
        payloads.decode(attempt.expectedInputTypeId(), attempt.command().serializedPayload());
    }

    TransitionResultEnvelope decode(String body) {
        try {
            JsonNode result = JSON.readTree(body);
            if (result == null || !result.isObject()) {
                throw new IllegalArgumentException("Expected a paired result object");
            }
            return switch (text(result, "outcome")) {
                case "COMPLETED" -> completed(result);
                case "FAILED" -> failed(result);
                default -> throw new IllegalArgumentException("Unsupported paired outcome");
            };
        } catch (IOException | IllegalArgumentException invalid) {
            // Neither JSON parsing errors nor untrusted values belong in coordinator diagnostics.
            throw new TransitionWorkerFailureException("Invalid response for the selected native worker result profile");
        }
    }

    private TransitionResultEnvelope completed(JsonNode result) {
        JsonNode outputs = result.path("outputPayloads");
        // Released Java serialization includes these absent optional values as null;
        // native producers may omit them. Neither form admits Await, paging or failure.
        Set<String> fields = Set.of("outcome", "outputPayloads", "terminalOutputPublished",
            "terminalInputPassthrough", "awaitSuspension", "failure", "pageCompletion");
        result.fieldNames().forEachRemaining(field -> {
            if (!fields.contains(field)) {
                throw new IllegalArgumentException("Unknown paired completion field");
            }
        });
        if (!outputs.isArray() || outputs.size() != 1
            || !absent(result, "awaitSuspension") || !absent(result, "failure") || !absent(result, "pageCompletion")
            || !isFalse(result.path("terminalOutputPublished")) || !isFalse(result.path("terminalInputPassthrough"))) {
            throw new IllegalArgumentException("Invalid paired completion");
        }
        JsonNode output = outputs.get(0);
        if (!output.isObject() || output.size() != 3) {
            throw new IllegalArgumentException("Invalid paired output");
        }
        var payload = new SerializedTransitionPayload(text(output, "payloadTypeId"),
            text(output, "payloadEncoding"), text(output, "payload"));
        int value = payloads.decode(attempt.expectedOutputTypeId(), payload);
        return TransitionResultEnvelope.pairedCompleted(attempt, payloads.encode(attempt.expectedOutputTypeId(), value));
    }

    private TransitionResultEnvelope failed(JsonNode result) {
        JsonNode failure = result.path("failure");
        if (result.size() != 3 || !attempt.target().profiles().resultProfile().equals(text(result, "resultProfile"))
            || !failure.isObject() || failure.size() != 3) {
            throw new IllegalArgumentException("Invalid portable failure");
        }
        PortableFailure.Code code = PortableFailure.Code.valueOf(text(failure, "code"));
        if (!code.diagnostic().equals(text(failure, "diagnostic"))) {
            throw new IllegalArgumentException("Invalid portable diagnostic");
        }
        String stepId = text(failure, "stepId");
        int stepIndex = steps.stream().filter(step -> step.authoredName().equals(stepId))
            .mapToInt(PipelineBundleStepDescriptor::index).findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown portable failure step"));
        return TransitionResultEnvelope.pairedFailed(attempt, new PortableFailure(code, stepIndex));
    }

    private static String text(JsonNode object, String field) {
        JsonNode value = object.path(field);
        if (!value.isTextual()) {
            throw new IllegalArgumentException("Missing paired response text field");
        }
        return value.textValue();
    }

    private static boolean isFalse(JsonNode value) {
        return value.isBoolean() && !value.booleanValue();
    }

    private static boolean absent(JsonNode object, String field) {
        return !object.has(field) || object.get(field).isNull();
    }
}
