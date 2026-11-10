package org.pipelineframework.orchestrator;

import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/** Provider-neutral observation of an admitted client key and its retained initial input. */
public record ExecutionClientAdmission(
    ExecutionSemanticCheckpoint checkpoint,
    long submittedAtEpochMs,
    Optional<String> inputFingerprint
) {
    public RunAsyncAcceptedDto accepted(boolean duplicate) {
        String executionId = checkpoint.executionId();
        return new RunAsyncAcceptedDto(executionId, duplicate, "/pipeline/executions/" + executionId, submittedAtEpochMs);
    }

    public static String fingerprint(SerializedTransitionPayload input) {
        try {
            var mapper = PipelineJson.mapper();
            Object value = mapper.readerFor(Object.class)
                .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .readValue(input.payload());
            byte[] canonical = mapper.writer().with(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .writeValueAsBytes(List.of(input.payloadTypeId(), input.payloadEncoding(), value));
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical));
        } catch (Exception invalid) {
            throw new IllegalArgumentException("Initial typed input could not be fingerprinted", invalid);
        }
    }
}
