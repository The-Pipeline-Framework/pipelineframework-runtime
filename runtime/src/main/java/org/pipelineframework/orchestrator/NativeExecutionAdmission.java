package org.pipelineframework.orchestrator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.CharBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import org.pipelineframework.orchestrator.release.PipelineReleaseEvidence;

/** Retained immutable admission authority; execution TTL never applies to this value. */
record NativeExecutionAdmission(ExecutionAdmissionIntent intent, PipelineReleaseEvidence evidence,
    ExecutionAdmissionReceipt receipt) {
    NativeExecutionAdmission {
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(receipt, "receipt");
        if (!receipt.matches(intent)
            || !receipt.releaseMetadataFingerprint().equals(evidence.metadataFingerprint())
            || !receipt.primaryArtifactId().equals(evidence.primaryArtifactId())
            || !receipt.primaryArtifactDigest().equals(evidence.primaryArtifactDigest())
            || !intent.pipelineId().equals(evidence.descriptor().pipelineId())
            || !intent.contractVersion().equals(evidence.descriptor().contractVersion())
            || !intent.releaseVersion().equals(evidence.descriptor().releaseVersion())) {
            throw new IllegalStateException("Corrupt native admission evidence");
        }
    }

    static void validate(ExecutionAdmissionCreateCommand command, PipelineReleaseEvidence evidence) {
        if (!command.releaseMetadataFingerprint().equals(evidence.metadataFingerprint())
            || !command.primaryArtifactId().equals(evidence.primaryArtifactId())
            || !command.primaryArtifactDigest().equals(evidence.primaryArtifactDigest())) {
            throw new IllegalStateException("Admission creation does not match verified Release evidence");
        }
        // Constructor validation also binds the existing P/C/R to the retained verified snapshot.
        create(command, evidence, "validation");
    }

    static NativeExecutionAdmission create(ExecutionAdmissionCreateCommand command,
        PipelineReleaseEvidence evidence, String executionId) {
        var intent = command.intent();
        return new NativeExecutionAdmission(intent, evidence, new ExecutionAdmissionReceipt(1,
            intent.tenantId(), intent.pipelineId(), intent.clientKey(), intent.contractVersion(),
            intent.releaseVersion(), executionId, evidence.metadataFingerprint(), evidence.primaryArtifactId(),
            evidence.primaryArtifactDigest(), intent.fingerprint(), command.execution().nowEpochMs()));
    }

    ExecutionAdmissionResult replay(ExecutionAdmissionIntent requested) {
        if (!intent.equals(requested)) {
            throw new IllegalStateException("Client key already admits a different immutable intent");
        }
        return new ExecutionAdmissionResult(Optional.empty(), receipt);
    }

    void requireEvidence(PipelineReleaseEvidence requested) {
        if (!evidence.equals(requested)) {
            throw new IllegalStateException("Client key already admits different verified Release evidence");
        }
    }

    static String key(String tenant, String pipeline, String clientKey) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var output = new DataOutputStream(bytes)) {
                for (String value : new String[] {tenant, pipeline, clientKey}) {
                    if (value == null || value.isBlank()) {
                        throw new IllegalArgumentException("Admission scope must not be blank");
                    }
                    byte[] encoded = utf8(value);
                    output.writeInt(encoded.length);
                    output.write(encoded);
                }
            }
            // This namespace cannot collide with the legacy length-prefixed numeric key namespace.
            // Exact original scope is retained and checked; this digest is only an internal index.
            return "execution-admission:v1:" + HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch (IllegalArgumentException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("Cannot encode native admission key", error);
        }
    }

    static byte[] utf8(String value) {
        try {
            var bytes = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
            byte[] result = new byte[bytes.remaining()];
            bytes.get(result);
            return result;
        } catch (java.nio.charset.CharacterCodingException error) {
            throw new IllegalArgumentException("Malformed UTF-8 admission representation", error);
        }
    }
}
