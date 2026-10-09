package org.pipelineframework.orchestrator;

import java.util.Map;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.release.PipelineReleaseEvidence;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** Inline, non-expiring authority: actual input bytes and complete verified Release evidence. */
final class NativeAdmissionDynamoCodec {
    static final String KEY = "tenant_execution_key";
    private static final String METADATA = "admission_metadata";
    private static final String INPUT = "admission_input";
    private static final long MAX_ITEM_BYTES = 400L * 1024;

    private NativeAdmissionDynamoCodec() { }

    static Map<String, AttributeValue> encode(NativeExecutionAdmission admission) {
        var intent = admission.intent();
        var metadata = new Metadata(new IntentMetadata(intent.schemaVersion(), intent.tenantId(), intent.pipelineId(),
            intent.clientKey(), intent.contractVersion(), intent.releaseVersion(), intent.inputShape(),
            intent.payloadTypeId(), intent.payloadEncoding(), intent.outputStreaming()), admission.evidence(), admission.receipt());
        try {
            String json = PipelineJson.mapper().writeValueAsString(metadata);
            String key = NativeExecutionAdmission.key(intent.tenantId(), intent.pipelineId(), intent.clientKey());
            byte[] input = intent.inputBytes();
            // Attribute names and string values count in UTF-8; binary counts decoded bytes, not base64.
            // A conservative safety allowance also covers provider item bookkeeping.
            long size = 128L + utf8Length(KEY) + utf8Length(key) + utf8Length(METADATA) + utf8Length(json)
                + utf8Length(INPUT) + input.length;
            if (size > MAX_ITEM_BYTES) {
                throw new ExecutionAdmissionTooLargeException();
            }
            return Map.of(KEY, AttributeValue.builder().s(key).build(), METADATA, AttributeValue.builder().s(json).build(),
                INPUT, AttributeValue.builder().b(SdkBytes.fromByteArray(input)).build());
        } catch (ExecutionAdmissionTooLargeException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("Cannot encode immutable native admission", error);
        }
    }

    static NativeExecutionAdmission decode(Map<String, AttributeValue> item, String tenant, String pipeline, String key) {
        try {
            if (item.size() != 3 || item.get(INPUT) == null || item.get(INPUT).b() == null
                || item.get(METADATA) == null || item.get(METADATA).s() == null
                || !NativeExecutionAdmission.key(tenant, pipeline, key).equals(item.get(KEY).s())) {
                throw new IllegalStateException("Corrupt native admission row shape");
            }
            Metadata metadata = PipelineJson.mapper().readValue(item.get(METADATA).s(), Metadata.class);
            var original = metadata.intent();
            var intent = new ExecutionAdmissionIntent(original.schemaVersion(), original.tenantId(), original.pipelineId(),
                original.clientKey(), original.contractVersion(), original.releaseVersion(), original.inputShape(),
                original.payloadTypeId(), original.payloadEncoding(), item.get(INPUT).b().asByteArray(), original.outputStreaming());
            if (!tenant.equals(intent.tenantId()) || !pipeline.equals(intent.pipelineId()) || !key.equals(intent.clientKey())) {
                throw new IllegalStateException("Corrupt native admission index scope");
            }
            return new NativeExecutionAdmission(intent, metadata.evidence(), metadata.receipt());
        } catch (Exception error) {
            throw new IllegalStateException("Corrupt retained native admission authority", error);
        }
    }

    private static int utf8Length(String value) {
        return NativeExecutionAdmission.utf8(value).length;
    }

    private record IntentMetadata(int schemaVersion, String tenantId, String pipelineId, String clientKey,
        String contractVersion, String releaseVersion, String inputShape, String payloadTypeId,
        String payloadEncoding, boolean outputStreaming) { }
    private record Metadata(IntentMetadata intent, PipelineReleaseEvidence evidence, ExecutionAdmissionReceipt receipt) { }
}
