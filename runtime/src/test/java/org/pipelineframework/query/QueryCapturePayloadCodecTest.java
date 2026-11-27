package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;

class QueryCapturePayloadCodecTest {
    private final QueryCapturePayloadCodec codec = new QueryCapturePayloadCodec(PipelineJson.mapper());

    @Test
    void roundTripsGeneratedLikeSealedUnionByCommittedCaseAndDiscriminator() {
        Decision original = new Decision.Call(new CallPayload("payments", "charge.create"));

        String captured = codec.encode(original, Decision.class);
        Decision decoded = codec.decode(captured, Decision.class);

        Decision.Call call = assertInstanceOf(Decision.Call.class, decoded);
        assertEquals(original, call);
        assertEquals("call", call.discriminator());
    }

    @Test
    void preservesOutputTypeForScalarJsonNodesAndBigDecimalPrecisionForContainers() {
        String exact29 = "99999999999999999999.123456789";

        DoubleNode doubleNode = assertInstanceOf(DoubleNode.class, codec.decode("1.25", DoubleNode.class));
        assertEquals(1.25d, doubleNode.doubleValue());

        DecimalNode decimalNode = assertInstanceOf(DecimalNode.class, codec.decode(exact29, DecimalNode.class));
        assertEquals(exact29, decimalNode.decimalValue().toPlainString());

        JsonNode genericNode = codec.decode("{\"amount\":" + exact29 + "}", JsonNode.class);
        assertTrue(genericNode.path("amount").isBigDecimal());
        assertEquals(exact29, genericNode.path("amount").decimalValue().toPlainString());

        ObjectNode objectNode = assertInstanceOf(
            ObjectNode.class, codec.decode("{\"amount\":" + exact29 + "}", ObjectNode.class));
        assertTrue(objectNode.path("amount").isBigDecimal());
        assertEquals(exact29, objectNode.path("amount").decimalValue().toPlainString());

        ArrayNode arrayNode = assertInstanceOf(
            ArrayNode.class, codec.decode("[" + exact29 + "]", ArrayNode.class));
        assertTrue(arrayNode.get(0).isBigDecimal());
        assertEquals(exact29, arrayNode.get(0).decimalValue().toPlainString());
    }

    sealed interface Decision permits Decision.Call, Decision.Complete {
        String discriminator();

        record Call(CallPayload value) implements Decision {
            @Override
            public String discriminator() {
                return "call";
            }
        }

        record Complete(CompletePayload value) implements Decision {
            @Override
            public String discriminator() {
                return "complete";
            }
        }
    }

    record CallPayload(String binding, String operation) {
    }

    record CompletePayload(String status) {
    }
}
