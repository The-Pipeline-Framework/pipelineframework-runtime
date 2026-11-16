import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import com.amazonaws.services.lambda.runtime.Context;
import com.sun.net.httpserver.HttpServer;
import io.quarkus.amazon.lambda.runtime.QuarkusStreamHandler;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.SqsTransitionWorkerProtocol;
import org.pipelineframework.orchestrator.SqsTransitionWorkerRequest;
import org.pipelineframework.orchestrator.SqsTransitionWorkerResponse;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionWireResult;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;

/** Invoked in a fresh JVM against the unpacked deployment ZIP. */
class TransitionWorkerSmoke {
    public static void main(String[] args) {
        try {
            execute(args);
        } catch (Exception | AssertionError failure) {
            failure.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    private static void execute(String[] args) throws Exception {
        var json = PipelineJson.mapper();
        String key = System.getenv("TRANSITION_SIGNING_KEY");
        var sent = new ArrayBlockingQueue<SqsTransitionWorkerResponse>(1);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
        String replyUrl = endpoint + "/123456789012/transition-replies";
        server.createContext("/", exchange -> {
            try (exchange) {
                var request = json.readTree(exchange.getRequestBody());
                require("AmazonSQS.SendMessage".equals(exchange.getRequestHeaders().getFirst("X-Amz-Target")),
                    "Worker must only send an SQS response");
                require(replyUrl.equals(request.path("QueueUrl").asText()), "Unexpected response queue");
                String body = request.path("MessageBody").asText();
                sent.add(json.readValue(body, SqsTransitionWorkerResponse.class));
                String md5 = HexFormat.of().formatHex(MessageDigest.getInstance("MD5")
                    .digest(body.getBytes(StandardCharsets.UTF_8)));
                byte[] reply = json.writeValueAsBytes(Map.of("MessageId", "fixture-reply", "MD5OfMessageBody", md5));
                exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.0");
                exchange.sendResponseHeaders(200, reply.length);
                exchange.getResponseBody().write(reply);
            } catch (Exception failure) {
                throw new IllegalStateException("Local SQS fixture failed", failure);
            }
        });
        server.start();
        try {
            System.setProperty("pipeline.orchestrator.sqs.endpoint-override", endpoint);
            System.setProperty("pipeline.orchestrator.worker.sqs.response-queue-url", replyUrl);
            String command = json.writeValueAsString(new TransitionCommandEnvelope("fixture-tenant",
                "fixture-execution", args[0], args[1], args[2], 0, -1, 0, ExecutionResultShape.SINGLE,
                1, "fixture-transition", "fixture-trace", "com.example.echo.EchoInput",
                "application/tpf-transition+json", "{\"value\":\"worker-only\"}"));
            String requestId = "fixture-request";
            String timestamp = Instant.now().toString();
            String nonce = "fixture-request-nonce";
            String signature = TransitionWorkerSignature.sign(key, SqsTransitionWorkerProtocol.SIGNATURE_METHOD,
                SqsTransitionWorkerProtocol.REQUEST_SIGNATURE_PATH, timestamp, nonce,
                SqsTransitionWorkerProtocol.signedBytes(requestId, command));
            var request = new SqsTransitionWorkerRequest(requestId, SqsTransitionWorkerProtocol.PROTOCOL_VERSION,
                SqsTransitionWorkerProtocol.PAYLOAD_ENCODING, command, timestamp, nonce, signature);
            byte[] event = json.writeValueAsBytes(Map.of("Records",
                List.of(Map.of("messageId", "fixture-message", "body", json.writeValueAsString(request)))));
            var response = new ByteArrayOutputStream();
            var context = (Context) java.lang.reflect.Proxy.newProxyInstance(Context.class.getClassLoader(),
                new Class<?>[] {Context.class}, (proxy, method, parameters) -> switch (method.getName()) {
                    case "getAwsRequestId" -> "fixture-invocation";
                    case "getRemainingTimeInMillis" -> 30_000;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
            new QuarkusStreamHandler().handleRequest(new ByteArrayInputStream(event), response, context);

            var failures = json.readTree(response.toByteArray()).path("batchItemFailures");
            require(failures.isArray() && failures.isEmpty(), "Expected acknowledged event: " + response);
            var reply = sent.remove();
            require(requestId.equals(reply.requestId()), "Reply request ID changed");
            String expectedSignature = TransitionWorkerSignature.sign(key, SqsTransitionWorkerProtocol.SIGNATURE_METHOD,
                SqsTransitionWorkerProtocol.RESPONSE_SIGNATURE_PATH, reply.timestamp(), reply.nonce(),
                SqsTransitionWorkerProtocol.signedBytes(reply.requestId(), reply.resultEnvelope()));
            require(TransitionWorkerSignature.matches(expectedSignature, reply.signature()), "Invalid reply signature");
            var result = json.readValue(reply.resultEnvelope(), TransitionWireResult.class);
            require(result.outcome() == TransitionWorkerOutcome.COMPLETED, "Worker outcome: " + result);
            require(result.outputPayloads().size() == 1, "Expected one output");
            var payload = result.outputPayloads().getFirst();
            require("com.example.echo.EchoOutput".equals(payload.payloadTypeId()), "Incorrect output type");
            require("worker-only".equals(json.readTree(payload.payload()).path("value").asText()), "Incorrect output value");
        } finally {
            server.stop(0);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
