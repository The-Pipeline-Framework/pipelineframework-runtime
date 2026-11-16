package org.pipelineframework.orchestrator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.lambda.runtime.Context;
import com.sun.net.httpserver.HttpServer;
import io.quarkus.amazon.lambda.runtime.QuarkusStreamHandler;
import io.smallrye.config.SmallRyeConfigBuilder;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

/** Round trip between the SQS transition client and the unpacked worker ZIP in a fresh JVM. */
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
        var requests = new ArrayBlockingQueue<String>(1);
        var replies = new ArrayBlockingQueue<String>(1);
        var replyDeleted = new CompletableFuture<Void>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
        String requestUrl = endpoint + "/111111111111/transition-requests";
        String replyUrl = endpoint + "/222222222222/transition-replies";
        try (var executor = Executors.newCachedThreadPool();
             var sqs = SqsClient.builder().endpointOverride(URI.create(endpoint)).region(Region.US_EAST_2)
                 .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                     "fixture-coordinator-key", "fixture-coordinator-secret"))).build()) {
            server.setExecutor(executor);
            server.createContext("/", exchange -> {
                try (exchange) {
                    var request = json.readTree(exchange.getRequestBody());
                    var operation = exchange.getRequestHeaders().getFirst("X-Amz-Target");
                    var authorization = exchange.getRequestHeaders().getFirst("Authorization");
                    boolean coordinator = authorization != null && authorization.contains("Credential=fixture-coordinator-key/");
                    String queue = request.path("QueueUrl").asText();
                    Map<String, ?> response = switch (operation) {
                        case "AmazonSQS.SendMessage" -> {
                            String body = request.required("MessageBody").asText();
                            if (coordinator) {
                                require(requestUrl.equals(queue), "Coordinator must send to the request queue");
                                requests.add(body);
                            } else {
                                require(authorization != null && authorization.contains("Credential=test-access-key/"),
                                    "Worker must sign with its own AWS SDK credentials");
                                require(replyUrl.equals(queue), "Worker must send to the reply queue");
                                replies.add(body);
                            }
                            yield Map.of("MessageId", "fixture-message", "MD5OfMessageBody", md5(body));
                        }
                        case "AmazonSQS.ReceiveMessage" -> {
                            require(coordinator && replyUrl.equals(queue), "Coordinator must receive from the reply queue");
                            String body = replies.poll(1, TimeUnit.SECONDS);
                            yield body == null ? Map.of() : Map.of("Messages", List.of(Map.of("MessageId", "fixture-reply",
                                "ReceiptHandle", "fixture-receipt", "Body", body, "MD5OfBody", md5(body))));
                        }
                        case "AmazonSQS.DeleteMessage" -> {
                            require(coordinator && replyUrl.equals(queue)
                                && "fixture-receipt".equals(request.path("ReceiptHandle").asText()), "Unexpected reply deletion");
                            replyDeleted.complete(null);
                            yield Map.of();
                        }
                        default -> throw new AssertionError("Unexpected SQS API " + operation);
                    };
                    byte[] bytes = json.writeValueAsBytes(response);
                    exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.0");
                    exchange.sendResponseHeaders(200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                } catch (Exception | AssertionError failure) {
                    failure.printStackTrace(System.err);
                    replyDeleted.completeExceptionally(failure);
                }
            });
            server.start();
            try {
                System.setProperty("pipeline.orchestrator.sqs.endpoint-override", endpoint);
                System.setProperty("pipeline.orchestrator.worker.sqs.response-queue-url", replyUrl);
                var handler = new QuarkusStreamHandler();
                var config = new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class)
                    .withDefaultValue("pipeline.orchestrator.mode", "QUEUE_ASYNC")
                    .withDefaultValue("pipeline.orchestrator.worker.sqs.request-queue-url", requestUrl)
                    .withDefaultValue("pipeline.orchestrator.worker.sqs.response-queue-url", replyUrl)
                    .withDefaultValue("pipeline.orchestrator.worker.sqs.shared-secret", key)
                    .build().getConfigMapping(PipelineOrchestratorConfig.class);
                var transport = new SqsPipelineTransitionWorker(sqs, config, new PipelineInvocationRuntime());
                var command = new TransitionCommandEnvelope("fixture-tenant", "fixture-execution", args[0], args[1], args[2],
                    0, -1, 0, ExecutionResultShape.SINGLE, 1, "fixture-transition", "fixture-trace",
                    "com.example.echo.EchoInput", "application/tpf-transition+json", "{\"value\":\"worker-only\"}");
                var resultAwait = transport.executeTransition(command).subscribeAsCompletionStage();
                String request = requests.poll(10, TimeUnit.SECONDS);
                require(request != null, "Coordinator did not publish a transition command");
                invokeWorker(handler, request);
                var result = resultAwait.get(15, TimeUnit.SECONDS).toWireResult();
                replyDeleted.get(5, TimeUnit.SECONDS);
                require(result.outcome() == TransitionWorkerOutcome.COMPLETED, "Worker outcome: " + result);
                require(result.outputPayloads().size() == 1, "Expected one output");
                var payload = result.outputPayloads().getFirst();
                require("com.example.echo.EchoOutput".equals(payload.payloadTypeId()), "Incorrect output type");
                require("worker-only".equals(json.readTree(payload.payload()).path("value").asText()), "Incorrect output value");
            } finally {
                server.stop(0);
            }
        }
    }

    private static void invokeWorker(QuarkusStreamHandler handler, String body) throws Exception {
        var json = PipelineJson.mapper();
        byte[] event = json.writeValueAsBytes(Map.of("Records", List.of(Map.of("messageId", "fixture-message", "body", body))));
        var response = new ByteArrayOutputStream();
        var context = (Context) java.lang.reflect.Proxy.newProxyInstance(Context.class.getClassLoader(),
            new Class<?>[] {Context.class}, (proxy, method, parameters) -> switch (method.getName()) {
                case "getAwsRequestId" -> "fixture-invocation";
                case "getRemainingTimeInMillis" -> 30_000;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        handler.handleRequest(new ByteArrayInputStream(event), response, context);
        var failures = json.readTree(response.toByteArray()).path("batchItemFailures");
        require(failures.isArray() && failures.isEmpty(), "Expected acknowledged event: " + response);
    }

    private static String md5(String body) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(body.getBytes(StandardCharsets.UTF_8)));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
