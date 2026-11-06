package org.pipelineframework.aws.durable;

import java.lang.reflect.*;
import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import io.smallrye.config.*;
import org.pipelineframework.command.*;
import org.pipelineframework.connector.*;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.release.*;
import org.pipelineframework.pipeline.service.pipeline.ProcessEchoCommandClientStep;
import com.example.echo.*;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

/** Local transport fixture; loaded generated application and Dynamo store execute production code. */
public final class HostCommandProof {
    private static final String SECRET = "fixture-worker-signing-key-32-characters";
    /** Makes proof failure observable to the separate harness process. */
    public static void main(String[] args) { try { run(args); System.exit(0); } catch (Throwable failure) { failure.printStackTrace(); System.exit(1); } }
    /** Exercises signed dispatch, durable replay and tenant isolation, always removing its table. */
    static void run(String[] args) throws Exception {
        var contract = new PipelineContractDescriptorLoader().load().orElseThrow();
        var config = new SmallRyeConfigBuilder().withMapping(PipelineOrchestratorConfig.class)
            .withSources(new PropertiesConfigSource(Map.of(
                "pipeline.orchestrator.mode", "QUEUE_ASYNC",
                "pipeline.orchestrator.pipeline-id", contract.pipelineId(),
                "pipeline.orchestrator.release-version", "native-command-release-5",
                "pipeline.orchestrator.worker.sqs.shared-secret", SECRET,
                "pipeline.orchestrator.worker.sqs.response-queue-url", "http://localhost/response"), "proof", 300))
            .build().getConfigMapping(PipelineOrchestratorConfig.class);
        try (var dynamo = DynamoDbClient.builder().endpointOverride(URI.create(args[0])).region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))).build()) {
            String table = "command-proof-" + UUID.randomUUID();
            dynamo.createTable(CreateTableRequest.builder().tableName(table).billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(AttributeDefinition.builder().attributeName("command_key").attributeType(ScalarAttributeType.S).build(),
                    AttributeDefinition.builder().attributeName("revision").attributeType(ScalarAttributeType.N).build())
                .keySchema(KeySchemaElement.builder().attributeName("command_key").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("revision").keyType(KeyType.RANGE).build()).build());
            try {
                var provider = new EchoProvider();
                var registry = new ConnectorRegistry(List.of(provider));
                var definitions = List.of(new ConnectorBindingDefinition(ConnectorBindingName.of("store"), ConnectorProviderId.of("fixture.echo"), 1, new ConnectorConfigurationDocument(Map.of())));
                var bindings = org.pipelineframework.connector.ConnectorRegistryLifecycle.createBindingRegistry(definitions, List.of(provider));
                var output = new AtomicReference<Optional<String>>(Optional.empty());
                var sqs = (SqsClient) Proxy.newProxyInstance(SqsClient.class.getClassLoader(), new Class<?>[]{SqsClient.class}, (proxy, method, params) -> {
                    if (method.getName().equals("sendMessage") && params[0] instanceof SendMessageRequest request) { output.set(Optional.of(request.messageBody())); return SendMessageResponse.builder().messageId("response").build(); }
                    if (method.getName().equals("serviceName")) return "sqs";
                    if (method.getName().equals("close")) return null;
                    throw new java.lang.UnsupportedOperationException(method.toString());
                });
                for (int attempt = 0; attempt < 3; attempt++) {
                    // New store/runtime/worker instances prove the result is not retained process memory.
                    var store = new DynamoCommandEffectStore(dynamo, table);
                    var generated = new ProcessEchoCommandClientStep();
                    set(generated, "support", new CommandStepSupport(registry, bindings, List.of(store), config));
                    set(generated, "commandIdGenerator", new StableId());
                    var service = org.pipelineframework.CommandApplicationFixture.application(config, contract, "native-command-release-5", List.of(generated));
                    var action = new SqsTransitionWorkerAction();
                    set(action, "orchestratorConfig", config); set(action, "executionService", service); set(action, "client", sqs);
                    var handler = new AwsDurableTransitionWorkerHandler(); handler.action = action;
                    var command = new TransitionCommandEnvelope(attempt < 2 ? "tenant" : "other-tenant", "execution", contract.pipelineId(), contract.contractVersion(), "native-command-release-5", 0, -1, 0,
                        ExecutionResultShape.SINGLE, 1, "execution:0:0", "trace", EchoInput.class.getName(), JsonTransitionPayloadCodec.ENCODING, "{\"value\":\"hello\"}");
                    String body = PipelineJson.mapper().writeValueAsString(command), timestamp = Instant.now().toString(), nonce = UUID.randomUUID().toString();
                    String signature = TransitionWorkerSignature.sign(SECRET, SqsTransitionWorkerProtocol.SIGNATURE_METHOD, SqsTransitionWorkerProtocol.REQUEST_SIGNATURE_PATH, timestamp, nonce, SqsTransitionWorkerProtocol.signedBytes("request", body));
                    var request = new SqsTransitionWorkerRequest("request", SqsTransitionWorkerProtocol.PROTOCOL_VERSION, SqsTransitionWorkerProtocol.PAYLOAD_ENCODING, body, timestamp, nonce, signature);
                    var msg = new SQSEvent.SQSMessage(); msg.setMessageId("message"); msg.setBody(PipelineJson.mapper().writeValueAsString(request));
                    var event = new SQSEvent(); event.setRecords(List.of(msg));
                    if (attempt == 0) {
                        msg.setBody(PipelineJson.mapper().writeValueAsString(new SqsTransitionWorkerRequest("request", SqsTransitionWorkerProtocol.PROTOCOL_VERSION,
                            SqsTransitionWorkerProtocol.PAYLOAD_ENCODING, body, timestamp, nonce, "invalid")));
                        handler.handleRequest(event, null);
                        if (EchoProvider.CALLS.get() != 0 || output.get().isPresent()) throw new AssertionError("Unauthenticated request had effects");
                        msg.setBody(PipelineJson.mapper().writeValueAsString(request));
                    }
                    output.set(Optional.empty());
                    if (!handler.handleRequest(event, null).getBatchItemFailures().isEmpty()) throw new AssertionError("Worker failed");
                    String captured = output.get().orElseThrow(() -> new AssertionError("Worker produced no response"));
                    var response = PipelineJson.mapper().readValue(captured, SqsTransitionWorkerResponse.class);
                    var wire = PipelineJson.mapper().readValue(response.resultEnvelope(), TransitionWireResult.class);
                    if (wire.outcome() != TransitionWorkerOutcome.COMPLETED) throw new AssertionError(response.resultEnvelope());
                    var actual = TransitionResultEnvelope.fromWireResult(wire).decodeOutputItems(new JsonTransitionPayloadCodec());
                    if (!actual.equals(List.of(new EchoOutput("hello")))) throw new AssertionError(actual);
                    if (EchoProvider.CALLS.get() != (attempt < 2 ? 1 : 2)) throw new AssertionError("Command dispatched again on replay");
                }
                if (dynamo.scan(ScanRequest.builder().tableName(table).build()).count() < 2) throw new AssertionError("Missing durable effect history");
            } finally {
                dynamo.deleteTable(DeleteTableRequest.builder().tableName(table).build());
            }
            System.out.println("PASS actual AWS host -> generated Command -> Dynamo effect store; new worker/store replay returned recorded output with one provider dispatch; invalid signature rejected and tenant effect histories isolated");
        }
    }
    /** Injects fixture-managed collaborators into the actual generated/runtime object. */
    static void set(Object instance, String name, Object value) throws Exception { Field f = instance.getClass().getDeclaredField(name); f.setAccessible(true); f.set(instance, value); }
}
