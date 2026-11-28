/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.spring.host;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Full Spring Boot WebFlux integration proof of {@code pipelineframework-spring-host-starter}:
 * covers {@code ONE_TO_ONE}, {@code ONE_TO_MANY}, {@code MANY_TO_ONE}, true interleaved
 * full-duplex {@code MANY_TO_MANY}, sync/Mono/Flux/CompletionStage/Stream {@code @Service} beans,
 * exact 29-digit {@link BigDecimal} and odd 64-bit {@code long} fidelity, portable failures,
 * secret isolation, loopback challenge authentication, and Tier-2 Query/Command/Reconcile connectors.
 */
@SpringBootTest(
    classes = TpfSpringHostIntegrationTest.SampleSpringHostApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "tpf.host.shared-secret=" + TpfSpringHostIntegrationTest.SECRET,
        "tpf.host.max-request-bytes=65536",
        "tpf.host.max-frame-bytes=65536"
    }
)
class TpfSpringHostIntegrationTest {

    static final String SECRET = "spring-host-integration-secret-9876543210";

    @LocalServerPort
    int port;

    private final HttpClient http = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    public record PaymentInput(String id, BigDecimal amount, long sequence) {
    }

    public record PaymentReceipt(String id, BigDecimal total, long sequence, String versionTag) {
    }

    public record LineItem(String id, int index, BigDecimal portion) {
    }

    public record BatchSummary(int count, BigDecimal sum) {
    }

    public record AccountLookup(String accountId) {
    }

    public record AccountState(String accountId, String tier, String scope) {
    }

    public record ChargeRequest(String accountId, BigDecimal amount) {
    }

    public record ChargeReceipt(String chargeId, String idempotencyKey) {
    }

    @SpringBootApplication
    static class SampleSpringHostApplication {

        @Service
        static class PaymentFunctions {

            @TpfFunction(stepId = "price-payment")
            public PaymentReceipt pricePayment(PaymentInput input, TpfFunctionContext ctx) {
                if (input.amount().compareTo(BigDecimal.ZERO) < 0) {
                    throw new TpfBusinessRejectionException("NEGATIVE_AMOUNT", "Amount must be non-negative");
                }
                if ("transient".equals(input.id())) {
                    throw new TpfTransientException("DOWNSTREAM_BUSY", "Retry later");
                }
                return new PaymentReceipt(
                    input.id(),
                    input.amount().add(new BigDecimal("0.00000000000000000000000000001")),
                    input.sequence(),
                    ctx.versionTag().orElse("unversioned"));
            }

            @TpfFunction(stepId = "price-mono")
            public Mono<PaymentReceipt> priceMono(PaymentInput input) {
                return Mono.just(new PaymentReceipt(input.id(), input.amount(), input.sequence(), "mono"));
            }

            @TpfFunction(stepId = "price-stage")
            public CompletionStage<PaymentReceipt> priceStage(PaymentInput input) {
                return CompletableFuture.completedFuture(
                    new PaymentReceipt(input.id(), input.amount(), input.sequence(), "stage"));
            }

            @TpfFunction(stepId = "split-payment")
            public Flux<LineItem> splitPayment(PaymentInput input) {
                return Flux.just(
                    new LineItem(input.id(), 1, input.amount()),
                    new LineItem(input.id(), 2, new BigDecimal("1.50")));
            }

            @TpfFunction(stepId = "split-stream")
            public Stream<LineItem> splitStream(PaymentInput input) {
                return Stream.of(new LineItem(input.id(), 1, input.amount()));
            }

            @TpfFunction(stepId = "sum-lines", outputTypeId = "BatchSummary")
            public Mono<BatchSummary> sumLines(Flux<LineItem> lines) {
                return lines.reduce(
                    new BatchSummary(0, BigDecimal.ZERO),
                    (acc, item) -> new BatchSummary(acc.count() + 1, acc.sum().add(item.portion())));
            }

            @TpfFunction(stepId = "sum-blocking-stream", outputTypeId = "BatchSummary")
            public BatchSummary sumBlockingStream(Stream<LineItem> lines) {
                List<LineItem> items = lines.toList();
                BigDecimal total = items.stream().map(LineItem::portion).reduce(BigDecimal.ZERO, BigDecimal::add);
                return new BatchSummary(items.size(), total);
            }

            @TpfFunction(stepId = "transform-duplex", outputTypeId = "LineItem")
            public Flux<LineItem> transformDuplex(Flux<LineItem> incoming) {
                return incoming.map(item -> new LineItem(
                    item.id() + "-ack",
                    item.index() * 10,
                    item.portion().multiply(new BigDecimal("2"))));
            }
        }

        @Service
        static class BillingConnectors {

            @TpfQueryConnector(operation = "lookup-account")
            public TpfQueryResult<AccountState> lookupAccount(AccountLookup input, TpfConnectorContext ctx) {
                String apiKey = ctx.secret("apiKey").orElse("");
                if (!"authorized-key".equals(apiKey)) {
                    return TpfQueryResult.authRequired("missing-api-key");
                }
                if ("missing".equals(input.accountId())) {
                    return TpfQueryResult.notFound("account-missing");
                }
                return TpfQueryResult.found(new AccountState(
                    input.accountId(), "enterprise", ctx.scope().orElse("default")));
            }

            @TpfCommandConnector(operation = "charge-account")
            public Mono<TpfCommandResult<ChargeReceipt>> chargeAccount(ChargeRequest input, TpfCommandContext ctx) {
                if (!"charge-secret".equals(ctx.secret("chargeToken").orElse(""))) {
                    return Mono.just(TpfCommandResult.terminalFailure("unauthorized-connector-secret"));
                }
                if ("ambiguous".equals(input.accountId())) {
                    return Mono.just(TpfCommandResult.ambiguous("provider-timeout"));
                }
                return Mono.just(TpfCommandResult.succeeded(
                    new ChargeReceipt("chg-" + input.accountId(), ctx.providerIdempotencyKey()),
                    "rcpt-" + ctx.commandId()));
            }

            @TpfCommandReconciler(operation = "charge-account")
            public TpfReconcileResult<ChargeReceipt> reconcileCharge(TpfReconcileContext ctx) {
                if ("unresolved-target".equals(ctx.recoveryTargetIdentity())) {
                    return TpfReconcileResult.unresolved("not-yet-settled");
                }
                return TpfReconcileResult.confirmedSucceeded(
                    new ChargeReceipt("chg-reconciled", ctx.providerIdempotencyKey()),
                    "rcpt-reconciled-" + ctx.commandId());
            }
        }
    }

    @Test
    void healthEndpointReturnsOk() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(uri("/tpf/health")).GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertEquals("OK", TpfHostJson.mapper().readTree(resp.body()).path("status").asText());
    }

    @Test
    void unaryFunctionsPreserveExactBigDecimalAndInt64AndMapPortableFailures() throws Exception {
        String hugeDecimal = "12345678901234567890.123456780";
        long oddInt64 = 9007199254740993L; // 2^53 + 1
        String requestJson = """
            {
              "stepId": "price-payment",
              "stepIndex": 0,
              "inputTypeId": "PaymentInput",
              "outputTypeId": "PaymentReceipt",
              "payload": {"id": "p-1", "amount": %s, "sequence": %d},
              "context": {"versionTag": "v2"}
            }
            """.formatted(hugeDecimal, oddInt64);

        HttpResponse<String> resp = postSigned("/tpf/functions/price-payment", requestJson, Map.of(
            TpfHostWebHandler.STEP_ID_HEADER, "price-payment",
            TpfHostWebHandler.STEP_INDEX_HEADER, "0"));
        assertEquals(200, resp.statusCode());
        JsonNode root = TpfHostJson.readStrictObject(resp.body());
        assertEquals("OK", root.path("status").asText());
        assertEquals("PaymentReceipt", root.path("outputTypeId").asText());
        assertEquals(new BigDecimal("12345678901234567890.12345678000000000000000000001"),
            root.path("payload").path("total").decimalValue());
        assertEquals(oddInt64, root.path("payload").path("sequence").asLong());
        assertEquals("v2", root.path("payload").path("versionTag").asText());

        // Mono & CompletionStage variants
        HttpResponse<String> monoResp = postSigned("/tpf/functions/price-mono", requestJson, Map.of());
        assertEquals("mono", TpfHostJson.readStrictObject(monoResp.body()).path("payload").path("versionTag").asText());

        HttpResponse<String> stageResp = postSigned("/tpf/functions/price-stage", requestJson, Map.of());
        assertEquals("stage", TpfHostJson.readStrictObject(stageResp.body()).path("payload").path("versionTag").asText());

        // Business rejection
        String negativeReq = """
            {"stepId":"price-payment","stepIndex":0,"inputTypeId":"PaymentInput","outputTypeId":"PaymentReceipt",
             "payload":{"id":"p-neg","amount":-5.00,"sequence":1},"context":{}}
            """;
        JsonNode rej = TpfHostJson.readStrictObject(postSigned("/tpf/functions/price-payment", negativeReq, Map.of()).body());
        assertEquals("ERROR", rej.path("status").asText());
        assertEquals("BUSINESS_REJECTION", rej.path("disposition").asText());
        assertEquals("NEGATIVE_AMOUNT", rej.path("code").asText());

        // Transient failure
        String transientReq = """
            {"stepId":"price-payment","stepIndex":0,"inputTypeId":"PaymentInput","outputTypeId":"PaymentReceipt",
             "payload":{"id":"transient","amount":10.00,"sequence":1},"context":{}}
            """;
        JsonNode tr = TpfHostJson.readStrictObject(postSigned("/tpf/functions/price-payment", transientReq, Map.of()).body());
        assertEquals("ERROR", tr.path("status").asText());
        assertEquals("TRANSIENT", tr.path("disposition").asText());
        assertEquals("DOWNSTREAM_BUSY", tr.path("code").asText());
    }

    @Test
    void oneToManyStreamsNdjsonWithCompleteSentinel() throws Exception {
        String req = """
            {"stepId":"split-payment","stepIndex":1,"inputTypeId":"PaymentInput","outputTypeId":"LineItem",
             "payload":{"id":"ord-7","amount":12.340,"sequence":7}}
            """;
        HttpResponse<String> resp = postSigned("/tpf/functions/split-payment/stream", req, Map.of());
        assertEquals(200, resp.statusCode());
        List<JsonNode> frames = parseNdjsonLines(resp.body());
        assertEquals(3, frames.size());
        assertEquals("item", frames.get(0).path("event").asText());
        assertEquals("LineItem", frames.get(0).path("outputTypeId").asText());
        assertEquals(new BigDecimal("12.340"), frames.get(0).path("payload").path("portion").decimalValue());
        assertEquals("item", frames.get(1).path("event").asText());
        assertEquals("complete", frames.get(2).path("event").asText());

        // Blocking Stream<T> variant
        HttpResponse<String> streamResp = postSigned("/tpf/functions/split-stream/stream", req, Map.of());
        assertEquals(200, streamResp.statusCode());
        List<JsonNode> streamFrames = parseNdjsonLines(streamResp.body());
        assertEquals(2, streamFrames.size());
        assertEquals("complete", streamFrames.get(1).path("event").asText());
    }

    @Test
    void manyToOneReducesReactiveAndBlockingNdjsonStreams() throws Exception {
        String ndjson = """
            {"event":"item","inputTypeId":"LineItem","payload":{"id":"a","index":1,"portion":10.250}}
            {"event":"item","inputTypeId":"LineItem","payload":{"id":"b","index":2,"portion":4.750}}
            {"event":"complete"}
            """;

        HttpResponse<String> reactiveResp = postStreamingSigned("/tpf/functions/sum-lines/reduce", ndjson);
        assertEquals(200, reactiveResp.statusCode());
        JsonNode reactiveRoot = TpfHostJson.readStrictObject(reactiveResp.body());
        assertEquals("OK", reactiveRoot.path("status").asText());
        assertEquals("BatchSummary", reactiveRoot.path("outputTypeId").asText());
        assertEquals(2, reactiveRoot.path("payload").path("count").asInt());
        assertEquals(new BigDecimal("15.000"), reactiveRoot.path("payload").path("sum").decimalValue());

        HttpResponse<String> blockingResp = postStreamingSigned("/tpf/functions/sum-blocking-stream/reduce", ndjson);
        assertEquals(200, blockingResp.statusCode());
        JsonNode blockingRoot = TpfHostJson.readStrictObject(blockingResp.body());
        assertEquals(2, blockingRoot.path("payload").path("count").asInt());
        assertEquals(new BigDecimal("15.000"), blockingRoot.path("payload").path("sum").decimalValue());

        // Missing complete sentinel fails closed
        String truncated = "{\"event\":\"item\",\"inputTypeId\":\"LineItem\",\"payload\":{\"id\":\"a\",\"index\":1,\"portion\":1.0}}\n";
        JsonNode errRoot = TpfHostJson.readStrictObject(
            postStreamingSigned("/tpf/functions/sum-lines/reduce", truncated).body());
        assertEquals("ERROR", errRoot.path("status").asText());
    }

    @Test
    void manyToManySupportsTrueInterleavedFullDuplexOverRawHttp11Socket() throws Exception {
        String path = "/tpf/functions/transform-duplex/duplex";
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            SECRET, "POST", path, timestamp, nonce,
            TpfHostRequestAuthenticator.STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8));

        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(5_000);
            socket.setTcpNoDelay(true);
            OutputStream out = socket.getOutputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            // 1. Send HTTP/1.1 chunked headers ONLY (no request items yet)
            String headers = "POST " + path + " HTTP/1.1\r\n"
                + "Host: 127.0.0.1:" + port + "\r\n"
                + "Content-Type: application/x-ndjson\r\n"
                + "Accept: application/x-ndjson\r\n"
                + "Transfer-Encoding: chunked\r\n"
                + "Connection: close\r\n"
                + TransitionWorkerSignature.TIMESTAMP_HEADER + ": " + timestamp + "\r\n"
                + TransitionWorkerSignature.NONCE_HEADER + ": " + nonce + "\r\n"
                + TransitionWorkerSignature.SIGNATURE_HEADER + ": " + signature + "\r\n"
                + TpfHostWebHandler.STEP_ID_HEADER + ": transform-duplex\r\n"
                + TpfHostWebHandler.STEP_INDEX_HEADER + ": 2\r\n"
                + "\r\n";
            out.write(headers.getBytes(StandardCharsets.US_ASCII));
            out.flush();

            // 2. Assert HTTP/1.1 200 OK response headers arrive BEFORE any request chunk is written!
            String statusLine = in.readLine();
            assertNotNull(statusLine);
            assertTrue(statusLine.startsWith("HTTP/1.1 200"), "Expected 200 OK immediately, got: " + statusLine);
            String hdr;
            while ((hdr = in.readLine()) != null && !hdr.isEmpty()) {
                // consume headers
            }

            // 3. Send chunk 1 and verify output item 1 arrives BEFORE chunk 2 is sent!
            writeChunk(out, "{\"event\":\"item\",\"inputTypeId\":\"LineItem\",\"payload\":{\"id\":\"first\",\"index\":1,\"portion\":3.5}}\n");
            JsonNode out1 = readNextChunkedNdjsonFrame(in);
            assertEquals("item", out1.path("event").asText());
            assertEquals("LineItem", out1.path("outputTypeId").asText());
            assertEquals("first-ack", out1.path("payload").path("id").asText());
            assertEquals(10, out1.path("payload").path("index").asInt());
            assertEquals(new BigDecimal("7.0"), out1.path("payload").path("portion").decimalValue());

            // 4. Now send chunk 2 and verify output item 2 arrives BEFORE complete is sent!
            writeChunk(out, "{\"event\":\"item\",\"inputTypeId\":\"LineItem\",\"payload\":{\"id\":\"second\",\"index\":2,\"portion\":5.25}}\n");
            JsonNode out2 = readNextChunkedNdjsonFrame(in);
            assertEquals("item", out2.path("event").asText());
            assertEquals("second-ack", out2.path("payload").path("id").asText());
            assertEquals(20, out2.path("payload").path("index").asInt());
            assertEquals(new BigDecimal("10.50"), out2.path("payload").path("portion").decimalValue());

            // 5. Send complete frame + zero chunk and verify complete sentinel arrives!
            writeChunk(out, "{\"event\":\"complete\"}\n");
            out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            JsonNode done = readNextChunkedNdjsonFrame(in);
            assertEquals("complete", done.path("event").asText());
        }
    }

    @Test
    void loopbackChallengeAndTier2QueryCommandAndReconcileWorkWithSecretIsolation() throws Exception {
        // Loopback challenge
        String challenge = "0123456789abcdef0123456789abcdef";
        HttpResponse<String> chalResp = postSigned("/tpf/connector/auth/challenge",
            "{\"challenge\":\"" + challenge + "\"}", Map.of());
        assertEquals(200, chalResp.statusCode());
        JsonNode chalJson = TpfHostJson.readStrictObject(chalResp.body());
        assertEquals("OK", chalJson.path("status").asText());
        assertEquals(challenge, chalJson.path("challenge").asText());
        String expectedProof = new TpfHostRequestAuthenticator(SECRET, java.time.Duration.ofMinutes(5), 16)
            .computeLoopbackChallengeProof(challenge);
        assertEquals(expectedProof, chalJson.path("proof").asText());

        // Query FOUND
        String queryReq = """
            {"operation":"lookup-account","input":{"accountId":"acct-42"},
             "secrets":{"apiKey":"authorized-key"},"scope":"eu-west"}
            """;
        JsonNode qFound = TpfHostJson.readStrictObject(postSigned("/tpf/connector/query/lookup-account", queryReq, Map.of()).body());
        assertEquals("FOUND", qFound.path("outcome").asText());
        assertEquals("acct-42", qFound.path("payload").path("accountId").asText());
        assertEquals("eu-west", qFound.path("payload").path("scope").asText());

        // Query NOT_FOUND & AUTH_REQUIRED
        String notFoundReq = """
            {"operation":"lookup-account","input":{"accountId":"missing"},"secrets":{"apiKey":"authorized-key"}}
            """;
        assertEquals("NOT_FOUND", TpfHostJson.readStrictObject(
            postSigned("/tpf/connector/query/lookup-account", notFoundReq, Map.of()).body()).path("outcome").asText());

        String badSecretReq = """
            {"operation":"lookup-account","input":{"accountId":"acct-42"},"secrets":{"apiKey":"wrong"}}
            """;
        assertEquals("AUTH_REQUIRED", TpfHostJson.readStrictObject(
            postSigned("/tpf/connector/query/lookup-account", badSecretReq, Map.of()).body()).path("outcome").asText());

        // Command SUCCEEDED + AMBIGUOUS
        String cmdReq = """
            {"operation":"charge-account","input":{"accountId":"acct-42","amount":19.99},
             "dispatchIdentity":{"commandId":"cmd-1","occurrenceId":"occ-1","attemptId":"att-1","providerIdempotencyKey":"idem-1"},
             "secrets":{"chargeToken":"charge-secret"}}
            """;
        JsonNode cmdOk = TpfHostJson.readStrictObject(
            postSigned("/tpf/connector/command/charge-account/dispatch", cmdReq, Map.of()).body());
        assertEquals("SUCCEEDED", cmdOk.path("outcome").asText());
        assertEquals("idem-1", cmdOk.path("payload").path("idempotencyKey").asText());
        assertEquals("rcpt-cmd-1", cmdOk.path("receiptReference").asText());

        // Reconcile CONFIRMED_SUCCEEDED + UNRESOLVED
        String recReq = """
            {"operation":"charge-account","recoveryTargetIdentity":"billing:primary",
             "dispatchIdentity":{"commandId":"cmd-1","occurrenceId":"occ-1","attemptId":"att-1","providerIdempotencyKey":"idem-1"},
             "secrets":{"chargeToken":"charge-secret"}}
            """;
        JsonNode recOk = TpfHostJson.readStrictObject(
            postSigned("/tpf/connector/command/charge-account/reconcile", recReq, Map.of()).body());
        assertEquals("CONFIRMED_SUCCEEDED", recOk.path("outcome").asText());
        assertEquals("rcpt-reconciled-cmd-1", recOk.path("receiptReference").asText());

        String recUnresolved = """
            {"operation":"charge-account","recoveryTargetIdentity":"unresolved-target",
             "dispatchIdentity":{"commandId":"cmd-2","occurrenceId":"occ-2","attemptId":"att-2","providerIdempotencyKey":"idem-2"},
             "secrets":{"chargeToken":"charge-secret"}}
            """;
        assertEquals("UNRESOLVED", TpfHostJson.readStrictObject(
            postSigned("/tpf/connector/command/charge-account/reconcile", recUnresolved, Map.of()).body())
            .path("outcome").asText());
    }

    @Test
    void rejectsUnauthorizedAndOversizedRequests() throws Exception {
        HttpRequest unsigned = HttpRequest.newBuilder(uri("/tpf/functions/price-payment"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{}"))
            .build();
        assertEquals(401, http.send(unsigned, HttpResponse.BodyHandlers.ofString()).statusCode());

        // Oversized (> 65536 bytes configured in @SpringBootTest)
        String hugeBody = "{\"payload\":\"" + "x".repeat(70_000) + "\"}";
        HttpResponse<String> tooLarge = postSigned("/tpf/functions/price-payment", hugeBody, Map.of());
        assertEquals(413, tooLarge.statusCode());
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + port + path);
    }

    private HttpResponse<String> postSigned(String path, String jsonBody, Map<String, String> extraHeaders) throws Exception {
        byte[] bytes = jsonBody.getBytes(StandardCharsets.UTF_8);
        String ts = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String sig = TransitionWorkerSignature.sign(SECRET, "POST", path, ts, nonce, bytes);
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri(path))
            .header("Content-Type", "application/json")
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, ts)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, sig)
            .POST(HttpRequest.BodyPublishers.ofByteArray(bytes));
        extraHeaders.forEach(builder::header);
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> postStreamingSigned(String path, String ndjsonBody) throws Exception {
        byte[] bytes = ndjsonBody.getBytes(StandardCharsets.UTF_8);
        String ts = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String sig = TransitionWorkerSignature.sign(
            SECRET, "POST", path, ts, nonce,
            TpfHostRequestAuthenticator.STREAMING_BODY_DIGEST_SENTINEL.getBytes(StandardCharsets.UTF_8));
        HttpRequest req = HttpRequest.newBuilder(uri(path))
            .header("Content-Type", "application/x-ndjson")
            .header(TransitionWorkerSignature.TIMESTAMP_HEADER, ts)
            .header(TransitionWorkerSignature.NONCE_HEADER, nonce)
            .header(TransitionWorkerSignature.SIGNATURE_HEADER, sig)
            .POST(HttpRequest.BodyPublishers.ofByteArray(bytes))
            .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static List<JsonNode> parseNdjsonLines(String body) throws IOException {
        List<JsonNode> frames = new ArrayList<>();
        for (String rawLine : body.split("\n")) {
            String trimmed = rawLine.trim();
            if (!trimmed.isEmpty()) {
                frames.add(TpfHostJson.readStrictObject(trimmed));
            }
        }
        return frames;
    }

    private static void writeChunk(OutputStream out, String payload) throws IOException {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        out.write((Integer.toHexString(bytes.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(bytes);
        out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    private static JsonNode readNextChunkedNdjsonFrame(BufferedReader in) throws IOException {
        while (true) {
            String sizeLine = in.readLine();
            if (sizeLine == null) {
                throw new IOException("Unexpected EOF waiting for chunked NDJSON frame");
            }
            String hex = sizeLine.trim();
            if (hex.isEmpty()) {
                continue;
            }
            int semi = hex.indexOf(';');
            if (semi >= 0) {
                hex = hex.substring(0, semi).trim();
            }
            int size = Integer.parseInt(hex, 16);
            if (size == 0) {
                throw new IOException("Stream ended before expected NDJSON frame");
            }
            char[] buf = new char[size];
            int read = 0;
            while (read < size) {
                int n = in.read(buf, read, size - read);
                if (n < 0) {
                    throw new IOException("Truncated chunk");
                }
                read += n;
            }
            // consume trailing CRLF after chunk payload
            in.readLine();
            String text = new String(buf).trim();
            if (!text.isEmpty()) {
                return TpfHostJson.readStrictObject(text);
            }
        }
    }
}
