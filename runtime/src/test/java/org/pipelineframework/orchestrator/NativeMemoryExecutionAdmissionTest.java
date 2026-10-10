package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.release.*;

class NativeMemoryExecutionAdmissionTest {
    @Test
    void expiryNeverExpiresAdmissionOrRecreatesItsExecution() {
        var store = new InMemoryExecutionStateStore();
        var intent = intent("key", "original");
        var evidence = evidence();
        var wrapper = command(intent, evidence, 1);
        var created = store.createOrGetNativeAdmittedExecution(wrapper, evidence).await().indefinitely();
        assertTrue(created.newlyCreated());
        assertTrue(store.getExecution("tenant", created.receipt().executionId()).await().indefinitely().isEmpty());
        var replay = store.createOrGetNativeAdmittedExecution(wrapper, evidence).await().indefinitely();
        assertFalse(replay.newlyCreated());
        assertTrue(replay.creation().isEmpty());
        assertEquals(created.receipt(), replay.receipt());
        assertEquals(created.receipt(), store.lookupAdmission("tenant", "pipeline", "key").await().indefinitely().orElseThrow());
        assertThrows(IllegalStateException.class, () -> store.inspectExistingAdmission(intent("key", "changed")).await().indefinitely());
        assertTrue(store.lookupAdmission("other", "pipeline", "key").await().indefinitely().isEmpty());
        assertFalse(store.supportsExecutionAdmission());
    }

    @Test
    void concurrentIdenticalCreatesHaveExactlyOneNewExecution() throws InterruptedException {
        var store = new InMemoryExecutionStateStore();
        var wrapper = command(intent("key", "original"), evidence(), System.currentTimeMillis() / 1000 + 3600);
        var context = Thread.currentThread().getContextClassLoader();
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(8, task -> {
                var thread = new Thread(task, "admission-concurrency");
                thread.setContextClassLoader(context);
                return thread;
            })) {
            var ready = new java.util.concurrent.CountDownLatch(8);
            var start = new java.util.concurrent.CountDownLatch(1);
            var calls = java.util.stream.IntStream.range(0, 8).mapToObj(index -> CompletableFuture.supplyAsync(() -> {
                ready.countDown();
                try { assertTrue(start.await(20, java.util.concurrent.TimeUnit.SECONDS)); }
                catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IllegalStateException(error); }
                return store.createOrGetNativeAdmittedExecution(wrapper, evidence()).await().indefinitely();
            }, executor)).toList();
            try { assertTrue(ready.await(20, java.util.concurrent.TimeUnit.SECONDS)); }
            finally { start.countDown(); }
            var results = calls.stream().map(CompletableFuture::join).toList();
            assertEquals(1, results.stream().filter(ExecutionAdmissionResult::newlyCreated).count());
            results.forEach(result -> assertEquals(results.getFirst().receipt(), result.receipt()));
            var persisted = store.getExecution("tenant", results.getFirst().receipt().executionId()).await().indefinitely().orElseThrow();
            assertEquals("pipeline", persisted.pipelineId());
            assertEquals("contract", persisted.contractVersion());
            assertEquals("release", persisted.releaseVersion());
            assertEquals("original", persisted.inputPayload());
        }
    }

    @Test
    void legacyKeyIsNeverAdoptedEvenAfterItsExpiry() {
        var store = new InMemoryExecutionStateStore();
        var wrapper = command(intent("key", "original"), evidence(), 1);
        var legacy = store.createOrGetExecution(wrapper.execution()).await().indefinitely();
        assertThrows(IllegalStateException.class, () -> store.createOrGetNativeAdmittedExecution(wrapper, evidence()).await().indefinitely());
        assertTrue(store.lookupAdmission("tenant", "pipeline", "key").await().indefinitely().isEmpty());
        assertTrue(legacy.record().executionId().length() > 0);
    }

    @Test
    void malformedPayloadStringsRejectButValidUnicodeIsExact() {
        assertThrows(IllegalArgumentException.class, () -> NativeExecutionAdmission.utf8("\uD800"));
        assertThrows(IllegalArgumentException.class, () -> NativeExecutionAdmission.utf8("\uDC00"));
        assertArrayEquals("é😀?".getBytes(java.nio.charset.StandardCharsets.UTF_8), NativeExecutionAdmission.utf8("é😀?"));
    }

    private static ExecutionAdmissionIntent intent(String key, String bytes) {
        return new ExecutionAdmissionIntent(1, "tenant", "pipeline", key, "contract", "release", "UNI", "java.lang.String", "json",
            NativeExecutionAdmission.utf8(bytes), false);
    }
    private static ExecutionAdmissionCreateCommand command(ExecutionAdmissionIntent intent, PipelineReleaseEvidence evidence, long ttl) {
        return new ExecutionAdmissionCreateCommand(new ExecutionCreateCommand("tenant", "execution-key", "pipeline", "contract", "release",
            "original", ExecutionResultShape.SINGLE, System.currentTimeMillis(), ttl), intent, evidence.metadataFingerprint(),
            evidence.primaryArtifactId(), evidence.primaryArtifactDigest());
    }
    private static PipelineReleaseEvidence evidence() {
        return ExecutionAdmissionReleaseEvidence.snapshot(new PipelineReleaseRecord("tenant", "pipeline", "contract", "release",
            PipelineReleaseStatus.REGISTERED, new PipelineReleaseDescriptor(1, "pipeline", "contract", "release", "artifact", List.of()),
            "artifact", "sha256:artifact", "file:/artifact.jar", 1, "artifact", null, 1, 1, 0));
    }
}
