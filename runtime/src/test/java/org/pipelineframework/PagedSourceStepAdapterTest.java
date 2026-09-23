package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PagedTransitionContext;
import org.pipelineframework.paging.PagedSourceCompletion;
import org.pipelineframework.paging.PagedSourceOperation;
import org.pipelineframework.paging.PagedSourceRequest;
import org.pipelineframework.paging.PagedSourceStream;
import org.pipelineframework.telemetry.PageExecutionTelemetry;

class PagedSourceStepAdapterTest {

    @Test
    void openingAndSubscribingDoNotDrainThePageAheadOfDemand() {
        AtomicInteger requests = new AtomicInteger();
        CompletableFuture<PagedSourceCompletion> providerCompletion = new CompletableFuture<>();
        PagedSourceOperation<Object, Object> source = request -> new PagedSourceStream<>(
            twoItemPublisher(requests, providerCompletion), providerCompletion);
        AtomicReference<CompletionStage<PagedSourceCompletion>> capturedCompletion = new AtomicReference<>();
        PagedSourceStepAdapter adapter = new PagedSourceStepAdapter(
            source,
            new PagedTransitionContext(0, "snapshot-v1", Optional.empty(), 2),
            capturedCompletion,
            PageExecutionTelemetry.disabled(),
            false);

        AssertSubscriber<Object> subscriber = adapter.applyOneToMany("input")
            .subscribe().withSubscriber(AssertSubscriber.create(0));

        subscriber.assertHasNotReceivedAnyItem();
        assertEquals(0, requests.get());
        assertFalse(providerCompletion.isDone());

        subscriber.request(1).awaitItems(1);
        subscriber.assertItems("record-1");
        assertEquals(1, requests.get());
        assertFalse(providerCompletion.isDone());

        subscriber.request(1).awaitCompletion(Duration.ofSeconds(1));
        subscriber.assertItems("record-1", "record-2").assertCompleted();
        assertEquals(2, requests.get());
        assertTrue(providerCompletion.isDone());
        assertEquals(2, capturedCompletion.get().toCompletableFuture().join().consumedRecords());
    }

    private static Flow.Publisher<Object> twoItemPublisher(
        AtomicInteger requests,
        CompletableFuture<PagedSourceCompletion> completion
    ) {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private int emitted;
            private boolean terminated;

            @Override
            public void request(long n) {
                if (terminated || n < 1) {
                    return;
                }
                requests.incrementAndGet();
                long remaining = n;
                while (!terminated && remaining-- > 0 && emitted < 2) {
                    subscriber.onNext("record-" + ++emitted);
                }
                if (emitted == 2 && !terminated) {
                    terminated = true;
                    subscriber.onComplete();
                    completion.complete(new PagedSourceCompletion(2, Optional.empty(), true));
                }
            }

            @Override
            public void cancel() {
                terminated = true;
                completion.completeExceptionally(new IllegalStateException("cancelled"));
            }
        });
    }
}
