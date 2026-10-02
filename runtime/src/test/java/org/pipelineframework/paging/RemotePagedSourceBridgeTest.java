package org.pipelineframework.paging;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import io.grpc.Status;
import io.smallrye.mutiny.Multi;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.jupiter.api.Test;

class RemotePagedSourceBridgeTest {
  private final RemotePagedSourceBridge bridge = new RemotePagedSourceBridge();
  private final PagedSourceRequest<String> request =
      new PagedSourceRequest<>("input", "snapshot", Optional.empty(), 2);

  @Test
  void demandKeepsOnlyOneItemAheadAndCompletionWaitsForNormalClose() {
    ControlledRemote remote = new ControlledRemote();
    PagedSourceStream<String> page = bridge.open(request, remote, frame -> frame);
    RecordingSubscriber items = new RecordingSubscriber();
    page.items().subscribe(items);

    assertFalse(remote.subscribed);
    items.subscription.request(1);
    assertEquals(1, remote.credits);
    remote.emit(new RemotePageFrame.Item<>("one"));
    assertEquals(1, remote.credits);
    remote.emit(new RemotePageFrame.Item<>("two"));
    assertEquals(List.of("one"), items.values);
    assertFalse(page.completion().toCompletableFuture().isDone());

    items.subscription.request(1);
    assertEquals(List.of("one", "two"), items.values);
    assertEquals(1, remote.credits);
    remote.emit(new RemotePageFrame.Completion<>(
        new PagedSourceCompletion(2, Optional.of("next"), false)));
    assertFalse(page.completion().toCompletableFuture().isDone());
    remote.complete();
    assertTrue(items.completed);
    assertEquals("next", page.completion().toCompletableFuture().join()
        .nextCheckpoint().orElseThrow());
  }

  @Test
  void missingOrRepeatedTerminalFrameCannotAdvanceCheckpoint() {
    ControlledRemote missing = new ControlledRemote();
    PagedSourceStream<String> missingPage = bridge.open(request, missing, frame -> frame);
    RecordingSubscriber first = new RecordingSubscriber();
    missingPage.items().subscribe(first);
    first.subscription.request(1);
    missing.complete();
    assertThrows(Exception.class, () -> missingPage.completion().toCompletableFuture().join());

    ControlledRemote repeated = new ControlledRemote();
    PagedSourceStream<String> repeatedPage = bridge.open(request, repeated, frame -> frame);
    RecordingSubscriber second = new RecordingSubscriber();
    repeatedPage.items().subscribe(second);
    second.subscription.request(1);
    var end = new RemotePageFrame.Completion<String>(
        new PagedSourceCompletion(0, Optional.empty(), true));
    repeated.emit(end);
    repeated.subscriber.onNext(end);
    assertTrue(repeated.cancelled);
    assertThrows(Exception.class, () -> repeatedPage.completion().toCompletableFuture().join());
  }

  @Test
  void cancellationFailsCompletionAndCancelsSource() {
    ControlledRemote remote = new ControlledRemote();
    PagedSourceStream<String> page = bridge.open(request, remote, frame -> frame);
    RecordingSubscriber items = new RecordingSubscriber();
    page.items().subscribe(items);
    items.subscription.request(1);
    items.subscription.cancel();
    assertTrue(remote.cancelled);
    assertThrows(Exception.class, () -> page.completion().toCompletableFuture().join());
  }

  @Test
  void missingRemoteCapabilityIsTerminal() {
    ControlledRemote remote = new ControlledRemote();
    PagedSourceStream<String> page = bridge.open(request, remote, frame -> frame);
    RecordingSubscriber items = new RecordingSubscriber();
    page.items().subscribe(items);
    items.subscription.request(1);
    remote.fail(new CompletionException(Status.UNIMPLEMENTED.asRuntimeException()));
    assertTrue(items.failure instanceof NonRetryableException);
    assertThrows(Exception.class, () -> page.completion().toCompletableFuture().join());
  }

  @Test
  void serverDoesNotEmitCompletionAfterResourceClosureFailure() {
    PagedSourceStream<String> page = new PagedSourceStream<>(
        Multi.createFrom().items("one"),
        CompletableFuture.failedFuture(new IllegalStateException("close failed")));
    assertThrows(Exception.class, () -> bridge.serve(page, request,
        RemotePageFrame.Item<String>::new, RemotePageFrame.Completion<String>::new)
        .collect().asList().await().indefinitely());
  }

  @Test
  void serverRejectsCompletionPastRecordLimit() {
    PagedSourceStream<String> page = new PagedSourceStream<>(
        Multi.createFrom().items("one"),
        CompletableFuture.completedFuture(new PagedSourceCompletion(3, Optional.empty(), true)));
    assertThrows(Exception.class, () -> bridge.serve(page, request,
        RemotePageFrame.Item<String>::new, RemotePageFrame.Completion<String>::new)
        .collect().asList().await().indefinitely());
  }

  @Test
  void sourceHostAcceptsPinnedReleaseAcrossDifferentModuleContractHashes() {
    PipelineReleaseIdentityResolver identity = mock(PipelineReleaseIdentityResolver.class);
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    PipelineContractDescriptor contract = mock(PipelineContractDescriptor.class);
    bridge.releaseIdentity = identity;
    bridge.orchestratorConfig = config;
    when(identity.pipelineId(config)).thenReturn("payments");
    when(identity.releaseVersion(config)).thenReturn("release-1");
    when(identity.contract()).thenReturn(contract);
    when(contract.canonicalCatalogFingerprint()).thenReturn("catalog-1");
    assertDoesNotThrow(() -> bridge.validateRelease("payments", "worker-contract",
        "release-1", "catalog-1"));
    assertThrows(IllegalArgumentException.class, () -> bridge.validateRelease("payments",
        "worker-contract", "other-release", "catalog-1"));
    assertThrows(IllegalArgumentException.class, () -> bridge.validateRelease("payments",
        "worker-contract", "release-1", "other-catalog"));
  }

  @Test
  void reentrantDemandDoesNotOverfillTheSingleItemBuffer() {
    int count = 1000;
    PagedSourceRequest<String> pageRequest = new PagedSourceRequest<>(
        "input", "snapshot", Optional.empty(), count);
    PagedSourceStream<String> page = bridge.open(pageRequest, new SynchronousRemote(count), frame -> frame);
    AtomicInteger received = new AtomicInteger();
    page.items().subscribe(new Flow.Subscriber<>() {
      private Flow.Subscription subscription;

      @Override public void onSubscribe(Flow.Subscription value) {
        subscription = value;
        subscription.request(1);
      }
      @Override public void onNext(String value) {
        received.incrementAndGet();
        subscription.request(1);
      }
      @Override public void onError(Throwable failure) {
        throw new AssertionError(failure);
      }
      @Override public void onComplete() { }
    });
    assertEquals(count, received.get());
    assertEquals(count, page.completion().toCompletableFuture().join().consumedRecords());
  }

  @Test
  void threeRemotePagesAdvanceFromTheirOwnOpaqueCheckpoints() {
    Optional<String> checkpoint = Optional.empty();
    List<String> received = new ArrayList<>();
    for (int pageIndex = 0; pageIndex < 3; pageIndex++) {
      int first = pageIndex * 2 + 1;
      boolean exhausted = pageIndex == 2;
      PagedSourceRequest<String> pageRequest = new PagedSourceRequest<>(
          "input", "snapshot", checkpoint, 2);
      PagedSourceCompletion result = new PagedSourceCompletion(
          exhausted ? 1 : 2,
          exhausted ? Optional.empty() : Optional.of("cursor-" + (pageIndex + 1)),
          exhausted);
      PagedSourceStream<String> source = new PagedSourceStream<>(
          exhausted ? Multi.createFrom().item(Integer.toString(first))
              : Multi.createFrom().items(Integer.toString(first), Integer.toString(first + 1)),
          CompletableFuture.completedFuture(result));
      PagedSourceStream<String> remote = bridge.open(pageRequest,
          bridge.serve(source, pageRequest,
              RemotePageFrame.Item<String>::new, RemotePageFrame.Completion<String>::new),
          frame -> frame);
      RecordingSubscriber items = new RecordingSubscriber();
      remote.items().subscribe(items);
      items.subscription.request(2);
      assertTrue(items.completed);
      assertEquals(result.consumedRecords(), items.values.size());
      checkpoint = remote.completion().toCompletableFuture().join().nextCheckpoint();
      received.addAll(items.values);
    }
    assertEquals(List.of("1", "2", "3", "4", "5"), received);
    assertTrue(checkpoint.isEmpty());
  }

  private static final class ControlledRemote implements Flow.Publisher<RemotePageFrame<String>> {
    private Flow.Subscriber<? super RemotePageFrame<String>> subscriber;
    private long credits;
    private boolean subscribed;
    private boolean cancelled;

    @Override
    public void subscribe(Flow.Subscriber<? super RemotePageFrame<String>> recipient) {
      subscribed = true;
      subscriber = recipient;
      recipient.onSubscribe(new Flow.Subscription() {
        @Override public void request(long amount) { credits += amount; }
        @Override public void cancel() { cancelled = true; }
      });
    }

    void emit(RemotePageFrame<String> frame) {
      assertTrue(credits > 0, "remote sent a frame without demand");
      credits--;
      subscriber.onNext(frame);
    }

    void complete() {
      subscriber.onComplete();
    }

    void fail(Throwable error) {
      subscriber.onError(error);
    }
  }

  private static final class SynchronousRemote implements Flow.Publisher<RemotePageFrame<String>> {
    private final int count;

    private SynchronousRemote(int count) {
      this.count = count;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super RemotePageFrame<String>> subscriber) {
      subscriber.onSubscribe(new Flow.Subscription() {
        private int emitted;
        private boolean closed;

        @Override public void request(long amount) {
          if (amount < 1) {
            throw new AssertionError("remote received zero demand");
          }
          for (long sent = 0; sent < amount && !closed; sent++) {
            if (emitted < count) {
              subscriber.onNext(new RemotePageFrame.Item<>(Integer.toString(++emitted)));
            } else {
              subscriber.onNext(new RemotePageFrame.Completion<>(
                  new PagedSourceCompletion(count, Optional.empty(), true)));
              closed = true;
              subscriber.onComplete();
            }
          }
        }

        @Override public void cancel() { closed = true; }
      });
    }
  }

  private static final class RecordingSubscriber implements Flow.Subscriber<String> {
    private final List<String> values = new ArrayList<>();
    private Flow.Subscription subscription;
    private boolean completed;
    private Throwable failure;

    @Override public void onSubscribe(Flow.Subscription value) { subscription = value; }
    @Override public void onNext(String value) { values.add(value); }
    @Override public void onError(Throwable value) { failure = value; }
    @Override public void onComplete() { completed = true; }
  }
}
