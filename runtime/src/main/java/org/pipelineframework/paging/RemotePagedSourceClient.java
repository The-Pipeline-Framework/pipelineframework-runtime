package org.pipelineframework.paging;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import jakarta.ws.rs.WebApplicationException;
import org.jboss.logging.Logger;
import org.pipelineframework.step.NonRetryableException;

/** One remote subscription with one extra frame credit for terminal page metadata. */
final class RemotePagedSourceClient<F, T> {
  private static final Logger LOG = Logger.getLogger(RemotePagedSourceClient.class);
  private final PagedSourceRequest<?> request;
  private final Flow.Publisher<F> remote;
  private final Function<F, RemotePageFrame<T>> decode;
  private final CompletableFuture<PagedSourceCompletion> completion = new CompletableFuture<>();
  private final AtomicBoolean subscribed = new AtomicBoolean();

  RemotePagedSourceClient(PagedSourceRequest<?> request, Flow.Publisher<F> remote,
      Function<F, RemotePageFrame<T>> decode) {
    this.request = Objects.requireNonNull(request, "request must not be null");
    this.remote = Objects.requireNonNull(remote, "remote must not be null");
    this.decode = Objects.requireNonNull(decode, "decode must not be null");
  }

  PagedSourceStream<T> stream() {
    return new PagedSourceStream<>(downstream -> {
      if (!subscribed.compareAndSet(false, true)) {
        downstream.onSubscribe(new Flow.Subscription() {
          @Override public void request(long n) { }
          @Override public void cancel() { }
        });
        downstream.onError(new IllegalStateException("a remote page permits one item subscription"));
        return;
      }
      downstream.onSubscribe(new BridgeSubscription(downstream));
    }, completion);
  }

  private final class BridgeSubscription implements Flow.Subscription, Flow.Subscriber<F> {
    private final Flow.Subscriber<? super T> downstream;
    private final AtomicInteger work = new AtomicInteger();
    private final AtomicInteger creditWork = new AtomicInteger();
    private Flow.Subscription upstream;
    private long demand;
    private boolean creditOutstanding;
    private T buffered;
    private PagedSourceCompletion terminal;
    private Throwable failure;
    private boolean started;
    private boolean remoteDone;
    private boolean cancelled;
    private boolean signalled;

    private BridgeSubscription(Flow.Subscriber<? super T> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void request(long n) {
      if (n <= 0) {
        fail(new IllegalArgumentException("reactive demand must be positive"));
        return;
      }
      boolean subscribeNow;
      synchronized (this) {
        if (cancelled || signalled) {
          return;
        }
        subscribeNow = !started;
        started = true;
        demand = addCap(demand, n);
      }
      drain();
      if (subscribeNow) {
        try {
          remote.subscribe(this);
        } catch (Throwable error) {
          fail(error);
        }
      }
    }

    @Override
    public void cancel() {
      Flow.Subscription current;
      synchronized (this) {
        if (cancelled || signalled) {
          return;
        }
        cancelled = true;
        buffered = null;
        current = upstream;
      }
      if (current != null) {
        current.cancel();
      }
      completion.completeExceptionally(new CancellationException("remote page cancelled"));
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      Objects.requireNonNull(subscription, "subscription must not be null");
      synchronized (this) {
        if (upstream != null || cancelled || signalled) {
          subscription.cancel();
          return;
        }
        upstream = subscription;
      }
      drain();
    }

    @Override
    public void onNext(F wireFrame) {
      RemotePageFrame<T> frame;
      try {
        frame = Objects.requireNonNull(decode.apply(wireFrame), "decoded frame must not be null");
      } catch (Throwable error) {
        fail(error);
        return;
      }
      Throwable protocolFailure;
      synchronized (this) {
        if (cancelled || signalled) {
          return;
        }
        creditOutstanding = false;
        if (terminal != null) {
          failure = new IllegalStateException("frame followed remote page completion");
        } else if (frame instanceof RemotePageFrame.Item<?> item) {
          if (buffered != null) {
            failure = new IllegalStateException("remote page exceeded its one-item buffer");
          } else {
            @SuppressWarnings("unchecked") T value = (T) item.value();
            buffered = value;
          }
        } else if (frame instanceof RemotePageFrame.Completion<?> end) {
          try {
            end.value().validateAgainst(request);
            terminal = end.value();
            LOG.infof("event=remote_page_client_completion_frame consumedRecords=%d exhausted=%s",
                terminal.consumedRecords(), terminal.exhausted());
          } catch (Throwable error) {
            failure = error;
          }
        } else {
          failure = new IllegalStateException("unknown remote page frame");
        }
        protocolFailure = failure;
      }
      if (protocolFailure != null) {
        Flow.Subscription current;
        synchronized (this) {
          current = upstream;
        }
        if (current != null) {
          current.cancel();
        }
      }
      drain();
    }

    @Override
    public void onError(Throwable error) {
      fail(error);
    }

    @Override
    public void onComplete() {
      synchronized (this) {
        if (cancelled || signalled) {
          return;
        }
        remoteDone = true;
        LOG.info("event=remote_page_client_rpc_complete");
        if (terminal == null) {
          failure = new IllegalStateException("remote page ended without completion frame");
        }
      }
      drain();
    }

    private void fail(Throwable error) {
      Flow.Subscription current;
      synchronized (this) {
        if (cancelled || signalled) {
          return;
        }
        failure = compatibilityFailure(Objects.requireNonNull(error, "error must not be null"));
        current = upstream;
      }
      if (current != null) {
        current.cancel();
      }
      drain();
    }

    private void drain() {
      if (work.getAndIncrement() != 0) {
        return;
      }
      do {
        while (true) {
          T item;
          Throwable error;
          PagedSourceCompletion result;
          synchronized (this) {
            if (cancelled || signalled) {
              break;
            }
            error = failure;
            if (error != null) {
              signalled = true;
              buffered = null;
              item = null;
              result = null;
            } else if (buffered != null && demand > 0) {
              item = buffered;
              buffered = null;
              demand--;
              result = null;
            } else if (remoteDone && buffered == null) {
              signalled = true;
              item = null;
              result = terminal;
            } else {
              break;
            }
          }
          if (error != null) {
            completion.completeExceptionally(error);
            downstream.onError(error);
            break;
          }
          if (item != null) {
            try {
              downstream.onNext(item);
            } catch (Throwable callbackFailure) {
              fail(callbackFailure);
            }
          } else {
            // The RPC has closed normally. Publish its page result before invoking
            // downstream completion, whose terminal path may await that result.
            completion.complete(result);
            downstream.onComplete();
            break;
          }
        }
      } while (work.decrementAndGet() != 0);
      flushCredits();
    }

    private void flushCredits() {
      if (creditWork.getAndIncrement() != 0) {
        return;
      }
      do {
        Flow.Subscription current;
        synchronized (this) {
          if (cancelled || signalled || upstream == null || buffered != null
              || creditOutstanding || terminal != null || remoteDone) {
            current = null;
          } else {
            current = upstream;
            creditOutstanding = true;
          }
        }
        if (current != null) {
          try {
            current.request(1);
          } catch (Throwable error) {
            fail(error);
          }
        }
      } while (creditWork.decrementAndGet() != 0);
    }

    private long addCap(long left, long right) {
      long sum = left + right;
      return sum < 0 ? Long.MAX_VALUE : sum;
    }
  }

  private Throwable compatibilityFailure(Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      Status.Code code = cause instanceof StatusRuntimeException runtime
          ? runtime.getStatus().getCode()
          : cause instanceof StatusException checked ? checked.getStatus().getCode() : Status.Code.OK;
      if (code == Status.Code.UNIMPLEMENTED || code == Status.Code.FAILED_PRECONDITION) {
        return new NonRetryableException("remote paged source capability or release is incompatible", error);
      }
      if (cause instanceof WebApplicationException http && http.getResponse() != null
          && (http.getResponse().getStatus() == 404 || http.getResponse().getStatus() == 409
              || http.getResponse().getStatus() == 501)) {
        return new NonRetryableException("remote paged source capability or release is incompatible", error);
      }
    }
    return error;
  }
}
