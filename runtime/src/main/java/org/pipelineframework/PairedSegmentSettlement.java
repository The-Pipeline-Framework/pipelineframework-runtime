package org.pipelineframework;

import java.util.function.Function;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.PairedTransitionSettlementStore;
import org.pipelineframework.orchestrator.PairedTransitionSettlementStore.Lease;
import org.pipelineframework.orchestrator.TransitionResultEnvelope.PairedResult;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

/** Settles an explicitly paired worker result under the retained attempt's execution lease. */
final class PairedSegmentSettlement {

  private final ExecutionStateStore executionStateStore;
  private final ExecutionFailureHandler executionFailureHandler;
  private final DeadLetterPublisher deadLetterPublisher;
  private final TerminalPublicationBoundary terminalPublicationBoundary;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;

  PairedSegmentSettlement(
      ExecutionStateStore executionStateStore,
      ExecutionFailureHandler executionFailureHandler,
      DeadLetterPublisher deadLetterPublisher,
      TerminalPublicationBoundary terminalPublicationBoundary,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger) {
    this.executionStateStore = executionStateStore;
    this.executionFailureHandler = executionFailureHandler;
    this.deadLetterPublisher = deadLetterPublisher;
    this.terminalPublicationBoundary = terminalPublicationBoundary;
    this.segmentBoundaryLedger = segmentBoundaryLedger;
  }

  Uni<Void> complete(CompletedSegment completed, PairedResult paired) {
    ClaimedSegment segment = completed.segment();
    return withPairedLease(segment, paired, lease -> {
      PairedTransitionSettlementStore store = pairedStore();
      if (terminalPublicationBoundary.publicationConfigured()) {
        return executionFailureHandler.rejectPairedTerminalPublication(
            segment.record(), lease, store, deadLetterPublisher);
      }
      return store.settlePairedSuccess(lease, completed.result().outputPayloads().getFirst())
          .chain(updated -> updated
              .map(succeeded -> segmentBoundaryLedger.get()
                  .recordSegmentCompleted(
                      segment.record(), segment.transitionKey(), completed.result(), succeeded.updatedAtEpochMs())
                  .chain(() -> segmentBoundaryLedger.get()
                      .recordRunSucceeded(succeeded, completed.outputItems(), succeeded.updatedAtEpochMs())))
              .orElseGet(() -> Uni.createFrom().voidItem()));
    });
  }

  Uni<Void> fail(PairedFailedSegment failed) {
    return withPairedLease(failed.segment(), failed.result(),
        lease -> executionFailureHandler.handlePortableFailure(failed.segment().record(), lease,
            failed.result().failure().orElseThrow(), pairedStore(), deadLetterPublisher));
  }

  private Uni<Void> withPairedLease(ClaimedSegment segment, PairedResult paired, Function<Lease, Uni<Void>> settle) {
    var command = paired.attempt().command();
    var claimed = segment.record();
    if (!command.tenantId().equals(claimed.tenantId()) || !command.executionId().equals(claimed.executionId())
        || !command.pipelineId().equals(claimed.pipelineId()) || !command.contractVersion().equals(claimed.contractVersion())
        || !command.releaseVersion().equals(claimed.releaseVersion()) || command.executionVersion() != claimed.version()
        || command.attempt() != claimed.attempt() || !command.transitionKey().equals(segment.transitionKey())
        || command.currentStepIndex() != claimed.currentStepIndex() || command.resultShape() != claimed.resultShape()
        || claimed.leaseOwner() == null) {
      return Uni.createFrom().voidItem();
    }
    return settle.apply(new Lease(claimed.tenantId(), claimed.executionId(), claimed.version(),
        claimed.leaseOwner(), segment.transitionKey()));
  }

  private PairedTransitionSettlementStore pairedStore() {
    if (executionStateStore instanceof PairedTransitionSettlementStore paired) {
      return paired;
    }
    throw new UnsupportedOperationException("Execution state store cannot settle an explicitly paired native attempt");
  }
}
