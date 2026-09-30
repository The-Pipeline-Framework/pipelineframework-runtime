package org.pipelineframework.awsproof;

import java.util.List;

final class ProofFaultScenarioCatalog {
    enum Lane { QUICK, EXTENDED }

    record Scenario(int id, String name, Lane lane, String evidenceGroup) {
        Scenario {
            if (id < 1 || id > 22) {
                throw new IllegalArgumentException("scenario id must be between 1 and 22");
            }
            if (name == null || name.isBlank() || evidenceGroup == null || evidenceGroup.isBlank()) {
                throw new IllegalArgumentException("scenario metadata must not be blank");
            }
        }
    }

    private ProofFaultScenarioCatalog() {
    }

    static List<Scenario> all() {
        return List.of(
            new Scenario(1, "lost ingress response after AWS accepted the start", Lane.QUICK, "idempotent-ingress"),
            new Scenario(2, "Durable replay before TPF submit", Lane.QUICK, "idempotent-ingress"),
            new Scenario(3, "TPF submit committed before Durable checkpoint", Lane.QUICK, "idempotent-ingress"),
            new Scenario(4, "work Lambda failure before action admission", Lane.QUICK, "worker-redelivery"),
            new Scenario(5, "work action committed before SQS acknowledgement", Lane.QUICK, "worker-redelivery"),
            new Scenario(6, "transition response published before acknowledgement", Lane.QUICK, "transition-redelivery"),
            new Scenario(7, "duplicate transition delivery preserves uncertain-outcome semantics", Lane.QUICK,
                "transition-redelivery"),
            new Scenario(8, "callback submitter failure before binding", Lane.QUICK, "binding-races"),
            new Scenario(9, "binding committed before submitter checkpoint", Lane.QUICK, "binding-races"),
            new Scenario(10, "Await completion before callback binding", Lane.QUICK, "binding-races"),
            new Scenario(11, "Await completion after callback binding", Lane.QUICK, "stream-reconciliation"),
            new Scenario(12, "duplicate DynamoDB Stream delivery", Lane.QUICK, "partial-stream-batch"),
            new Scenario(13, "callback accepted before API response loss", Lane.QUICK, "callback-uncertainty"),
            new Scenario(14, "closed callback reconstructs as the next generation", Lane.QUICK,
                "callback-expiry-recovery"),
            new Scenario(15, "old and replacement driver generations race", Lane.QUICK, "generation-fence"),
            new Scenario(16, "stopped Durable execution reconstructs from TPF state", Lane.QUICK, "closed-callback"),
            new Scenario(17, "alias rollout preserves a parked numbered version", Lane.QUICK, "version-survival"),
            new Scenario(18, "stream failure reaches retained failure records", Lane.QUICK,
                "stream-retained-failure"),
            new Scenario(19, "missed stream record is recovered by reconciliation", Lane.QUICK,
                "stream-reconciliation"),
            new Scenario(20, "Durable callback API throttling is retried", Lane.QUICK, "callback-uncertainty"),
            new Scenario(21, "TPF retry exhaustion retains DLQ and re-drive authority", Lane.QUICK,
                "tpf-retry-redrive"),
            new Scenario(22, "AWS schedules Await deadline and TPF admits timeout", Lane.QUICK,
                "await-deadline"));
    }
}
