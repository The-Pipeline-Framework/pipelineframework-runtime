package org.pipelineframework.aws.durable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.orchestrator.AwaitCompletionObservation;
import org.pipelineframework.orchestrator.AwaitCompletionObservationPage;

/** Mechanical observations only; these counts do not authorise semantic state transitions. */
final class AwsDurableHostTelemetry {
    enum Stage { WAKEUP, REPLACEMENT, REPAIR, BINDING, REGISTRATION, GENERATION, COMPLETION, CALLBACK_RESUME }
    enum Outcome {
        CHECKPOINT_MISSING, SEMANTIC_NOT_COMPLETED, BINDING_MISSING, GENERATION_UNCONFIRMED,
        STALE, ALREADY_DELIVERED, CALLBACK_ACCEPTED, PROVIDER_SUCCEEDED, PROVIDER_CLOSED,
        PROVIDER_OPEN, PROVIDER_UNKNOWN, TRANSIENT_FAILURE, STARTED, REJECTED,
        IDENTITY_MISMATCH, PROVIDER_ACTIVE, SEMANTIC_TERMINAL, FAILED, CREATED, REPLAYED, CONFLICT, STALE_REJECTED,
        OUTSTANDING, RELEASED, TERMINAL_FAILED, UNKNOWN, BOUNDED_PAGE, MEASURED,
        REPAIR_AGE_SAMPLED, REPAIR_AGE_UNKNOWN
    }

    private final Clock clock;
    private final Consumer<String> sink;
    private final ObjectMapper mapper = new ObjectMapper();

    AwsDurableHostTelemetry() { this(Clock.systemUTC(), System.out::println); }

    AwsDurableHostTelemetry(Clock clock, Consumer<String> sink) {
        this.clock = Objects.requireNonNull(clock);
        this.sink = Objects.requireNonNull(sink);
    }

    void record(Stage stage, Outcome outcome, String correlation, long generation) {
        try {
            var fields = new java.util.LinkedHashMap<String, Object>();
            fields.put("HostOutcomeCount", 1);
            fields.put("Generation", generation);
            emit(stage, outcome, correlation, fields, List.of(metric("HostOutcomeCount", "Count")));
        } catch (RuntimeException unavailable) {
            // An observation failure must never change acknowledgement, fencing or recovery behavior.
        }
    }

    void recordCompletion(AwaitCompletionObservation observation) {
        Objects.requireNonNull(observation, "observation");
        try {
            var fields = new java.util.LinkedHashMap<String, Object>();
            fields.put("CompletionObservationCount", 1);
            var metrics = new java.util.ArrayList<Map<String, String>>();
            metrics.add(metric("CompletionObservationCount", "Count"));
            if (observation.admissionToReleaseLatencyMs().isPresent()) {
                fields.put("AdmissionToReleaseLatencyMilliseconds", observation.admissionToReleaseLatencyMs().getAsLong());
                metrics.add(metric("AdmissionToReleaseLatencyMilliseconds", "Milliseconds"));
            }
            emit(Stage.COMPLETION, Outcome.valueOf(observation.state().name()),
                observation.tenantId() + "\u0000" + observation.interactionId(), fields, metrics);
        } catch (RuntimeException unavailable) {
            // Observability cannot become an authority or failure path for a semantic read.
        }
    }

    void recordCompletionPage(String tenantId, AwaitCompletionObservationPage page) {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(page, "page");
        try {
            page.observations().forEach(this::recordCompletion);
            var fields = new java.util.LinkedHashMap<String, Object>();
            fields.put("CompletionObservationPageCount", 1);
            fields.put("CompletionObservationEvaluatedEntries", page.evaluatedEntryCount());
            fields.put("CompletionObservationReturnedEntries", page.observations().size());
            fields.put("CompletionObservationHasMorePages", page.nextCursor().isPresent() ? 1 : 0);
            fields.put("Coverage", "BOUNDED_PAGE_ONLY_NOT_GLOBAL_INVENTORY");
            emit(Stage.COMPLETION, Outcome.BOUNDED_PAGE, tenantId + "\u0000completion-page", fields, List.of(
                metric("CompletionObservationPageCount", "Count"),
                metric("CompletionObservationEvaluatedEntries", "Count"),
                metric("CompletionObservationReturnedEntries", "Count"),
                metric("CompletionObservationHasMorePages", "Count")));
        } catch (RuntimeException unavailable) {
            // A failed or partial telemetry write cannot change the bounded query result.
        }
    }

    /** The sample starts at the host's normal callback API return observation, not provider event time. */
    void recordCallbackResume(String correlation, long generation, java.util.OptionalLong latencyMillis) {
        try {
            var fields = new java.util.LinkedHashMap<String, Object>();
            fields.put("Generation", generation);
            var metrics = new java.util.ArrayList<Map<String, String>>();
            if (latencyMillis.isPresent()) {
                fields.put("CallbackToFirstResumedActionMilliseconds", latencyMillis.getAsLong());
                metrics.add(metric("CallbackToFirstResumedActionMilliseconds", "Milliseconds"));
                emit(Stage.CALLBACK_RESUME, Outcome.MEASURED, correlation, fields, metrics);
            } else {
                fields.put("Latency", "UNKNOWN_MISSING_OR_INVALID_CALLBACK_TIMESTAMP");
                fields.put("CallbackResumeUnknownCount", 1);
                metrics.add(metric("CallbackResumeUnknownCount", "Count"));
                emit(Stage.CALLBACK_RESUME, Outcome.UNKNOWN, correlation, fields, metrics);
            }
        } catch (RuntimeException unavailable) {
            // Telemetry cannot participate in durable callback or resume authority.
        }
    }

    /** Emits age sampled for one targeted repair attempt; this is not an outstanding-repair inventory. */
    void recordRepairAge(String providerExecutionArn, java.util.OptionalLong firstObservedAtEpochMs) {
        try {
            long now = clock.millis();
            boolean measured = firstObservedAtEpochMs != null && firstObservedAtEpochMs.isPresent()
                && firstObservedAtEpochMs.getAsLong() >= 0 && now >= firstObservedAtEpochMs.getAsLong();
            var fields = new java.util.LinkedHashMap<String, Object>();
            var metrics = new java.util.ArrayList<Map<String, String>>();
            fields.put("RepairAgeCoverage", "SAMPLED_PER_TARGETED_REPAIR_ATTEMPT_NOT_GLOBAL_INVENTORY");
            if (measured) {
                fields.put("RepairAgeMilliseconds", now - firstObservedAtEpochMs.getAsLong());
                fields.put("RepairAgeSampleCount", 1);
                metrics.add(metric("RepairAgeMilliseconds", "Milliseconds"));
                metrics.add(metric("RepairAgeSampleCount", "Count"));
            } else {
                fields.put("RepairAgeUnknownReason", "MISSING_INVALID_OR_NEGATIVE_TIMESTAMP");
                fields.put("RepairAgeUnknownCount", 1);
                metrics.add(metric("RepairAgeUnknownCount", "Count"));
            }
            String correlation = providerExecutionArn == null || providerExecutionArn.isBlank()
                ? "unknown-repair" : providerExecutionArn;
            emit(Stage.REPAIR, measured ? Outcome.REPAIR_AGE_SAMPLED : Outcome.REPAIR_AGE_UNKNOWN,
                correlation, fields, metrics);
        } catch (RuntimeException unavailable) {
            // Repair-age telemetry cannot delay or change targeted semantic recovery.
        }
    }

    private void emit(Stage stage, Outcome outcome, String correlation,
        Map<String, Object> fields, List<Map<String, String>> metrics) {
        try {
            var metric = Map.of("Namespace", "TPF/AwsDurable",
                "Dimensions", List.of(List.of("Stage", "Outcome")), "Metrics", metrics);
            var event = new java.util.LinkedHashMap<String, Object>();
            event.put("_aws", Map.of("Timestamp", clock.millis(), "CloudWatchMetrics", List.of(metric)));
            event.put("Stage", stage.name());
            event.put("Outcome", outcome.name());
            event.put("CorrelationHash", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(correlation.getBytes(StandardCharsets.UTF_8))));
            event.putAll(fields);
            sink.accept(mapper.writeValueAsString(event));
        } catch (JsonProcessingException | NoSuchAlgorithmException | RuntimeException unavailable) {
            // Telemetry is best effort and never participates in control-plane authority.
        }
    }

    private static Map<String, String> metric(String name, String unit) {
        return Map.of("Name", name, "Unit", unit);
    }
}
