package org.pipelineframework.orchestrator;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;
import org.pipelineframework.awaitable.admission.AwaitAdmissionAcquireResult;
import org.pipelineframework.awaitable.admission.AwaitAdmissionOwner;
import org.pipelineframework.awaitable.admission.AwaitAdmissionReservation;
import org.pipelineframework.awaitable.admission.AwaitAdmissionScope;
import org.pipelineframework.awaitable.admission.AwaitAdmissionStore;
import org.pipelineframework.config.PipelineStepConfig;

/**
 * Control-plane tenant admission and per-tenant transition quota policy.
 * Supports either in-process counting (default) or distributed fixed-slot leases backed by
 * {@link AwaitAdmissionStore} so multiple coordinator instances share one enforceable per-tenant cap.
 */
@ApplicationScoped
public class LocalControlPlaneAdmissionPolicy implements ControlPlaneAdmissionPolicy {

    private static final Logger LOG = Logger.getLogger(LocalControlPlaneAdmissionPolicy.class);
    static final String TRANSITION_QUOTA_PIPELINE = "__tpf_transition_quota__";
    static final String TRANSITION_QUOTA_STEP = "__tenant__";
    private static final String NON_SHARED_STORE_PROVIDER = "in-memory";
    private static final long DISTRIBUTED_IO_TIMEOUT_MS = 5_000L;
    private static final long DEFAULT_LEASE_MS = 180_000L;
    private static final long MIN_LEASE_MS = 2_000L;
    private static final long MAX_LEASE_SAFETY_MARGIN_MS = 10_000L;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    @Inject
    PipelineStepConfig stepConfig;

    @Inject
    Instance<AwaitAdmissionStore> admissionStores;

    private final AwaitAdmissionStore directStore;
    private final Clock clock;
    private final long distributedIoTimeoutMs;
    private final ConcurrentHashMap<String, AtomicInteger> activeTransitionsByTenant = new ConcurrentHashMap<>();

    public LocalControlPlaneAdmissionPolicy() {
        this(null, null, Clock.systemUTC(), DISTRIBUTED_IO_TIMEOUT_MS);
    }

    public LocalControlPlaneAdmissionPolicy(PipelineOrchestratorConfig orchestratorConfig) {
        this(orchestratorConfig, null, Clock.systemUTC(), DISTRIBUTED_IO_TIMEOUT_MS);
    }

    LocalControlPlaneAdmissionPolicy(
        PipelineOrchestratorConfig orchestratorConfig,
        AwaitAdmissionStore directStore,
        Clock clock
    ) {
        this(orchestratorConfig, directStore, clock, DISTRIBUTED_IO_TIMEOUT_MS);
    }

    LocalControlPlaneAdmissionPolicy(
        PipelineOrchestratorConfig orchestratorConfig,
        AwaitAdmissionStore directStore,
        Clock clock,
        long distributedIoTimeoutMs
    ) {
        this.orchestratorConfig = orchestratorConfig;
        this.directStore = directStore;
        this.clock = clock == null ? Clock.systemUTC() : clock;
        this.distributedIoTimeoutMs = Math.max(10L, distributedIoTimeoutMs);
    }

    @Override
    public ControlPlaneAdmissionDecision admit(ControlPlaneAdmissionRequest request) {
        if (request == null) {
            return ControlPlaneAdmissionDecision.deny("INVALID_ADMISSION_REQUEST", "Admission request is required");
        }
        if (requireExplicitTenant()
            && request.operation() == ControlPlaneAdmissionOperation.SUBMIT_EXECUTION
            && !request.explicitTenant()) {
            return ControlPlaneAdmissionDecision.deny(
                ControlPlaneAdmissionDecision.TENANT_REQUIRED,
                "Tenant id is required for async execution submission");
        }
        String tenantId = effectiveTenantId(request);
        List<String> allowedTenants = allowedTenants();
        if (!allowedTenants.isEmpty() && !allowedTenants.contains(tenantId)) {
            return ControlPlaneAdmissionDecision.deny(
                ControlPlaneAdmissionDecision.TENANT_NOT_ALLOWED,
                "Tenant '" + tenantId + "' is not allowed for local control-plane operations");
        }
        return ControlPlaneAdmissionDecision.allow();
    }

    @Override
    public ControlPlaneTransitionAdmission admitTransition(ControlPlaneAdmissionRequest request) {
        ControlPlaneAdmissionDecision decision = admit(request);
        if (!decision.allowed()) {
            return ControlPlaneTransitionAdmission.denied(decision);
        }
        int maxInFlight = maxInFlightTransitionsPerTenant();
        if (maxInFlight <= 0) {
            return ControlPlaneTransitionAdmission.admitted(ControlPlaneTransitionPermit.noop());
        }
        String tenantId = effectiveTenantId(request);
        if (distributedTransitionAdmission()) {
            return admitDistributedTransition(request, tenantId, maxInFlight);
        }
        AtomicInteger counter = activeTransitionsByTenant.computeIfAbsent(tenantId, ignored -> new AtomicInteger());
        while (true) {
            int current = counter.get();
            if (current >= maxInFlight) {
                return ControlPlaneTransitionAdmission.denied(ControlPlaneAdmissionDecision.deny(
                    ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED,
                    "Tenant '" + tenantId + "' has reached max in-flight transitions "
                        + maxInFlight));
            }
            if (counter.compareAndSet(current, current + 1)) {
                return ControlPlaneTransitionAdmission.admitted(() -> releasePermit(tenantId));
            }
        }
    }

    private ControlPlaneTransitionAdmission admitDistributedTransition(
        ControlPlaneAdmissionRequest request,
        String tenantId,
        int maxInFlight
    ) {
        AwaitAdmissionStore store = resolveDistributedStore();
        if (store == null) {
            LOG.warnf("Distributed transition quota enabled for tenant=%s but no matching AwaitAdmissionStore is available; failing closed",
                tenantId);
            return ControlPlaneTransitionAdmission.denied(ControlPlaneAdmissionDecision.deny(
                ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED,
                "Distributed transition admission store is unavailable for tenant '" + tenantId + "'"));
        }
        String scopeTenant = tenantId.isEmpty() ? "default" : tenantId;
        AwaitAdmissionScope scope = new AwaitAdmissionScope(TRANSITION_QUOTA_PIPELINE, TRANSITION_QUOTA_STEP, scopeTenant);
        String executionId = request.executionId() == null ? "" : request.executionId();
        AwaitAdmissionOwner owner = new AwaitAdmissionOwner(
            AwaitAdmissionScope.lengthPrefixedKey("transition", executionId, UUID.randomUUID().toString()));
        long now = clock.millis();
        long leaseDurationMs = transitionLeaseDurationMs();
        long expiresAt = now + leaseDurationMs;
        CompletableFuture<AwaitAdmissionAcquireResult> acquireFuture =
            store.acquire(scope, owner, maxInFlight, expiresAt, now).toCompletableFuture();
        AtomicBoolean abandoned = new AtomicBoolean(false);
        AtomicBoolean orphanReleased = new AtomicBoolean(false);
        acquireFuture.whenComplete((lateResult, lateFailure) -> {
            if (abandoned.get() && lateFailure == null) {
                releaseLateReservation(store, lateResult, orphanReleased, scopeTenant);
            }
        });
        final AwaitAdmissionAcquireResult acquired;
        try {
            acquired = acquireFuture.get(distributedIoTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception failure) {
            abandoned.set(true);
            if (acquireFuture.isDone() && !acquireFuture.isCompletedExceptionally()) {
                releaseLateReservation(store, acquireFuture.getNow(null), orphanReleased, scopeTenant);
            }
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.warnf(failure, "Failed to acquire distributed transition quota slot for tenant=%s; deferring work", scopeTenant);
            return ControlPlaneTransitionAdmission.denied(ControlPlaneAdmissionDecision.deny(
                ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED,
                "Distributed transition admission temporarily unavailable for tenant '" + scopeTenant + "'"));
        }
        if (acquired == null || !acquired.acquired() || acquired.reservation().isEmpty()) {
            return ControlPlaneTransitionAdmission.denied(ControlPlaneAdmissionDecision.deny(
                ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED,
                "Tenant '" + scopeTenant + "' has reached max in-flight transitions " + maxInFlight));
        }
        AwaitAdmissionReservation reservation = acquired.reservation().get();
        Duration maxActiveDuration = Duration.ofMillis(maxActiveDurationMs(leaseDurationMs));
        AtomicBoolean released = new AtomicBoolean(false);
        return ControlPlaneTransitionAdmission.admitted(new ControlPlaneTransitionPermit() {
            @Override
            public void close() {
                if (released.compareAndSet(false, true)) {
                    try {
                        store.release(reservation)
                            .toCompletableFuture()
                            .get(distributedIoTimeoutMs, TimeUnit.MILLISECONDS);
                    } catch (Exception failure) {
                        if (failure instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        LOG.warnf(failure, "Failed to release distributed transition quota slot=%d for tenant=%s; lease will expire",
                            reservation.slot(), scopeTenant);
                    }
                }
            }

            @Override
            public Optional<Duration> maxActiveDuration() {
                return Optional.of(maxActiveDuration);
            }
        });
    }

    private void releaseLateReservation(
        AwaitAdmissionStore store,
        AwaitAdmissionAcquireResult lateResult,
        AtomicBoolean orphanReleased,
        String scopeTenant
    ) {
        if (lateResult == null || !lateResult.acquired() || lateResult.reservation().isEmpty()) {
            return;
        }
        if (!orphanReleased.compareAndSet(false, true)) {
            return;
        }
        AwaitAdmissionReservation orphan = lateResult.reservation().get();
        store.release(orphan).whenComplete((ignored, releaseFailure) -> {
            if (releaseFailure != null) {
                LOG.warnf(releaseFailure, "Failed to release late distributed transition quota slot=%d for tenant=%s; lease will expire",
                    orphan.slot(), scopeTenant);
            }
        });
    }

    private AwaitAdmissionStore resolveDistributedStore() {
        if (directStore != null) {
            return directStore;
        }
        if (admissionStores == null) {
            return null;
        }
        String desired = configuredTransitionAdmissionStore();
        if (NON_SHARED_STORE_PROVIDER.equals(desired)) {
            LOG.warn("Distributed transition quota requires a shared AwaitAdmissionStore; rejecting non-shared 'in-memory' store");
            return null;
        }
        return admissionStores.stream()
            .filter(candidate -> desired.equals(candidate.providerName().toLowerCase(Locale.ROOT)))
            .filter(candidate -> !NON_SHARED_STORE_PROVIDER.equals(candidate.providerName().toLowerCase(Locale.ROOT)))
            .findFirst()
            .orElse(null);
    }

    private String configuredTransitionAdmissionStore() {
        PipelineOrchestratorConfig.QuotaConfig quotas = orchestratorConfig == null ? null : orchestratorConfig.quotas();
        if (quotas != null && quotas.transitionAdmissionStore() != null
            && quotas.transitionAdmissionStore().isPresent()
            && !quotas.transitionAdmissionStore().get().isBlank()) {
            return quotas.transitionAdmissionStore().get().trim().toLowerCase(Locale.ROOT);
        }
        if (stepConfig != null && stepConfig.awaitAdmission() != null
            && stepConfig.awaitAdmission().store() != null && !stepConfig.awaitAdmission().store().isBlank()) {
            return stepConfig.awaitAdmission().store().trim().toLowerCase(Locale.ROOT);
        }
        return "dynamo";
    }

    private boolean distributedTransitionAdmission() {
        PipelineOrchestratorConfig.QuotaConfig quotas = orchestratorConfig == null ? null : orchestratorConfig.quotas();
        return quotas != null && quotas.distributedTransitionAdmission();
    }

    private long transitionLeaseDurationMs() {
        PipelineOrchestratorConfig.QuotaConfig quotas = orchestratorConfig == null ? null : orchestratorConfig.quotas();
        Duration configured = quotas == null ? null : quotas.transitionLeaseDuration();
        if (configured == null || configured.isZero() || configured.isNegative()) {
            return DEFAULT_LEASE_MS;
        }
        return Math.max(MIN_LEASE_MS, configured.toMillis());
    }

    private static long maxActiveDurationMs(long leaseDurationMs) {
        long safetyMargin = Math.min(MAX_LEASE_SAFETY_MARGIN_MS, Math.max(500L, leaseDurationMs / 5L));
        return Math.max(500L, leaseDurationMs - safetyMargin);
    }

    private void releasePermit(String tenantId) {
        activeTransitionsByTenant.computeIfPresent(tenantId, (ignored, activeCounter) -> {
            int remaining = activeCounter.updateAndGet(value -> Math.max(0, value - 1));
            return remaining == 0 ? null : activeCounter;
        });
    }

    private String effectiveTenantId(ControlPlaneAdmissionRequest request) {
        String tenantId = request == null ? null : request.tenantId();
        return tenantId == null ? "" : tenantId.trim();
    }

    private boolean requireExplicitTenant() {
        PipelineOrchestratorConfig.TenancyConfig tenancy = orchestratorConfig == null ? null : orchestratorConfig.tenancy();
        return tenancy != null && tenancy.requireExplicitTenant();
    }

    private List<String> allowedTenants() {
        PipelineOrchestratorConfig.TenancyConfig tenancy = orchestratorConfig == null ? null : orchestratorConfig.tenancy();
        if (tenancy == null || tenancy.allowedTenants() == null) {
            return List.of();
        }
        return tenancy.allowedTenants().stream()
            .filter(value -> value != null && !value.isBlank())
            .map(String::trim)
            .toList();
    }

    private int maxInFlightTransitionsPerTenant() {
        PipelineOrchestratorConfig.QuotaConfig quotas = orchestratorConfig == null ? null : orchestratorConfig.quotas();
        return quotas == null ? 0 : Math.max(0, quotas.maxInFlightTransitionsPerTenant());
    }
}
