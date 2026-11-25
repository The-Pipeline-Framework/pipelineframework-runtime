package org.pipelineframework.orchestrator;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.admission.AwaitAdmissionAcquireResult;
import org.pipelineframework.awaitable.admission.AwaitAdmissionOwner;
import org.pipelineframework.awaitable.admission.AwaitAdmissionReservation;
import org.pipelineframework.awaitable.admission.AwaitAdmissionScope;
import org.pipelineframework.awaitable.admission.AwaitAdmissionStore;
import org.pipelineframework.awaitable.admission.InMemoryAwaitAdmissionStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ControlPlaneAdmissionPolicyTest {

    @Test
    void defaultPolicyAllowsAllTenantsAndDoesNotRequireExplicitTenant() {
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy();

        ControlPlaneAdmissionDecision decision = policy.admit(request("tenant-a", false));

        assertTrue(decision.allowed());
    }

    @Test
    void allowedTenantListRejectsUnknownTenant() {
        PipelineOrchestratorConfig config = config(List.of("tenant-a", "tenant-b"), false, 0);
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy(config);

        ControlPlaneAdmissionDecision allowed = policy.admit(request("tenant-a", true));
        ControlPlaneAdmissionDecision denied = policy.admit(request("tenant-c", true));

        assertTrue(allowed.allowed());
        assertFalse(denied.allowed());
        assertEquals(ControlPlaneAdmissionDecision.TENANT_NOT_ALLOWED, denied.errorCode());
    }

    @Test
    void explicitTenantRequirementAppliesToSubmission() {
        PipelineOrchestratorConfig config = config(List.of(), true, 0);
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy(config);

        ControlPlaneAdmissionDecision decision = policy.admit(request("default", false));

        assertFalse(decision.allowed());
        assertEquals(ControlPlaneAdmissionDecision.TENANT_REQUIRED, decision.errorCode());
    }

    @Test
    void perTenantTransitionQuotaSaturatesAndReleases() {
        PipelineOrchestratorConfig config = config(List.of(), false, 1);
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy(config);
        ControlPlaneAdmissionRequest request = transitionRequest("tenant-a");

        ControlPlaneTransitionAdmission first = policy.admitTransition(request);
        ControlPlaneTransitionAdmission saturated = policy.admitTransition(request);
        first.permit().close();
        ControlPlaneTransitionAdmission afterRelease = policy.admitTransition(request);
        afterRelease.permit().close();

        assertTrue(first.decision().allowed());
        assertFalse(saturated.decision().allowed());
        assertEquals(ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED, saturated.decision().errorCode());
        assertTrue(afterRelease.decision().allowed());
    }

    @Test
    void admissionDecisionRejectsAllowedErrorDetails() {
        assertThrows(IllegalArgumentException.class,
            () -> new ControlPlaneAdmissionDecision(true, "ERROR", "reason"));
    }

    @Test
    void deniedTransitionAdmissionRequiresDeniedDecision() {
        assertThrows(IllegalArgumentException.class,
            () -> ControlPlaneTransitionAdmission.denied(ControlPlaneAdmissionDecision.allow()));
    }

    @Test
    void admissionExceptionRequiresDeniedDecision() {
        assertThrows(IllegalArgumentException.class,
            () -> new ControlPlaneAdmissionException(ControlPlaneAdmissionDecision.allow()));
    }

    @Test
    void distributedQuotaEnforcesCombinedLimitAcrossIndependentInstancesWithoutStarvingOtherTenants() {
        InMemoryAwaitAdmissionStore sharedStore = new InMemoryAwaitAdmissionStore();
        Clock fixedClock = Clock.fixed(Instant.parse("2026-11-25T20:00:00Z"), ZoneOffset.UTC);
        PipelineOrchestratorConfig config = distributedConfig(2, Duration.ofSeconds(60));
        LocalControlPlaneAdmissionPolicy instanceOne = new LocalControlPlaneAdmissionPolicy(config, sharedStore, fixedClock);
        LocalControlPlaneAdmissionPolicy instanceTwo = new LocalControlPlaneAdmissionPolicy(config, sharedStore, fixedClock);

        ControlPlaneTransitionAdmission a1 = instanceOne.admitTransition(transitionRequest("tenant-a", "exec-a1"));
        ControlPlaneTransitionAdmission a2 = instanceTwo.admitTransition(transitionRequest("tenant-a", "exec-a2"));
        ControlPlaneTransitionAdmission a3Denied = instanceOne.admitTransition(transitionRequest("tenant-a", "exec-a3"));
        ControlPlaneTransitionAdmission b1 = instanceTwo.admitTransition(transitionRequest("tenant-b", "exec-b1"));
        ControlPlaneTransitionAdmission b2 = instanceOne.admitTransition(transitionRequest("tenant-b", "exec-b2"));

        assertTrue(a1.decision().allowed());
        assertTrue(a2.decision().allowed());
        assertFalse(a3Denied.decision().allowed());
        assertEquals(ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED, a3Denied.decision().errorCode());
        assertTrue(b1.decision().allowed());
        assertTrue(b2.decision().allowed());

        // Closing a1 twice must be idempotent and only free one slot for tenant-a.
        a1.permit().close();
        a1.permit().close();

        ControlPlaneTransitionAdmission a3Retry = instanceTwo.admitTransition(transitionRequest("tenant-a", "exec-a3"));
        ControlPlaneTransitionAdmission a4Denied = instanceOne.admitTransition(transitionRequest("tenant-a", "exec-a4"));
        assertTrue(a3Retry.decision().allowed());
        assertFalse(a4Denied.decision().allowed());

        a2.permit().close();
        a3Retry.permit().close();
        b1.permit().close();
        b2.permit().close();
    }

    @Test
    void distributedQuotaReclaimsExpiredSlotsAfterInstanceCrash() {
        InMemoryAwaitAdmissionStore sharedStore = new InMemoryAwaitAdmissionStore();
        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-11-25T20:00:00Z"));
        Clock mutableClock = new Clock() {
            @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return now.get(); }
        };
        PipelineOrchestratorConfig config = distributedConfig(1, Duration.ofSeconds(30));
        LocalControlPlaneAdmissionPolicy crashedInstance = new LocalControlPlaneAdmissionPolicy(config, sharedStore, mutableClock);
        LocalControlPlaneAdmissionPolicy healthyInstance = new LocalControlPlaneAdmissionPolicy(config, sharedStore, mutableClock);

        ControlPlaneTransitionAdmission leaked = crashedInstance.admitTransition(transitionRequest("tenant-a", "exec-crash"));
        assertTrue(leaked.decision().allowed());
        // Simulate crash: permit is never closed.
        assertFalse(healthyInstance.admitTransition(transitionRequest("tenant-a", "exec-next")).decision().allowed());

        now.set(now.get().plusSeconds(31));
        ControlPlaneTransitionAdmission reclaimed = healthyInstance.admitTransition(transitionRequest("tenant-a", "exec-next"));
        assertTrue(reclaimed.decision().allowed());
        reclaimed.permit().close();
    }

    @Test
    void distributedQuotaFailsClosedWhenStoreIsMissingOrFails() {
        PipelineOrchestratorConfig config = distributedConfig(2, Duration.ofSeconds(30));
        LocalControlPlaneAdmissionPolicy missingStore = new LocalControlPlaneAdmissionPolicy(config, null, Clock.systemUTC());
        ControlPlaneTransitionAdmission deniedMissing = missingStore.admitTransition(transitionRequest("tenant-a", "exec-1"));
        assertFalse(deniedMissing.decision().allowed());
        assertEquals(ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED, deniedMissing.decision().errorCode());

        AwaitAdmissionStore failingStore = mock(AwaitAdmissionStore.class);
        when(failingStore.acquire(any(), any(), anyInt(), anyLong(), anyLong()))
            .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("dynamo throttled")));
        LocalControlPlaneAdmissionPolicy failingPolicy = new LocalControlPlaneAdmissionPolicy(config, failingStore, Clock.systemUTC());
        ControlPlaneTransitionAdmission deniedFailure = failingPolicy.admitTransition(transitionRequest("tenant-a", "exec-2"));
        assertFalse(deniedFailure.decision().allowed());
        assertEquals(ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED, deniedFailure.decision().errorCode());
    }

    @Test
    @SuppressWarnings("unchecked")
    void distributedQuotaRejectsNonSharedInMemoryProviderLookup() {
        PipelineOrchestratorConfig config = distributedConfig(2, Duration.ofSeconds(30));
        when(config.quotas().transitionAdmissionStore()).thenReturn(Optional.of("in-memory"));
        Instance<AwaitAdmissionStore> stores = mock(Instance.class);
        when(stores.stream()).thenAnswer(invocation -> Stream.of(new InMemoryAwaitAdmissionStore()));
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy(config);
        policy.admissionStores = stores;

        ControlPlaneTransitionAdmission denied = policy.admitTransition(transitionRequest("tenant-a", "exec-mem"));
        assertFalse(denied.decision().allowed());
        assertEquals(ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED, denied.decision().errorCode());
    }

    @Test
    void distributedQuotaBoundsActiveDurationStrictlyBelowLeaseAndReleasesLateAcquireAfterTimeout() {
        InMemoryAwaitAdmissionStore sharedStore = new InMemoryAwaitAdmissionStore();
        PipelineOrchestratorConfig config = distributedConfig(1, Duration.ofSeconds(30));
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy(config, sharedStore, Clock.systemUTC());
        ControlPlaneTransitionAdmission admitted = policy.admitTransition(transitionRequest("tenant-a", "exec-bound"));
        assertTrue(admitted.decision().allowed());
        assertTrue(admitted.permit().maxActiveDuration().isPresent());
        assertTrue(admitted.permit().maxActiveDuration().get().compareTo(Duration.ofSeconds(30)) < 0);
        admitted.permit().close();

        CompletableFuture<AwaitAdmissionAcquireResult> delayedAcquire = new CompletableFuture<>();
        AwaitAdmissionStore slowStore = mock(AwaitAdmissionStore.class);
        when(slowStore.acquire(any(), any(), anyInt(), anyLong(), anyLong())).thenReturn(delayedAcquire);
        when(slowStore.release(any())).thenReturn(CompletableFuture.completedFuture(true));

        LocalControlPlaneAdmissionPolicy timeoutPolicy =
            new LocalControlPlaneAdmissionPolicy(config, slowStore, Clock.systemUTC(), 25L);
        ControlPlaneTransitionAdmission timedOut = timeoutPolicy.admitTransition(transitionRequest("tenant-a", "exec-timeout"));
        assertFalse(timedOut.decision().allowed());

        AwaitAdmissionReservation lateReservation = new AwaitAdmissionReservation(
            new AwaitAdmissionScope(LocalControlPlaneAdmissionPolicy.TRANSITION_QUOTA_PIPELINE,
                LocalControlPlaneAdmissionPolicy.TRANSITION_QUOTA_STEP, "tenant-a"),
            new AwaitAdmissionOwner("late-owner"),
            0,
            System.currentTimeMillis() + 30_000L);
        delayedAcquire.complete(AwaitAdmissionAcquireResult.acquired(lateReservation));
        verify(slowStore).release(lateReservation);
    }

    private static ControlPlaneAdmissionRequest request(String tenantId, boolean explicitTenant) {
        return new ControlPlaneAdmissionRequest(
            tenantId,
            ControlPlaneAdmissionOperation.SUBMIT_EXECUTION,
            "pipeline-1",
            "bundle-1",
            null,
            "api",
            explicitTenant);
    }

    private static ControlPlaneAdmissionRequest transitionRequest(String tenantId) {
        return transitionRequest(tenantId, "exec-1");
    }

    private static ControlPlaneAdmissionRequest transitionRequest(String tenantId, String executionId) {
        return new ControlPlaneAdmissionRequest(
            tenantId,
            ControlPlaneAdmissionOperation.PROCESS_WORK_ITEM,
            "pipeline-1",
            "bundle-1",
            executionId,
            "worker-dispatch",
            true);
    }

    private static PipelineOrchestratorConfig distributedConfig(int maxInFlight, Duration leaseDuration) {
        PipelineOrchestratorConfig cfg = config(List.of(), false, maxInFlight);
        when(cfg.quotas().distributedTransitionAdmission()).thenReturn(true);
        when(cfg.quotas().transitionLeaseDuration()).thenReturn(leaseDuration);
        when(cfg.quotas().transitionAdmissionStore()).thenReturn(Optional.empty());
        return cfg;
    }

    private static PipelineOrchestratorConfig config(
        List<String> allowedTenants,
        boolean requireExplicitTenant,
        int maxInFlightTransitionsPerTenant) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.TenancyConfig tenancy = mock(PipelineOrchestratorConfig.TenancyConfig.class);
        PipelineOrchestratorConfig.QuotaConfig quotas = mock(PipelineOrchestratorConfig.QuotaConfig.class);
        when(config.tenancy()).thenReturn(tenancy);
        when(config.quotas()).thenReturn(quotas);
        when(tenancy.allowedTenants()).thenReturn(allowedTenants);
        when(tenancy.requireExplicitTenant()).thenReturn(requireExplicitTenant);
        when(quotas.maxInFlightTransitionsPerTenant()).thenReturn(maxInFlightTransitionsPerTenant);
        return config;
    }
}
