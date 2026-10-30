package com.example.grpcclient;

import com.example.grpcclient.domain.FinalReply;
import com.example.grpcclient.domain.Input;
import com.example.grpcclient.domain.Prepared;
import com.example.grpcclient.domain.RemoteReply;
import com.example.grpcclient.grpc.ProcessRemoteServiceGrpc;
import com.example.grpcclient.service.PrepareService;
import com.example.grpcpeer.PeerApplication;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.core.PipelineUnaryStep;
import org.pipelineframework.runtime.core.TpfExecutionContext;
import org.pipelineframework.runtime.spring.SpringPipelineRunner;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.*;

class GeneratedSpringGrpcClientIT {
    private static final String TARGET = "spring.grpc.client.channel.process-remote.target";
    private static ConfigurableApplicationContext peer;
    private static ConfigurableApplicationContext caller;

    @BeforeAll
    static void startApplications() {
        peer = new SpringApplicationBuilder(PeerApplication.class).web(WebApplicationType.NONE)
            .properties("spring.grpc.server.port=0", "spring.main.banner-mode=off").run();
        int port = peer.getEnvironment().getRequiredProperty("local.grpc.server.port", Integer.class);
        caller = caller(TARGET + "=static://localhost:" + port).run();
    }

    private static SpringApplicationBuilder caller(String... properties) {
        return new SpringApplicationBuilder(CallerApplication.class, Deadlines.class).web(WebApplicationType.NONE)
            .properties("spring.grpc.server.enabled=false", "spring.main.banner-mode=off")
            .properties(properties);
    }

    @AfterAll
    static void stopApplications() {
        if (caller != null) caller.close();
        if (peer != null) peer.close();
    }

    @Test
    void runsMixedPipelineAndPropagatesContextAcrossAsyncLocalStep() throws Exception {
        var preparation = caller.getBean(PrepareService.class);
        preparation.hold();
        CompletableFuture<Object> first;
        CompletableFuture<Object> second;
        try {
            first = run("one", "v1", "r1", "c1");
            second = run("two", "v2", "r2", "c2");
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            assertTrue(TpfExecutionContext.versionTag().isEmpty());
        } finally {
            preparation.release().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        assertEquals(new FinalReply("wire:one|prepared|remote|mapped|final"), first.get(5, TimeUnit.SECONDS));
        assertEquals(new FinalReply("wire:two|prepared|remote|mapped|final"), second.get(5, TimeUnit.SECONDS));
        var seen = peer.getBean(PeerApplication.PeerService.class).seen;
        assertEquals(new PeerApplication.Seen(Optional.of("v1"), Optional.of("r1"), Optional.of("c1")),
            seen.get("wire:one|prepared"));
        assertEquals(new PeerApplication.Seen(Optional.of("v2"), Optional.of("r2"), Optional.of("c2")),
            seen.get("wire:two|prepared"));
        assertTrue(TpfExecutionContext.versionTag().isEmpty());
        assertEquals(3, caller.getBean(SpringPipelineRunner.class).stepCount());
    }

    @Test
    void preservesRemoteStatusAndCompletesMapperFailuresExceptionally() {
        ExecutionException remote = assertThrows(ExecutionException.class,
            () -> run("rpc-failure", "v", "r", "c").get(5, TimeUnit.SECONDS));
        assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(remote.getCause()).getCode());
        for (String value : List.of("request-map-failure", "response-map-failure")) {
            ExecutionException mapping = assertThrows(ExecutionException.class,
                () -> run(value, "v", "r", "c").get(5, TimeUnit.SECONDS));
            assertInstanceOf(IllegalArgumentException.class, mapping.getCause());
        }
    }

    @Test
    void cancellationReachesDownstreamCall() throws Exception {
        var result = client().apply(new Prepared("cancel-call")).toCompletableFuture();
        var service = peer.getBean(PeerApplication.PeerService.class);
        assertTrue(service.started.await(5, TimeUnit.SECONDS));
        assertTrue(result.cancel(true));
        assertTrue(service.cancelled.await(5, TimeUnit.SECONDS));
    }

    @Test
    void startupRejectsMissingBlankUnresolvedAndMalformedTargets() {
        for (String value : List.of("", "=", "=${MISSING_GRPC_SMOKE_TARGET}", "=bad target with spaces")) {
            RuntimeException failure = assertThrows(RuntimeException.class, () -> {
                try (var unexpected = caller(value.isEmpty() ? new String[]{} : new String[]{TARGET + value}).run()) {
                    fail("Application started with invalid target");
                }
            });
            assertTrue(causes(failure).contains(TARGET));
            assertTrue(causes(failure).contains("ProcessRemoteService"));
        }
    }

    @Test
    void validUnreachableTargetDoesNotRequireNetworkAtStartup() {
        try (var context = caller(TARGET + "=static://unreachable.invalid:9090").run()) {
            assertTrue(context.isActive());
        }
    }

    private static String causes(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable current = failure; current != null; current = current.getCause()) messages.append(current.getMessage());
        return messages.toString();
    }

    private CompletableFuture<Object> run(String input, String version, String replay, String cache) {
        try (var scope = TpfExecutionContext.withHeaders(version, replay, cache)) {
            return caller.getBean(SpringPipelineRunner.class).run(new Input(input)).toCompletableFuture();
        }
    }

    @SuppressWarnings("unchecked")
    private PipelineUnaryStep<Prepared, RemoteReply> client() {
        return (PipelineUnaryStep<Prepared, RemoteReply>) caller.getBean("processRemoteGrpcClientStep");
    }

    @Configuration(proxyBeanMethods = false)
    static class Deadlines {
        @Bean
        static BeanPostProcessor grpcDeadlines() {
            return new BeanPostProcessor() {
                @Override public Object postProcessAfterInitialization(Object bean, String name) {
                    if (bean instanceof ProcessRemoteServiceGrpc.ProcessRemoteServiceFutureStub stub)
                        return stub.withDeadlineAfter(5, TimeUnit.SECONDS);
                    return bean;
                }
            };
        }
    }
}
