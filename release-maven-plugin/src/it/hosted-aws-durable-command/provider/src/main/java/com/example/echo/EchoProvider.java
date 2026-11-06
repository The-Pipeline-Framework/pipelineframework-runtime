package com.example.echo;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.*;
import java.util.concurrent.*;
import org.pipelineframework.connector.*;
@ApplicationScoped
public class EchoProvider implements ConnectorProvider<Void> {
    public record Config() {}
    public static final java.util.concurrent.atomic.AtomicInteger CALLS = new java.util.concurrent.atomic.AtomicInteger();
    public ConnectorProviderId id() { return ConnectorProviderId.of("fixture.echo"); }
    public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
    public Collection<? extends ConnectorOperation> operations() {
        return List.of(new CommandOperation<EchoInput, Config, EchoOutput>() {
            public Optional<ConnectorConfigSchema<Config>> configurationSchema() { return Optional.of(ConnectorConfigSchema.record(Config.class, "fixture.echo.config", 1)); }
            public String id() { return "echo"; }
            public CompletionStage<CommandOutcome<EchoOutput>> dispatch(CommandInvocation<EchoInput, Config> invocation) {
                CALLS.incrementAndGet();
                return CompletableFuture.completedFuture(new CommandOutcome.Succeeded<>(new EchoOutput(invocation.input().value()), CommandConfirmation.none(), List.of()));
            }
        });
    }
}
