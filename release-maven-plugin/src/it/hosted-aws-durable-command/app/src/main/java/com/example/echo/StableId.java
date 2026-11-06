package com.example.echo;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.command.*;
@ApplicationScoped
public class StableId implements CommandIdGenerator<EchoInput> {
    public String commandId(CommandDescriptor descriptor, EchoInput input) { return input.value(); }
}
