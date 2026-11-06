package org.pipelineframework.extension;

import io.quarkus.arc.deployment.ValidationPhaseBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.GeneratedResourceBuildItem;
import java.util.HashMap;
import java.util.List;
import org.pipelineframework.command.CommandEffectStore;
import org.pipelineframework.command.DynamoCommandEffectStore;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.orchestrator.release.PipelineReleaseRuntimeCapabilities;
import org.pipelineframework.query.DynamoQueryCaptureStore;
import org.pipelineframework.query.QueryCaptureStore;

/** Seals actual retained CDI selection after build conditions and unused-bean removal. */
public class CommandStoreCapabilityBuildStep {
    @BuildStep
    GeneratedResourceBuildItem selectedStores(ValidationPhaseBuildItem validation) {
        var commands = selectedBeans(validation, CommandEffectStore.class);
        var queries = selectedBeans(validation, QueryCaptureStore.class);
        return new GeneratedResourceBuildItem(PipelineReleaseRuntimeCapabilities.RESOURCE_PATH, facts(commands, queries).encode());
    }

    private static List<String> selectedBeans(ValidationPhaseBuildItem validation, Class<?> store) {
        return validation.getContext().beans().withBeanType(store).stream()
            .map(bean -> bean.getBeanClass().toString()).sorted().toList();
    }

    static PipelineReleaseRuntimeCapabilities facts(List<String> commands, List<String> queries) {
        var facts = new HashMap<String, PipelineReleaseRuntimeCapabilities.Selection>();
        boolean durableCommands = commands.equals(List.of(DynamoCommandEffectStore.class.getName()));
        boolean memoryCommands = commands.equals(List.of(InMemoryCommandEffectStore.class.getName()));
        String commandProvider = durableCommands ? "dynamo" : memoryCommands ? "memory" : "unsupported";
        facts.put(PipelineReleaseRuntimeCapabilities.COMMAND_EFFECT_DYNAMODB,
            new PipelineReleaseRuntimeCapabilities.Selection(commandProvider, commands, durableCommands));
        if (queries.equals(List.of(DynamoQueryCaptureStore.class.getName()))) {
            facts.put(PipelineReleaseRuntimeCapabilities.QUERY_CAPTURE_DYNAMODB,
                new PipelineReleaseRuntimeCapabilities.Selection("dynamo", queries, true));
        }
        return new PipelineReleaseRuntimeCapabilities(1, facts);
    }
}
