package org.pipelineframework.extension;

import io.quarkus.arc.deployment.ValidationPhaseBuildItem;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.BuildExtension;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.GeneratedResourceBuildItem;
import jakarta.enterprise.inject.AmbiguousResolutionException;
import jakarta.enterprise.inject.Default;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassType;
import org.pipelineframework.command.CommandEffectStore;
import org.pipelineframework.command.DynamoCommandEffectStore;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.orchestrator.release.PipelineReleaseRuntimeCapabilities;
import org.pipelineframework.query.DynamoQueryCaptureStore;
import org.pipelineframework.query.QueryCaptureStore;

/** Seals actual retained CDI selection after build conditions and unused-bean removal. */
public class CommandStoreCapabilityBuildStep {
    /** Writes package facts from the stores eligible for unqualified runtime lookup. */
    @BuildStep
    GeneratedResourceBuildItem selectedStores(ValidationPhaseBuildItem validation) {
        var commands = selectedBeans(validation, CommandEffectStore.class);
        var queries = selectedBeans(validation, QueryCaptureStore.class);
        return new GeneratedResourceBuildItem(PipelineReleaseRuntimeCapabilities.RESOURCE_PATH, facts(commands, queries).encode());
    }

    /** Mirrors the default-qualified Instance lookup used by Command and Query execution. */
    private static List<String> selectedBeans(ValidationPhaseBuildItem validation, Class<?> store) {
        var resolver = validation.getContext().get(BuildExtension.Key.DEPLOYMENT).getBeanResolver();
        var candidates = resolver.resolveBeans(ClassType.create(store),
            Set.of(AnnotationInstance.builder(Default.class).build()));
        if (candidates.isEmpty()) return List.of();
        try {
            return List.of(selectedType(resolver.resolveAmbiguity(candidates)));
        } catch (AmbiguousResolutionException ambiguous) {
            // Runtime rejects multiple eligible stores; never advertise their capability as recoverable.
            return candidates.stream().map(CommandStoreCapabilityBuildStep::selectedType).sorted().toList();
        }
    }

    /** Producer return/field types are evidence; the producer's declaring class is not. */
    static String selectedType(BeanInfo bean) {
        return (bean.isProducer() ? bean.getProviderType().name() : bean.getBeanClass()).toString();
    }

    /** Recognises only the exact supported durable implementations, failing closed otherwise. */
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
