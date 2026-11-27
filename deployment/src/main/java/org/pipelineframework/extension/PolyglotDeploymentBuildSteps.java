package org.pipelineframework.extension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.ApplicationArchivesBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.PolyglotStepAutoWirer;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorRegistryLifecycle;
import org.pipelineframework.connector.polyglot.PolyglotConnectorAutoConfiguration;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.PolyglotRuntimeProducers;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.query.QueryStepSupport;

/**
 * Quarkus build-time validation, bean retention, and native-image resource registration for
 * class-free canonical v3 polyglot applications (ADR-0092).
 */
public class PolyglotDeploymentBuildSteps {

    private static final Set<String> SUPPORTED_INTERNAL_CARDINALITIES = Set.of(
        "ONE_TO_ONE", "UNARY", "SIDE_EFFECT",
        "ONE_TO_MANY", "EXPANSION",
        "MANY_TO_ONE", "REDUCTION",
        "MANY_TO_MANY");
    private static final Set<String> SUPPORTED_CONNECTOR_CARDINALITIES = Set.of(
        "ONE_TO_ONE", "UNARY", "SIDE_EFFECT",
        "ONE_TO_MANY", "EXPANSION",
        "MANY_TO_ONE", "REDUCTION");

    @BuildStep
    void registerClassFreePolyglotDeployment(
        ApplicationArchivesBuildItem archives,
        BuildProducer<NativeImageResourceBuildItem> nativeResources,
        BuildProducer<UnremovableBeanBuildItem> unremovableBeans
    ) {
        Optional<PipelineContractDescriptor> classFreeContract = findClassFreeContract(archives);
        if (classFreeContract.isEmpty()) {
            return;
        }
        validateClassFreeContract(classFreeContract.orElseThrow());

        List<String> resources = new ArrayList<>();
        resources.add(PipelineContractDescriptor.RESOURCE_PATH);
        if (hasPackagedResource(archives, "pipeline.yaml")) {
            resources.add("pipeline.yaml");
        }
        if (hasPackagedResource(archives, "META-INF/pipeline/pipeline.yaml")) {
            resources.add("META-INF/pipeline/pipeline.yaml");
        }
        nativeResources.produce(new NativeImageResourceBuildItem(resources));

        unremovableBeans.produce(UnremovableBeanBuildItem.beanClassNames(
            PolyglotStepAutoWirer.class.getName(),
            PolyglotRuntimeProducers.class.getName(),
            PolyglotConnectorAutoConfiguration.class.getName(),
            ConnectorRegistryLifecycle.class.getName(),
            PipelineExecutionService.class.getName(),
            "org.pipelineframework.QueueAsyncCoordinator",
            QueryStepSupport.class.getName(),
            CommandStepSupport.class.getName()));
    }

    static void validateClassFreeContract(PipelineContractDescriptor contract) {
        if (contract.canonicalTypes() == null || contract.canonicalTypes().isEmpty()) {
            throw new IllegalStateException(
                "Class-free canonical v3 contract '" + contract.pipelineId() + "' is missing canonicalTypes");
        }
        if (contract.canonicalCatalogFingerprint() == null || contract.canonicalCatalogFingerprint().isBlank()) {
            throw new IllegalStateException(
                "Class-free canonical v3 contract '" + contract.pipelineId() + "' is missing canonicalCatalogFingerprint");
        }
        for (PipelineBundleStepDescriptor step : contract.steps()) {
            String kind = step.kind() == null || step.kind().isBlank()
                ? "internal"
                : step.kind().trim().toLowerCase(Locale.ROOT);
            String card = step.cardinality() == null ? "ONE_TO_ONE" : step.cardinality().trim().toUpperCase(Locale.ROOT);
            Set<String> allowed = "internal".equals(kind)
                ? SUPPORTED_INTERNAL_CARDINALITIES
                : SUPPORTED_CONNECTOR_CARDINALITIES;
            if (!allowed.contains(card)) {
                throw new IllegalStateException(
                    "Class-free polyglot step '" + step.authoredName()
                        + "' (kind='" + kind + "') declares unsupported cardinality '" + step.cardinality() + "'");
            }
            requireCanonicalType(contract, step.authoredName(), "inputTypeId", step.inputTypeId());
            requireCanonicalType(contract, step.authoredName(), "outputTypeId", step.outputTypeId());
        }
    }

    private static void requireCanonicalType(
        PipelineContractDescriptor contract, String stepName, String role, String typeId
    ) {
        var definition = contract.canonicalTypes().get(typeId);
        if (definition == null) {
            throw new IllegalStateException(
                "Class-free polyglot step '" + stepName + "' references unknown canonical " + role + " '" + typeId + "'");
        }
        Object fp = definition.get("definitionFingerprint");
        if (!(fp instanceof String s) || s.isBlank()) {
            throw new IllegalStateException(
                "Class-free polyglot step '" + stepName + "' references canonical " + role + " '" + typeId
                    + "' without definitionFingerprint");
        }
    }

    private static Optional<PipelineContractDescriptor> findClassFreeContract(ApplicationArchivesBuildItem archives) {
        if (archives == null) {
            return Optional.empty();
        }
        for (var archive : archives.getAllArchives()) {
            Optional<Path> path = Optional.ofNullable(archive.getChildPath(PipelineContractDescriptor.RESOURCE_PATH))
                .filter(Files::isRegularFile);
            if (path.isEmpty()) {
                continue;
            }
            try (var in = Files.newInputStream(path.orElseThrow())) {
                PipelineContractDescriptor contract = PipelineJson.mapper().readValue(in, PipelineContractDescriptor.class);
                if (isClassFreeCandidate(contract)) {
                    return Optional.of(contract);
                }
            } catch (IOException failure) {
                throw new IllegalStateException(
                    "Cannot read " + PipelineContractDescriptor.RESOURCE_PATH + " during Quarkus build", failure);
            }
        }
        return Optional.empty();
    }

    static boolean isClassFreeCandidate(PipelineContractDescriptor contract) {
        if (contract == null || contract.schemaVersion() < 3 || contract.steps().isEmpty()) {
            return false;
        }
        return contract.steps().stream().allMatch(step ->
            (step.runtimeClass() == null || step.runtimeClass().isBlank())
                && step.inputTypeId() != null && !step.inputTypeId().isBlank()
                && step.outputTypeId() != null && !step.outputTypeId().isBlank());
    }

    private static boolean hasPackagedResource(ApplicationArchivesBuildItem archives, String resourcePath) {
        for (var archive : archives.getAllArchives()) {
            Path path = archive.getChildPath(resourcePath);
            if (path != null && Files.isRegularFile(path)) {
                return true;
            }
        }
        return false;
    }
}
