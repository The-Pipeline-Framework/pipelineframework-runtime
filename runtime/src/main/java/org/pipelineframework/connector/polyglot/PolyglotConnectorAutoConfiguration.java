package org.pipelineframework.connector.polyglot;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.quarkus.runtime.StartupEvent;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConnectorBinding;
import org.pipelineframework.config.pipeline.PipelineYamlOperationSelection;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.connector.ConnectorRegistryLifecycle;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PolyglotRuntimeConfig;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Quarkus CDI auto-configuration for Tier-2 {@link PolyglotConnectorProvider} instances.
 *
 * <p>Produces a {@link Dependent} {@link PolyglotConnectorProvider} built from the packaged
 * canonical v3 {@link PipelineContractDescriptor} and {@code pipeline.yaml} connector declarations
 * so {@link ConnectorRegistryLifecycle} discovers and starts binding-owned instances automatically.</p>
 */
@ApplicationScoped
public class PolyglotConnectorAutoConfiguration {

    @Inject
    PolyglotRuntimeConfig runtimeConfig;

    @Inject
    PipelineReleaseIdentityResolver releaseIdentityResolver;

    private volatile Optional<PolyglotConnectorSpec> cachedSpec;

    void onStart(@Observes StartupEvent event) {
        if (runtimeConfig != null && runtimeConfig.enabled() && releaseIdentityResolver != null) {
            cachedSpec = resolveSpec(releaseIdentityResolver);
        }
    }

    @Produces
    @Dependent
    @LookupIfProperty(name = "pipeline.polyglot.enabled", stringValue = "true")
    public PolyglotConnectorProvider polyglotConnectorProvider(
        PipelineReleaseIdentityResolver identityResolver
    ) {
        Objects.requireNonNull(identityResolver, "identityResolver must not be null");
        Optional<PolyglotConnectorSpec> specOpt = cachedSpec;
        if (specOpt == null) {
            specOpt = resolveSpec(identityResolver);
            cachedSpec = specOpt;
        }
        return specOpt
            .map(PolyglotConnectorProvider::new)
            .orElseThrow(() -> new IllegalStateException(
                "PolyglotConnectorProvider requires a class-free canonical v3 META-INF/pipeline/pipeline-contract.json and packaged pipeline.yaml"));
    }

    private static Optional<PolyglotConnectorSpec> resolveSpec(PipelineReleaseIdentityResolver identityResolver) {
        Optional<PipelineContractDescriptor> contractOpt = identityResolver.compiledContract();
        if (contractOpt.isEmpty() || contractOpt.orElseThrow().canonicalTypes().isEmpty()) {
            return Optional.empty();
        }
        PipelineContractDescriptor contract = contractOpt.orElseThrow();
        Optional<PipelineYamlConfig> yamlOpt = ConnectorRegistryLifecycle.loadPipelineYamlConfig();
        if (yamlOpt.isEmpty()) {
            return Optional.of(new PolyglotConnectorSpec(
                CanonicalTypeCatalogue.fromCanonicalTypes(contract.canonicalTypes()), List.of(), List.of()));
        }
        return Optional.of(buildSpec(contract, yamlOpt.orElseThrow()));
    }

    public static PolyglotConnectorSpec buildSpec(
        PipelineContractDescriptor contract,
        PipelineYamlConfig yamlConfig
    ) {
        Objects.requireNonNull(contract, "contract must not be null");
        Objects.requireNonNull(yamlConfig, "yamlConfig must not be null");
        CanonicalTypeCatalogue catalogue = CanonicalTypeCatalogue.fromCanonicalTypes(contract.canonicalTypes());
        Map<String, PipelineYamlStep> yamlByToken = indexYamlSteps(yamlConfig);
        Map<String, PolyglotConnectorSpec.QueryOperationSpec> queries = new LinkedHashMap<>();
        Map<String, PolyglotConnectorSpec.CommandOperationSpec> commands = new LinkedHashMap<>();

        for (PipelineBundleStepDescriptor step : contract.steps()) {
            PipelineYamlStep yamlStep = yamlByToken.get(normalizeToken(step.authoredName()));
            if (yamlStep == null || yamlStep.operationSelection().isEmpty()) {
                continue;
            }
            PipelineYamlOperationSelection selection = yamlStep.operationSelection().orElseThrow();
            PipelineYamlConnectorBinding binding = yamlConfig.connectors().get(selection.using());
            if (binding == null || !PolyglotConnectorProvider.PROVIDER_ID.value().equalsIgnoreCase(binding.provider())) {
                continue;
            }
            String kind = yamlStep.kind().trim().toLowerCase(Locale.ROOT);
            int major = selection.operationVersion();
            String opKey = selection.operation() + "@" + major;
            if ("query".equals(kind)) {
                var spec = new PolyglotConnectorSpec.QueryOperationSpec(
                    selection.operation(), major, step.inputTypeId(), step.outputTypeId());
                var existing = queries.putIfAbsent(opKey, spec);
                if (existing != null && !existing.equals(spec)) {
                    throw new IllegalStateException(
                        "Conflicting polyglot.connector query declarations for '" + opKey + "'");
                }
            } else if ("command".equals(kind)) {
                Map<String, Object> cfg = binding.config() == null ? Map.of() : binding.config();
                boolean reconcile = Boolean.parseBoolean(
                    String.valueOf(cfg.getOrDefault("reconciliationSupported", false)));
                Optional<String> receiptKind = Optional.ofNullable(cfg.get("receiptReferenceKind"))
                    .map(Object::toString)
                    .map(String::trim)
                    .filter(v -> !v.isBlank());
                var spec = new PolyglotConnectorSpec.CommandOperationSpec(
                    selection.operation(), major, step.inputTypeId(), step.outputTypeId(),
                    reconcile, receiptKind);
                var existing = commands.putIfAbsent(opKey, spec);
                if (existing != null && !existing.equals(spec)) {
                    throw new IllegalStateException(
                        "Conflicting polyglot.connector command declarations for '" + opKey + "'");
                }
            }
        }
        return new PolyglotConnectorSpec(catalogue, List.copyOf(queries.values()), List.copyOf(commands.values()));
    }

    private static Map<String, PipelineYamlStep> indexYamlSteps(PipelineYamlConfig yamlConfig) {
        Map<String, PipelineYamlStep> byToken = new LinkedHashMap<>();
        for (PipelineYamlStep step : yamlConfig.steps()) {
            if (step != null && step.name() != null) {
                byToken.put(normalizeToken(step.name()), step);
            }
        }
        return byToken;
    }

    private static String normalizeToken(String raw) {
        return raw == null ? "" : raw.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
    }
}
