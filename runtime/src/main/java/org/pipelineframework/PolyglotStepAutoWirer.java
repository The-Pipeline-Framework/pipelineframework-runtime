package org.pipelineframework;

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.ClientProxy;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.command.NativeCommandSelector;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConnectorBinding;
import org.pipelineframework.config.pipeline.PipelineYamlOperationSelection;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRegistryLifecycle;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.orchestrator.CanonicalFunctionHostManyToOneStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostOneToManyStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostStep;
import org.pipelineframework.orchestrator.ForeignDispatchIntentStore;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PolyglotRuntimeConfig;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.query.NativeQuerySelector;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Automatically materializes ordered {@code CanonicalFunctionHost*} and native Connector
 * ({@code query}/{@code command}) runtime steps from a packaged class-free canonical v3
 * {@link PipelineContractDescriptor} and {@link PolyglotRuntimeConfig}.
 */
@ApplicationScoped
public class PolyglotStepAutoWirer {

    private static final ObjectMapper EXACT_JSON = PipelineJson.mapper().copy()
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    @Inject
    PolyglotRuntimeConfig config;

    @Inject
    PipelineReleaseIdentityResolver releaseIdentityResolver;

    @Inject
    Instance<ForeignDispatchIntentStore> dispatchIntentStores;

    @Inject
    Instance<QueryStepSupport> queryStepSupports;

    @Inject
    Instance<CommandStepSupport> commandStepSupports;

    private volatile List<Object> cachedSteps;

    public PolyglotStepAutoWirer() {
    }

    PolyglotStepAutoWirer(
        PolyglotRuntimeConfig config,
        PipelineReleaseIdentityResolver releaseIdentityResolver,
        ForeignDispatchIntentStore dispatchIntentStore,
        Optional<QueryStepSupport> queryStepSupport,
        Optional<CommandStepSupport> commandStepSupport
    ) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.releaseIdentityResolver = Objects.requireNonNull(releaseIdentityResolver, "releaseIdentityResolver must not be null");
        this.cachedSteps = buildOrderedSteps(
            releaseIdentityResolver.compiledContract().orElseThrow(
                () -> new IllegalStateException("Class-free polyglot auto-wiring requires META-INF/pipeline/pipeline-contract.json")),
            ConnectorRegistryLifecycle.loadPipelineYamlConfig(),
            Objects.requireNonNull(dispatchIntentStore, "dispatchIntentStore must not be null"),
            queryStepSupport,
            commandStepSupport);
    }

    void onStart(@Observes StartupEvent event) {
        if (isActive()) {
            loadOrderedSteps();
        }
    }

    public boolean isActive() {
        if (config == null || !config.enabled() || releaseIdentityResolver == null) {
            return false;
        }
        return releaseIdentityResolver.compiledContract()
            .filter(PolyglotStepAutoWirer::isClassFreeContract)
            .isPresent();
    }

    public static boolean isClassFreeContract(PipelineContractDescriptor contract) {
        if (contract == null || contract.schemaVersion() < 3
            || contract.steps().isEmpty() || contract.canonicalTypes().isEmpty()) {
            return false;
        }
        return contract.steps().stream().allMatch(step ->
            (step.runtimeClass() == null || step.runtimeClass().isBlank())
                && step.inputTypeId() != null && !step.inputTypeId().isBlank()
                && step.outputTypeId() != null && !step.outputTypeId().isBlank());
    }

    public List<Object> loadOrderedSteps() {
        List<Object> existing = cachedSteps;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = cachedSteps;
            if (existing != null) {
                return existing;
            }
            PipelineContractDescriptor contract = releaseIdentityResolver.compiledContract()
                .filter(PolyglotStepAutoWirer::isClassFreeContract)
                .orElseThrow(() -> new IllegalStateException(
                    "pipeline.polyglot.enabled=true requires a class-free schema v3 META-INF/pipeline/pipeline-contract.json"));
            ForeignDispatchIntentStore intentStore = ClientProxy.unwrap(resolveIntentStore());
            Optional<QueryStepSupport> querySupport = queryStepSupports != null && queryStepSupports.isResolvable()
                ? Optional.of(ClientProxy.unwrap(queryStepSupports.get()))
                : Optional.empty();
            Optional<CommandStepSupport> commandSupport = commandStepSupports != null && commandStepSupports.isResolvable()
                ? Optional.of(ClientProxy.unwrap(commandStepSupports.get()))
                : Optional.empty();
            List<Object> built = buildOrderedSteps(
                contract,
                ConnectorRegistryLifecycle.loadPipelineYamlConfig(),
                intentStore,
                querySupport,
                commandSupport);
            cachedSteps = built;
            return built;
        }
    }

    List<Object> buildOrderedSteps(
        PipelineContractDescriptor contract,
        Optional<PipelineYamlConfig> yamlConfigOpt,
        ForeignDispatchIntentStore intentStore,
        Optional<QueryStepSupport> querySupport,
        Optional<CommandStepSupport> commandSupport
    ) {
        CanonicalTypeCatalogue catalogue = CanonicalTypeCatalogue.fromCanonicalTypes(contract.canonicalTypes());
        Map<String, String> definitionFingerprints = extractDefinitionFingerprints(contract);
        String catalogFingerprint = contract.canonicalCatalogFingerprint();
        if (catalogFingerprint == null || catalogFingerprint.isBlank()) {
            throw new IllegalStateException("Class-free canonical v3 contract is missing canonicalCatalogFingerprint");
        }
        Map<String, PipelineYamlStep> yamlByToken = yamlConfigOpt
            .map(PolyglotStepAutoWirer::indexYamlSteps)
            .orElseGet(Map::of);
        Map<String, PipelineYamlConnectorBinding> yamlConnectors = yamlConfigOpt
            .map(PipelineYamlConfig::connectors)
            .orElseGet(Map::of);

        List<Object> steps = new ArrayList<>(contract.steps().size());
        for (PipelineBundleStepDescriptor step : contract.steps()) {
            String stepId = toKebabCase(step.authoredName());
            String kind = step.kind() == null ? "internal" : step.kind().trim().toLowerCase(Locale.ROOT);
            String inputType = step.inputTypeId();
            String outputType = step.outputTypeId();
            String inputFp = requireDefinitionFingerprint(definitionFingerprints, inputType, stepId);
            String outputFp = requireDefinitionFingerprint(definitionFingerprints, outputType, stepId);

            if ("query".equals(kind)) {
                String queryCardinality = normalizeCardinality(step.cardinality());
                if (!"ONE_TO_ONE".equals(queryCardinality)) {
                    throw new IllegalStateException(
                        "Polyglot query step '" + step.authoredName()
                            + "' currently requires ONE_TO_ONE cardinality, got '" + step.cardinality() + "'");
                }
                PipelineYamlStep yamlStep = requireYamlStep(yamlByToken, step.authoredName(), kind);
                PipelineYamlOperationSelection sel = yamlStep.operationSelection().orElseThrow(
                    () -> new IllegalStateException("Query step '" + step.authoredName() + "' is missing connector operation selection"));
                PipelineYamlConnectorBinding binding = requireConnectorBinding(yamlConnectors, sel.using(), step.authoredName());
                Map<String, Object> opConfig = operationConfiguration(yamlStep.operationConfig());
                QueryStepDescriptor descriptor = QueryStepDescriptor.nativeQuery(
                    stepId,
                    JsonNode.class.getName(),
                    JsonNode.class.getName(),
                    queryCardinality,
                    new NativeQuerySelector(
                        ConnectorBindingName.of(sel.using()),
                        new ConnectorOperationIdentity(
                            ConnectorProviderId.of(binding.provider()),
                            sel.operation(),
                            ConnectorOperationKind.QUERY,
                            sel.operationVersion()),
                        binding.version()),
                    opConfig);
                steps.add(new BoundPolyglotQueryStep(
                    querySupport.orElseThrow(() -> new IllegalStateException(
                        "Query step '" + step.authoredName() + "' requires a QueryStepSupport CDI bean")),
                    descriptor, outputType, outputFp, catalogFingerprint));
            } else if ("command".equals(kind)) {
                String commandCardinality = normalizeCardinality(step.cardinality());
                if (!"ONE_TO_ONE".equals(commandCardinality)) {
                    throw new IllegalStateException(
                        "Polyglot command step '" + step.authoredName()
                            + "' requires ONE_TO_ONE cardinality, got '" + step.cardinality() + "'");
                }
                PipelineYamlStep yamlStep = requireYamlStep(yamlByToken, step.authoredName(), kind);
                PipelineYamlOperationSelection sel = yamlStep.operationSelection().orElseThrow(
                    () -> new IllegalStateException("Command step '" + step.authoredName() + "' is missing connector operation selection"));
                PipelineYamlConnectorBinding binding = requireConnectorBinding(yamlConnectors, sel.using(), step.authoredName());
                Map<String, Object> bindingCfg = binding.config() == null ? Map.of() : binding.config();
                boolean reconcile = Boolean.parseBoolean(String.valueOf(bindingCfg.getOrDefault("reconciliationSupported", false)));
                Map<String, Object> opConfig = operationConfiguration(yamlStep.operationConfig());
                CommandDescriptor descriptor = CommandDescriptor.nativeCommand(
                    stepId,
                    new NativeCommandSelector(
                        Optional.of(ConnectorBindingName.of(sel.using())),
                        new ConnectorOperationIdentity(
                            ConnectorProviderId.of(binding.provider()),
                            sel.operation(),
                            ConnectorOperationKind.COMMAND,
                            sel.operationVersion()),
                        binding.version(),
                        new CommandPolicy(
                            true,
                            true,
                            reconcile,
                            Optional.empty(),
                            Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED),
                            false)),
                    JsonNode.class.getName(),
                    JsonNode.class.getName(),
                    stepId + "-id",
                    CommandDuplicatePolicy.RETURN_RECORDED,
                    opConfig);
                steps.add(new BoundPolyglotCommandStep(
                    commandSupport.orElseThrow(() -> new IllegalStateException(
                        "Command step '" + step.authoredName() + "' requires a CommandStepSupport CDI bean")),
                    descriptor, stepId, outputType, outputFp, catalogFingerprint));
            } else {
                URI targetUri = resolveStepHostUri(stepId);
                String secret = requireSharedSecret(stepId);
                String cardinality = normalizeCardinality(step.cardinality());
                Optional<String> commandId = Optional.of("cmd-" + stepId);
                Object instance = switch (cardinality) {
                    case "ONE_TO_ONE" -> new CanonicalFunctionHostStep(
                        new CanonicalFunctionHostStep.Spec(
                            stepId, step.index(), inputType, inputFp, outputType, outputFp,
                            catalogFingerprint, catalogue, targetUri, secret,
                            config.requestTimeout(), commandId, intentStore, config.maxResponseBytes()),
                        config.allowLoopbackHttp());
                    case "ONE_TO_MANY" -> new CanonicalFunctionHostOneToManyStep(
                        new CanonicalFunctionHostOneToManyStep.Spec(
                            stepId, step.index(), inputType, inputFp, outputType, outputFp,
                            catalogFingerprint, catalogue, targetUri, secret,
                            config.requestTimeout(), config.streamIdleTimeout(), commandId, intentStore,
                            config.maxResponseBytes()),
                        config.allowLoopbackHttp());
                    case "MANY_TO_ONE" -> new CanonicalFunctionHostManyToOneStep(
                        new CanonicalFunctionHostManyToOneStep.Spec(
                            stepId, step.index(), inputType, inputFp, outputType, outputFp,
                            catalogFingerprint, catalogue, targetUri, secret,
                            config.requestTimeout(), commandId, intentStore, config.maxResponseBytes()),
                        config.allowLoopbackHttp());
                    case "MANY_TO_MANY" -> throw new IllegalStateException(
                        "Full-duplex MANY_TO_MANY polyglot streaming is not yet supported for step '"
                            + step.authoredName() + "' (ADR-0092)");
                    default -> throw new IllegalStateException(
                        "Unsupported polyglot step cardinality '" + step.cardinality() + "' for step '" + step.authoredName() + "'");
                };
                steps.add(instance);
            }
        }
        return List.copyOf(steps);
    }

    private ForeignDispatchIntentStore resolveIntentStore() {
        if (dispatchIntentStores == null || dispatchIntentStores.isUnsatisfied()) {
            throw new IllegalStateException(
                "No ForeignDispatchIntentStore CDI bean available while pipeline.polyglot.enabled=true");
        }
        return dispatchIntentStores.get();
    }

    private URI resolveStepHostUri(String stepId) {
        String perStep = config.stepUris() == null ? null : config.stepUris().get(stepId);
        String raw = perStep != null && !perStep.isBlank()
            ? perStep.trim()
            : config.functionHostUri().map(String::trim).filter(v -> !v.isBlank()).orElseThrow(
                () -> new IllegalStateException(
                    "No foreign function host URI configured for step '" + stepId
                        + "'; set pipeline.polyglot.step-uris." + stepId
                        + " or pipeline.polyglot.function-host-uri"));
        return URI.create(raw);
    }

    private String requireSharedSecret(String stepId) {
        return config.sharedSecret()
            .map(String::trim)
            .filter(v -> !v.isBlank())
            .orElseThrow(() -> new IllegalStateException(
                "pipeline.polyglot.shared-secret must be configured for foreign function step '" + stepId + "'"));
    }

    private static Map<String, String> extractDefinitionFingerprints(PipelineContractDescriptor contract) {
        Map<String, String> fps = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : contract.canonicalTypes().entrySet()) {
            Object raw = entry.getValue().get("definitionFingerprint");
            if (raw instanceof String s && !s.isBlank()) {
                fps.put(entry.getKey(), s);
            }
        }
        return fps;
    }

    private static String requireDefinitionFingerprint(Map<String, String> fps, String typeId, String stepId) {
        String fp = fps.get(typeId);
        if (fp == null || fp.isBlank()) {
            throw new IllegalStateException(
                "Canonical type '" + typeId + "' referenced by step '" + stepId + "' is missing definitionFingerprint");
        }
        return fp;
    }

    private static PipelineYamlStep requireYamlStep(
        Map<String, PipelineYamlStep> yamlByToken, String authoredName, String kind
    ) {
        PipelineYamlStep found = yamlByToken.get(normalizeToken(authoredName));
        if (found == null) {
            throw new IllegalStateException(
                "Polyglot " + kind + " step '" + authoredName + "' requires packaged pipeline.yaml with connector operation metadata");
        }
        return found;
    }

    private static PipelineYamlConnectorBinding requireConnectorBinding(
        Map<String, PipelineYamlConnectorBinding> connectors, String using, String stepName
    ) {
        PipelineYamlConnectorBinding binding = connectors.get(using);
        if (binding == null) {
            throw new IllegalStateException(
                "Step '" + stepName + "' references undeclared connector binding '" + using + "' in pipeline.yaml");
        }
        return binding;
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

    private static String toKebabCase(String raw) {
        String spaced = raw.replaceAll("([a-z0-9])([A-Z])", "$1-$2")
            .replaceAll("[^a-zA-Z0-9]+", "-")
            .toLowerCase(Locale.ROOT);
        String trimmed = spaced.replaceAll("^-+|-+$", "");
        return trimmed.isEmpty() ? "step" : trimmed;
    }

    private static Map<String, Object> operationConfiguration(Map<String, Object> raw) {
        if (raw == null || raw.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> filtered = new LinkedHashMap<>();
        raw.forEach((key, value) -> {
            if (key != null && !key.startsWith("__tpf_native_")) {
                filtered.put(key, value);
            }
        });
        return Map.copyOf(filtered);
    }

    private static String normalizeCardinality(String raw) {
        if (raw == null || raw.isBlank()) {
            return "ONE_TO_ONE";
        }
        return switch (raw.trim().toUpperCase(Locale.ROOT)) {
            case "ONE_TO_ONE", "UNARY", "SIDE_EFFECT" -> "ONE_TO_ONE";
            case "ONE_TO_MANY", "EXPANSION" -> "ONE_TO_MANY";
            case "MANY_TO_ONE", "REDUCTION" -> "MANY_TO_ONE";
            default -> raw.trim().toUpperCase(Locale.ROOT);
        };
    }

    static final class BoundPolyglotQueryStep extends ConfigurableStep
        implements StepOneToOne<TypedDurablePayload, TypedDurablePayload> {
        private final QueryStepSupport querySupport;
        private final QueryStepDescriptor descriptor;
        private final String outputType;
        private final String outputFp;
        private final String catalogFp;

        BoundPolyglotQueryStep(
            QueryStepSupport querySupport,
            QueryStepDescriptor descriptor,
            String outputType,
            String outputFp,
            String catalogFp
        ) {
            this.querySupport = querySupport;
            this.descriptor = descriptor;
            this.outputType = outputType;
            this.outputFp = outputFp;
            this.catalogFp = catalogFp;
        }

        @Override
        public Uni<TypedDurablePayload> applyOneToOne(TypedDurablePayload input) {
            try {
                JsonNode node = EXACT_JSON.readTree(input.payload());
                return querySupport.queryOneToOne(descriptor, node, JsonNode.class).map(out -> {
                    try {
                        return new TypedDurablePayload(
                            outputType, outputFp, catalogFp,
                            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
                            EXACT_JSON.writeValueAsBytes(out));
                    } catch (IOException failure) {
                        throw new IllegalStateException("Failed encoding polyglot query output", failure);
                    }
                });
            } catch (IOException failure) {
                return Uni.createFrom().failure(failure);
            }
        }
    }

    static final class BoundPolyglotCommandStep extends ConfigurableStep
        implements StepOneToOne<TypedDurablePayload, TypedDurablePayload> {
        private final CommandStepSupport commandSupport;
        private final CommandDescriptor descriptor;
        private final String stepId;
        private final String outputType;
        private final String outputFp;
        private final String catalogFp;

        BoundPolyglotCommandStep(
            CommandStepSupport commandSupport,
            CommandDescriptor descriptor,
            String stepId,
            String outputType,
            String outputFp,
            String catalogFp
        ) {
            this.commandSupport = commandSupport;
            this.descriptor = descriptor;
            this.stepId = stepId;
            this.outputType = outputType;
            this.outputFp = outputFp;
            this.catalogFp = catalogFp;
        }

        @Override
        public Uni<TypedDurablePayload> applyOneToOne(TypedDurablePayload input) {
            try {
                JsonNode node = EXACT_JSON.readTree(input.payload());
                String executionScope = PipelineExecutionContextHolder.get()
                    .map(PipelineExecutionContext::executionId)
                    .orElse("direct");
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                digest.update(executionScope.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                digest.update((byte) ':');
                digest.update(stepId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                digest.update((byte) ':');
                digest.update(input.payload());
                String commandId = stepId + "-" + HexFormat.of().formatHex(digest.digest()).substring(0, 16);
                return commandSupport.<JsonNode, JsonNode>execute(descriptor, (d, i) -> commandId, node).map(out -> {
                    try {
                        return new TypedDurablePayload(
                            outputType, outputFp, catalogFp,
                            JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
                            EXACT_JSON.writeValueAsBytes(out));
                    } catch (IOException failure) {
                        throw new IllegalStateException("Failed encoding polyglot command output", failure);
                    }
                });
            } catch (Exception failure) {
                return Uni.createFrom().failure(failure);
            }
        }
    }
}
