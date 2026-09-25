package org.pipelineframework.objectpublish;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.config.boundary.PipelineObjectOutputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.orchestrator.PagedExecutionState;
import org.pipelineframework.orchestrator.PagedTransitionContext;

/**
 * Runtime-neutral terminal object publish engine.
 */
public final class ObjectPublishRunner {

    private static final Logger LOG = Logger.getLogger(ObjectPublishRunner.class);
    private static final Pattern JAVA_CLASS_NAME = Pattern.compile("[a-zA-Z0-9._$]+");
    private static final ObjectPublishRunner DISABLED =
        new ObjectPublishRunner(null, new ObjectTargetRegistry(List.of()), ObjectPublishTelemetry.NOOP, List.of(), true);

    private final PipelineYamlConfig config;
    private final ObjectTargetRegistry registry;
    private final ObjectPublishTelemetry telemetry;
    private final List<TerminalOutputAdapter<?, ?>> terminalOutputAdapters;
    private final boolean disabled;
    private volatile Object resolvedMapper;
    private volatile Class<?> resolvedOutputType;
    private volatile TerminalOutputAdapter<?, ?> resolvedTerminalOutputAdapter;

    public ObjectPublishRunner(
        PipelineYamlConfig config,
        ObjectTargetRegistry registry,
        ObjectPublishTelemetry telemetry
    ) {
        this(config, registry, telemetry, loadTerminalOutputAdapters(), false);
    }

    public ObjectPublishRunner(
        PipelineYamlConfig config,
        ObjectTargetRegistry registry,
        ObjectPublishTelemetry telemetry,
        List<TerminalOutputAdapter<?, ?>> terminalOutputAdapters
    ) {
        this(config, registry, telemetry, terminalOutputAdapters, false);
    }

    private ObjectPublishRunner(
        PipelineYamlConfig config,
        ObjectTargetRegistry registry,
        ObjectPublishTelemetry telemetry,
        List<TerminalOutputAdapter<?, ?>> terminalOutputAdapters,
        boolean disabled
    ) {
        this.config = config;
        this.registry = Objects.requireNonNull(registry, "registry");
        this.telemetry = telemetry == null ? ObjectPublishTelemetry.NOOP : telemetry;
        this.terminalOutputAdapters = terminalOutputAdapters == null ? List.of() : List.copyOf(terminalOutputAdapters);
        this.disabled = disabled;
    }

    public static ObjectPublishRunner disabled() {
        return DISABLED;
    }

    public static ObjectPublishRunner loadFromDefaultConfig() {
        return loadFromDefaultConfig(ObjectPublishTelemetry.NOOP);
    }

    public static ObjectPublishRunner loadFromDefaultConfig(ObjectPublishTelemetry telemetry) {
        Optional<Path> configPath = locateConfig();
        if (configPath.isEmpty()) {
            return disabled();
        }
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath.get());
        return new ObjectPublishRunner(config, ObjectTargetRegistry.load(), telemetry);
    }

    public boolean enabled() {
        return !disabled && objectOutput().isPresent();
    }

    public Object publish(Object terminalOutput) {
        if (!enabled()) {
            return terminalOutput;
        }
        if (terminalOutput instanceof Multi<?> multi) {
            return publishMulti(multi);
        }
        if (terminalOutput instanceof Uni<?> uni) {
            return publishUni(uni);
        }
        return terminalOutput;
    }

    private Multi<?> publishMulti(Multi<?> multi) {
        PipelineObjectOutputConfig output = objectOutput()
            .orElseThrow(() -> new IllegalStateException("pipeline output object binding is not configured"));
        PipelineObjectPublishConfig target = target(output);
        StreamingObjectPublishMapper<Object> mapper = streamingMapper();
        Optional<PagedPublishContext> page = pageContext();
        ObjectTargetProvider provider = registry.require(target.provider());
        if (page.isPresent() && (!(mapper instanceof PagedStreamingObjectPublishMapper<?>)
            || !(provider instanceof PagedObjectTargetProvider))) {
            throw new IllegalStateException(
                "paged Object Publish requires PagedStreamingObjectPublishMapper and PagedObjectTargetProvider");
        }
        StreamingPublishState state = new StreamingPublishState(target, provider, mapper, page);
        return multi.onItem()
            .transformToUniAndConcatenate(item -> state.publishItem(adaptTerminalItem(item)).replaceWith(item))
            .onCompletion().call(state::closeAll)
            .onFailure().call(state::abortAll)
            .onCancellation().call(() -> state.abortAll(new CancellationException("Object publish stream cancelled")));
    }

    private Uni<?> publishUni(Uni<?> uni) {
        return uni.onItem().transformToUni(item -> {
            if (item == null) {
                return Uni.createFrom().nullItem();
            }
            return publishItems(List.of(item)).replaceWith(item);
        });
    }

    public Uni<Void> publishItems(List<?> items) {
        return publishItems(items, pageContext());
    }

    public Uni<Void> publishItems(
        List<?> items,
        String executionId,
        PagedExecutionState page) {
        Objects.requireNonNull(executionId, "executionId must not be null");
        Objects.requireNonNull(page, "page must not be null");
        return publishItems(items, Optional.of(new PagedPublishContext(
            executionId, page.toTransitionContext())));
    }

    private Uni<Void> publishItems(List<?> items, Optional<PagedPublishContext> page) {
        PipelineObjectOutputConfig output = objectOutput()
            .orElseThrow(() -> new IllegalStateException("pipeline output object binding is not configured"));
        PipelineObjectPublishConfig target = target(output);
        if (items == null || items.isEmpty()) {
            telemetry.skipped(target.name());
            return Uni.createFrom().voidItem();
        }
        if (mapper() instanceof StreamingObjectPublishMapper<?>) {
            return publishStreamingItems(items, page);
        }
        if (page.isPresent()) {
            return Uni.createFrom().failure(new IllegalStateException(
                "paged Object Publish requires PagedStreamingObjectPublishMapper"));
        }
        return publishBatchItems(target, items);
    }

    private Uni<Void> publishBatchItems(PipelineObjectPublishConfig target, List<?> items) {
        ObjectPublishMapper<Object> mapper = batchMapper();
        List<Object> domainItems = items.stream().map(this::adaptTerminalItem).toList();
        Map<String, List<Object>> groups = group(domainItems, mapper);
        telemetry.grouped(target.name(), items.size(), groups.size());
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (Map.Entry<String, List<Object>> entry : groups.entrySet()) {
            chain = chain.chain(() -> writeBatchGroup(target, mapper, entry.getKey(), entry.getValue()));
        }
        return chain;
    }

    private Uni<Void> publishStreamingItems(List<?> items, Optional<PagedPublishContext> page) {
        PipelineObjectOutputConfig output = objectOutput()
            .orElseThrow(() -> new IllegalStateException("pipeline output object binding is not configured"));
        PipelineObjectPublishConfig target = target(output);
        if (items == null || items.isEmpty()) {
            telemetry.skipped(target.name());
            return Uni.createFrom().voidItem();
        }
        StreamingObjectPublishMapper<Object> mapper = streamingMapper();
        ObjectTargetProvider provider = registry.require(target.provider());
        if (page.isPresent() && (!(mapper instanceof PagedStreamingObjectPublishMapper<?>)
            || !(provider instanceof PagedObjectTargetProvider))) {
            return Uni.createFrom().failure(new IllegalStateException(
                "paged Object Publish requires PagedStreamingObjectPublishMapper and PagedObjectTargetProvider"));
        }
        StreamingPublishState state = new StreamingPublishState(
            target, provider, mapper, page);
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (Object item : items) {
            chain = chain.chain(() -> state.publishItem(adaptTerminalItem(item)).replaceWithVoid());
        }
        return chain.chain(state::closeAll).onFailure().call(state::abortAll);
    }

    private Object adaptTerminalItem(Object item) {
        Objects.requireNonNull(item, "Object Publish terminal item must not be null");
        Class<?> outputType = outputType();
        if (outputType.isInstance(item)) {
            return item;
        }
        TerminalOutputAdapter<?, ?> adapter = terminalOutputAdapter(outputType)
            .orElseThrow(() -> new IllegalStateException(
                "Object Publish received terminal item type '" + item.getClass().getName()
                    + "' but output.consumes.type is '" + outputType.getName()
                    + "' and no " + TerminalOutputAdapter.class.getName() + " is available"));
        return toDomain(adapter, item);
    }

    @SuppressWarnings("unchecked")
    private static Object toDomain(TerminalOutputAdapter<?, ?> adapter, Object item) {
        return ((TerminalOutputAdapter<Object, Object>) adapter).toDomain(item);
    }

    private Map<String, List<Object>> group(List<?> items, ObjectPublishMapper<Object> mapper) {
        Map<String, List<Object>> groups = new LinkedHashMap<>();
        for (Object item : items) {
            String groupKey = normalize(mapper.groupKey(item))
                .orElseThrow(() -> new IllegalStateException("Object publish mapper returned a blank group key"));
            groups.computeIfAbsent(groupKey, ignored -> new java.util.ArrayList<>()).add(item);
        }
        return groups;
    }

    private Uni<Void> writeBatchGroup(
        PipelineObjectPublishConfig target,
        ObjectPublishMapper<Object> mapper,
        String groupKey,
        List<Object> items
    ) {
        Map<String, String> labels = mapper.labels(groupKey, List.copyOf(items));
        if (labels == null) {
            labels = Map.of();
        }
        ObjectPayload payload = mapper.render(groupKey, List.copyOf(items));
        if (payload == null) {
            throw new IllegalStateException("Object publish mapper returned null payload for group: " + groupKey);
        }
        byte[] bytes = payload.bytes();
        String contentType = normalize(payload.contentType()).orElse(target.payload().contentType());
        String objectKey = renderKey(target.naming().keyTemplate(), groupKey, labels);
        String checksum = sha256(bytes);
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("target", target.name());
        metadata.put("groupKey", groupKey);
        metadata.putAll(labels);
        metadata.putAll(payload.metadata());
        ObjectWriteRequest request = new ObjectWriteRequest(
            target.name(),
            target,
            objectKey,
            bytes,
            contentType,
            metadata,
            checksum,
            "object-publish:" + target.name() + ":" + objectKey + ":" + checksum);
        ObjectTargetProvider provider = registry.require(target.provider());
        Instant writeStarted = Instant.now();
        return toUni(provider.write(request))
            .invoke(result -> {
                telemetry.writeDuration(target.name(), target.provider(), Duration.between(writeStarted, Instant.now()));
                telemetry.published(target.name(), target.provider(), objectKey, result.bytes());
            })
            .onFailure().invoke(failure -> {
                telemetry.writeDuration(target.name(), target.provider(), Duration.between(writeStarted, Instant.now()));
                telemetry.failed(target.name(), target.provider(), objectKey, failure);
                LOG.warnf(failure, "Object publish failed for target=%s key=%s", target.name(), objectKey);
            })
            .replaceWithVoid();
    }

    private String renderKey(String template, String groupKey, Map<String, String> labels) {
        String rendered = template == null ? "{groupKey}" : template;
        rendered = rendered.replace("{groupKey}", groupKey);
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            rendered = rendered.replace("{" + entry.getKey() + "}", entry.getValue());
        }
        return normalize(rendered)
            .orElseThrow(() -> new IllegalStateException("Object publish rendered a blank object key"));
    }

    private Object mapper() {
        PipelineObjectOutputConfig output = objectOutput()
            .orElseThrow(() -> new IllegalStateException("pipeline output object binding is not configured"));
        Object mapper = resolvedMapper;
        if (mapper != null) {
            return mapper;
        }
        synchronized (this) {
            mapper = resolvedMapper;
            if (mapper != null) {
                return mapper;
            }
            resolvedMapper = createMapper(output);
            return resolvedMapper;
        }
    }

    @SuppressWarnings("unchecked")
    private ObjectPublishMapper<Object> batchMapper() {
        Object mapper = mapper();
        if (!(mapper instanceof ObjectPublishMapper<?> publishMapper)) {
            throw new IllegalStateException(
                "Object output mapper '" + mapper.getClass().getName() + "' must implement "
                    + ObjectPublishMapper.class.getName() + " for batch publish");
        }
        return (ObjectPublishMapper<Object>) publishMapper;
    }

    @SuppressWarnings("unchecked")
    private StreamingObjectPublishMapper<Object> streamingMapper() {
        Object mapper = mapper();
        if (!(mapper instanceof StreamingObjectPublishMapper<?> publishMapper)) {
            throw new IllegalStateException(
                "Object output mapper '" + mapper.getClass().getName() + "' must implement "
                    + StreamingObjectPublishMapper.class.getName() + " for streaming Object Publish");
        }
        return (StreamingObjectPublishMapper<Object>) publishMapper;
    }

    private Object createMapper(PipelineObjectOutputConfig output) {
        String mapperClassName = normalize(output.mapper())
            .orElseThrow(() -> new IllegalStateException("Object output mapper must not be blank"));
        validateMapperClassName(mapperClassName);
        try {
            Class<?> mapperClass = Class.forName(mapperClassName, true, Thread.currentThread().getContextClassLoader());
            Object mapper = mapperClass.getDeclaredConstructor().newInstance();
            if (!(mapper instanceof ObjectPublishMapper<?>) && !(mapper instanceof StreamingObjectPublishMapper<?>)) {
                throw new IllegalStateException(
                    "Object output mapper '" + mapperClassName + "' must implement "
                        + ObjectPublishMapper.class.getName() + " or " + StreamingObjectPublishMapper.class.getName());
            }
            return mapper;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to create object output mapper: " + mapperClassName, e);
        }
    }

    private Class<?> outputType() {
        Class<?> outputType = resolvedOutputType;
        if (outputType != null) {
            return outputType;
        }
        PipelineObjectOutputConfig output = objectOutput()
            .orElseThrow(() -> new IllegalStateException("pipeline output object binding is not configured"));
        String outputTypeName = normalize(output.type())
            .orElseThrow(() -> new IllegalStateException("Object output consumes type must not be blank"));
        validateOutputTypeClassName(outputTypeName);
        try {
            outputType = Class.forName(outputTypeName, false, Thread.currentThread().getContextClassLoader());
            resolvedOutputType = outputType;
            return outputType;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Object output consumes type not found: " + outputTypeName, e);
        }
    }

    private Optional<TerminalOutputAdapter<?, ?>> terminalOutputAdapter(Class<?> outputType) {
        TerminalOutputAdapter<?, ?> adapter = resolvedTerminalOutputAdapter;
        if (adapter != null && Objects.equals(adapter.domainType(), outputType)) {
            return Optional.of(adapter);
        }
        for (TerminalOutputAdapter<?, ?> candidate : terminalOutputAdapters) {
            if (candidate != null && Objects.equals(candidate.domainType(), outputType)) {
                resolvedTerminalOutputAdapter = candidate;
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    private void validateMapperClassName(String mapperClassName) {
        validateConfiguredClassName("Object output mapper", mapperClassName);
    }

    private void validateOutputTypeClassName(String outputTypeName) {
        validateConfiguredClassName("Object output consumes type", outputTypeName);
    }

    private void validateConfiguredClassName(String label, String className) {
        String basePackage = normalize(config.basePackage())
            .orElseThrow(() -> new IllegalStateException(
                label + " requires pipeline basePackage for runtime class loading"));
        if (!JAVA_CLASS_NAME.matcher(className).matches()
            || className.contains("..")
            || className.contains("/")
            || className.contains("\\")) {
            throw new IllegalStateException(label + " class name is invalid: " + className);
        }
        if (!className.startsWith(basePackage + ".")) {
            throw new IllegalStateException(
                label + " '" + className + "' must be under basePackage '" + basePackage + "'");
        }
    }

    private Optional<PipelineObjectOutputConfig> objectOutput() {
        return config == null || config.output() == null
            ? Optional.empty()
            : Optional.ofNullable(config.output().object());
    }

    private PipelineObjectPublishConfig target(PipelineObjectOutputConfig output) {
        PipelineObjectPublishConfig target = config.publish().get(output.target());
        if (target == null) {
            throw new IllegalStateException("output object publish target not found: " + output.target());
        }
        return target;
    }

    private static Optional<Path> locateConfig() {
        Optional<String> explicit = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        if (explicit.isPresent()) {
            return Optional.of(Path.of(explicit.get()).toAbsolutePath().normalize());
        }
        try {
            return new PipelineYamlConfigLocator().locate(Path.of(System.getProperty("user.dir")));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static List<TerminalOutputAdapter<?, ?>> loadTerminalOutputAdapters() {
        List<TerminalOutputAdapter<?, ?>> adapters = new ArrayList<>();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        ServiceLoader<TerminalOutputAdapter> serviceLoader = loader == null
            ? ServiceLoader.load(TerminalOutputAdapter.class)
            : ServiceLoader.load(TerminalOutputAdapter.class, loader);
        for (TerminalOutputAdapter<?, ?> adapter : serviceLoader) {
            adapters.add(adapter);
        }
        return List.copyOf(adapters);
    }

    private static Optional<String> firstNonBlank(String primary, String fallback) {
        if (primary != null && !primary.isBlank()) {
            return Optional.of(primary.trim());
        }
        if (fallback != null && !fallback.isBlank()) {
            return Optional.of(fallback.trim());
        }
        return Optional.empty();
    }

    private static String sha256(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is not available", e);
        }
    }

    private static <T> Uni<T> toUni(CompletionStage<T> stage) {
        return Uni.createFrom().completionStage(stage);
    }

    private static Optional<String> normalize(String value) {
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value.trim());
    }

    /** Composes committed parts after the exhausted page and before logical execution success. */
    public Uni<Void> completePagedOutput(String executionId, int lastPageIndex) {
        PipelineObjectOutputConfig output = objectOutput()
            .orElseThrow(() -> new IllegalStateException("pipeline output object binding is not configured"));
        PipelineObjectPublishConfig target = target(output);
        if (!(registry.require(target.provider()) instanceof PagedObjectTargetProvider provider)) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Object target provider does not support paged composition: " + target.provider()));
        }
        if (!(streamingMapper() instanceof PagedStreamingObjectPublishMapper<?> rawMapper)) {
            return Uni.createFrom().failure(new IllegalStateException(
                "Object output mapper does not support paged composition"));
        }
        @SuppressWarnings("unchecked")
        PagedStreamingObjectPublishMapper<Object> mapper =
            (PagedStreamingObjectPublishMapper<Object>) rawMapper;
        String stageRoot = stageRoot(executionId, target.name());
        return toUni(provider.listParts(new PagedObjectPartQuery(target.name(), target, stageRoot)))
            .onItem().transformToUni(parts -> composeGroups(
                executionId, lastPageIndex, target, provider, mapper, parts));
    }

    private Uni<Void> composeGroups(
        String executionId,
        int lastPageIndex,
        PipelineObjectPublishConfig target,
        PagedObjectTargetProvider provider,
        PagedStreamingObjectPublishMapper<Object> mapper,
        List<PagedObjectPart> allParts) {
        Map<String, List<PagedObjectPart>> groups = new LinkedHashMap<>();
        allParts.stream()
            .filter(part -> part.pageIndex() <= lastPageIndex)
            .forEach(part -> groups.computeIfAbsent(part.groupKey(), ignored -> new ArrayList<>()).add(part));
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (Map.Entry<String, List<PagedObjectPart>> entry : groups.entrySet()) {
            chain = chain.chain(() -> composeGroup(executionId, target, provider, mapper,
                entry.getKey(), entry.getValue()));
        }
        return chain;
    }

    private Uni<Void> composeGroup(
        String executionId,
        PipelineObjectPublishConfig target,
        PagedObjectTargetProvider provider,
        PagedStreamingObjectPublishMapper<Object> mapper,
        String groupKey,
        List<PagedObjectPart> parts) {
        List<PagedObjectPart> ordered = parts.stream()
            .sorted(java.util.Comparator.comparingInt(PagedObjectPart::pageIndex))
            .toList();
        String finalKey = ordered.getFirst().finalObjectKey();
        String contentType = ordered.getFirst().contentType();
        if (ordered.stream().anyMatch(part -> !finalKey.equals(part.finalObjectKey()))) {
            return Uni.createFrom().failure(new IllegalStateException(
                "paged object group changed its final key across pages: " + groupKey));
        }
        Map<String, String> combined = Map.of();
        for (PagedObjectPart part : ordered) {
            combined = safeMap(mapper.combinePageMetadata(groupKey, combined, part.metadata()),
                "combined page metadata");
        }
        ObjectPayloadChunk prefix = mapper.groupPrefix(groupKey);
        ObjectPayloadChunk suffix = mapper.groupSuffix(groupKey, combined);
        Map<String, String> metadata = new LinkedHashMap<>(combined);
        metadata.put("target", target.name());
        metadata.put("groupKey", groupKey);
        PagedObjectCompositionRequest request = new PagedObjectCompositionRequest(
            target.name(), target, finalKey, contentType, metadata,
            "object-publish-compose:" + target.name() + ":" + executionId + ":" + finalKey,
            prefix == null ? new byte[0] : prefix.bytes(),
            ordered.stream().map(PagedObjectPart::objectKey).toList(),
            suffix == null ? new byte[0] : suffix.bytes());
        Instant started = Instant.now();
        return toUni(provider.compose(request))
            .invoke(result -> {
                telemetry.writeDuration(target.name(), target.provider(), Duration.between(started, Instant.now()));
                telemetry.published(target.name(), target.provider(), finalKey, result.bytes());
            })
            .replaceWithVoid();
    }

    private static Optional<PagedPublishContext> pageContext() {
        return Optional.ofNullable(AwaitExecutionContextHolder.get())
            .flatMap(context -> context.pageContext()
                .map(page -> new PagedPublishContext(context.executionId(), page)));
    }

    private static String stageRoot(String executionId, String targetName) {
        return ".tpf-pages/" + sha256(executionId.getBytes(java.nio.charset.StandardCharsets.UTF_8))
            + "/" + targetName + "/";
    }

    private record PagedPublishContext(String executionId, PagedTransitionContext page) {
    }

    private final class StreamingPublishState {
        private final PipelineObjectPublishConfig target;
        private final ObjectTargetProvider provider;
        private final StreamingObjectPublishMapper<Object> mapper;
        private final Optional<PagedPublishContext> page;
        private final Map<String, StreamingGroupState> groups = new LinkedHashMap<>();

        private StreamingPublishState(
            PipelineObjectPublishConfig target,
            ObjectTargetProvider provider,
            StreamingObjectPublishMapper<Object> mapper,
            Optional<PagedPublishContext> page
        ) {
            this.target = target;
            this.provider = provider;
            this.mapper = mapper;
            this.page = page;
        }

        private Uni<Object> publishItem(Object item) {
            String groupKey = normalize(mapper.groupKey(item))
                .orElseThrow(() -> new IllegalStateException("Object publish mapper returned a blank group key"));
            StreamingGroupState group = groups.get(groupKey);
            if (group == null) {
                if (groups.size() >= target.grouping().maxOpenGroups()) {
                    throw new IllegalStateException(
                        "Object publish target '" + target.name() + "' exceeded grouping.maxOpenGroups="
                            + target.grouping().maxOpenGroups());
                }
                group = openGroup(groupKey, item);
                groups.put(groupKey, group);
            }
            return group.writeItem(item).replaceWith(item);
        }

        private StreamingGroupState openGroup(String groupKey, Object firstItem) {
            ObjectPublishGroupRenderer<Object> renderer = mapper.openGroup(groupKey, firstItem);
            if (renderer == null) {
                throw new IllegalStateException("Object publish mapper returned null renderer for group: " + groupKey);
            }
            Map<String, String> initialLabels = safeMap(renderer.initialLabels(), "initial labels");
            String contentType = normalize(renderer.contentType()).orElse(target.payload().contentType());
            String objectKey = renderKey(target.naming().keyTemplate(), groupKey, initialLabels);
            String finalObjectKey = objectKey;
            Map<String, String> metadata = new LinkedHashMap<>();
            metadata.put("target", target.name());
            metadata.put("groupKey", groupKey);
            metadata.putAll(initialLabels);
            if (page.isPresent()) {
                PagedPublishContext context = page.orElseThrow();
                String groupToken = sha256(groupKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                objectKey = stageRoot(context.executionId(), target.name()) + groupToken
                    + "/page-" + String.format(java.util.Locale.ROOT, "%020d", context.page().pageIndex()) + ".part";
                metadata.put("tpf.page.index", String.valueOf(context.page().pageIndex()));
                metadata.put("tpf.page.group", groupKey);
                metadata.put("tpf.page.finalKey", finalObjectKey);
                metadata.put("tpf.page.contentType", contentType);
                metadata.put("tpf.page.partKey", objectKey);
            }
            ObjectWriteOpenRequest request = new ObjectWriteOpenRequest(
                target.name(),
                target,
                objectKey,
                contentType,
                metadata,
                page.isPresent()
                    ? "object-publish-page:" + target.name() + ":" + objectKey
                    : "object-publish:" + target.name() + ":" + objectKey);
            return new StreamingGroupState(target, provider, renderer, request, metadata);
        }

        private Uni<Void> closeAll() {
            if (groups.isEmpty()) {
                telemetry.skipped(target.name());
                return Uni.createFrom().voidItem();
            }
            long itemCount = groups.values().stream().mapToLong(StreamingGroupState::itemCount).sum();
            telemetry.grouped(target.name(), Math.toIntExact(itemCount), groups.size());
            Uni<Void> chain = Uni.createFrom().voidItem();
            for (StreamingGroupState group : groups.values()) {
                chain = chain.chain(group::close);
            }
            return chain;
        }

        private Uni<Void> abortAll(Throwable failure) {
            Uni<Void> chain = Uni.createFrom().voidItem();
            for (StreamingGroupState group : groups.values()) {
                chain = chain.chain(() -> group.abort(failure));
            }
            return chain;
        }
    }

    private final class StreamingGroupState {
        private final PipelineObjectPublishConfig target;
        private final ObjectTargetProvider provider;
        private final ObjectPublishGroupRenderer<Object> renderer;
        private final ObjectWriteOpenRequest openRequest;
        private final Map<String, String> initialMetadata;
        private final MessageDigest digest;
        private ObjectWriteSession session;
        private Uni<ObjectWriteSession> openSession;
        private long bytes;
        private long itemCount;
        private boolean closed;
        private boolean aborted;

        private StreamingGroupState(
            PipelineObjectPublishConfig target,
            ObjectTargetProvider provider,
            ObjectPublishGroupRenderer<Object> renderer,
            ObjectWriteOpenRequest openRequest,
            Map<String, String> initialMetadata
        ) {
            this.target = target;
            this.provider = provider;
            this.renderer = renderer;
            this.openRequest = openRequest;
            this.initialMetadata = Map.copyOf(initialMetadata);
            try {
                this.digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 digest is not available", e);
            }
        }

        private long itemCount() {
            return itemCount;
        }

        private Uni<Void> writeItem(Object item) {
            boolean firstItem = itemCount == 0;
            itemCount++;
            Uni<Void> chain = firstItem ? writeChunk(renderer.onOpen()) : Uni.createFrom().voidItem();
            return chain.chain(() -> writeChunk(renderer.onItem(item)));
        }

        private Uni<Void> close() {
            if (closed || aborted) {
                return Uni.createFrom().voidItem();
            }
            closed = true;
            Instant writeStarted = Instant.now();
            return writeChunk(renderer.onClose())
                .chain(() -> open())
                .chain(session -> toUni(session.close(new ObjectWriteCloseRequest(bytes, checksum(), finalMetadata()))))
                .invoke(result -> {
                    telemetry.writeDuration(target.name(), target.provider(), Duration.between(writeStarted, Instant.now()));
                    telemetry.published(target.name(), target.provider(), openRequest.objectKey(), result.bytes());
                })
                .onFailure().invoke(failure -> {
                    telemetry.writeDuration(target.name(), target.provider(), Duration.between(writeStarted, Instant.now()));
                    telemetry.failed(target.name(), target.provider(), openRequest.objectKey(), failure);
                    LOG.warnf(failure, "Object publish failed for target=%s key=%s", target.name(), openRequest.objectKey());
                })
                .replaceWithVoid();
        }

        private Uni<Void> abort(Throwable failure) {
            if (aborted || session == null) {
                aborted = true;
                return Uni.createFrom().voidItem();
            }
            aborted = true;
            return toUni(session.abort(failure)).replaceWithVoid();
        }

        private Uni<Void> writeChunk(ObjectPayloadChunk chunk) {
            byte[] data = chunk == null ? new byte[0] : chunk.bytes();
            if (data.length == 0) {
                return Uni.createFrom().voidItem();
            }
            digest.update(data);
            bytes += data.length;
            return open().chain(session -> toUni(session.write(java.nio.ByteBuffer.wrap(data))).replaceWithVoid());
        }

        private Uni<ObjectWriteSession> open() {
            if (session != null) {
                return Uni.createFrom().item(session);
            }
            if (openSession == null) {
                openSession = toUni(provider.open(openRequest))
                    .invoke(opened -> {
                        if (opened == null) {
                            throw new IllegalStateException("Object target provider returned null write session");
                        }
                        session = opened;
                    })
                    .memoize().indefinitely();
            }
            return openSession;
        }

        private Map<String, String> finalMetadata() {
            Map<String, String> metadata = new LinkedHashMap<>(initialMetadata);
            metadata.putAll(safeMap(renderer.finalMetadata(), "final metadata"));
            return metadata;
        }

        private String checksum() {
            return HexFormat.of().formatHex(digest.digest());
        }
    }

    private static Map<String, String> safeMap(Map<String, String> values, String label) {
        if (values == null || values.isEmpty()) {
            return Map.of();
        }
        Map<String, String> copy = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                throw new IllegalStateException("Object publish " + label + " must not contain null keys or values");
            }
            copy.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(copy);
    }
}
