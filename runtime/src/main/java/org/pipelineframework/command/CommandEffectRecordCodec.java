package org.pipelineframework.command;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.pipelineframework.connector.CommandRecoveryBinding;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.filter.FilteringParserDelegate;
import com.fasterxml.jackson.core.filter.JsonPointerBasedFilter;
import com.fasterxml.jackson.core.filter.TokenFilter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Versioned, type-preserving durable representation of a Command effect record. */
final class CommandEffectRecordCodec {
    static final int SCHEMA_VERSION = 3;
    private static final int OCCURRENCE_SCHEMA_VERSION = 2;
    private static final int LEGACY_SCHEMA_VERSION = 1;

    private static final String JSON_ENCODING = "json";
    private static final String PROTOBUF_JSON_ENCODING = "protobuf-json";
    private static final JsonFormat.Printer PROTOBUF_PRINTER =
        JsonFormat.printer().omittingInsignificantWhitespace();

    private final ObjectMapper json;
    private final ClassLoader classLoader;

    CommandEffectRecordCodec() {
        this(PipelineJson.mapper(), Thread.currentThread().getContextClassLoader());
    }

    CommandEffectRecordCodec(ObjectMapper json, ClassLoader classLoader) {
        this.json = Objects.requireNonNull(json, "json mapper must not be null");
        this.classLoader = classLoader == null ? CommandEffectRecordCodec.class.getClassLoader() : classLoader;
    }

    String encode(CommandEffectRecord record, String inputDeclaredType, String outputDeclaredType) {
        Objects.requireNonNull(record, "command effect record must not be null");
        requireText(inputDeclaredType, "input declared type");
        requireText(outputDeclaredType, "output declared type");
        validateRecovery(record, inputDeclaredType, outputDeclaredType);
        PersistedSnapshotV3 snapshot = new PersistedSnapshotV3(
            SCHEMA_VERSION,
            record.tenantId(),
            record.executionId(),
            record.stepId(),
            record.command(),
            record.commandId(),
            record.status(),
            encodeValue(record.input(), inputDeclaredType),
            encodeValue(record.output(), outputDeclaredType),
            Optional.ofNullable(record.errorClass()),
            Optional.ofNullable(record.errorMessage()),
            record.outcome(),
            record.attempts().stream()
                .map(attempt -> encodeAttempt(attempt, outputDeclaredType))
                .toList(),
            record.createdAtEpochMs(),
            record.updatedAtEpochMs(),
            inputDeclaredType,
            outputDeclaredType);
        try {
            return json.writeValueAsString(snapshot);
        } catch (IOException failure) {
            throw new CommandEffectStoreException(
                "Failed encoding Command effect " + record.commandId(), failure);
        }
    }

    DecodedSnapshot decode(String encoded) {
        requireText(encoded, "encoded command effect record");
        JsonNode root;
        try {
            root = json.reader().with(StreamReadFeature.STRICT_DUPLICATE_DETECTION).readTree(encoded);
        } catch (IOException | RuntimeException failure) {
            throw new CommandEffectStoreException("Failed decoding durable Command effect record", failure);
        }
        int schemaVersion = root.path("schemaVersion").asInt(-1);
        if (!supportsSchemaVersion(schemaVersion)) {
            throw new CommandEffectStoreException(
                "Unsupported durable Command effect schema version " + schemaVersion);
        }
        if (schemaVersion < SCHEMA_VERSION
            && java.util.stream.StreamSupport.stream(root.path("attempts").spliterator(), false)
                .anyMatch(attempt -> !attempt.path("recoveryBinding").isMissingNode() && !attempt.path("recoveryBinding").isNull()
                    || !attempt.path("reconciliationReceipt").isMissingNode() && !attempt.path("reconciliationReceipt").isNull())) {
            throw new CommandEffectStoreException("Older Command schema cannot acquire recovery metadata");
        }
        try {
            return schemaVersion == LEGACY_SCHEMA_VERSION
                ? decodeLegacy(json.treeToValue(root, PersistedSnapshotV1.class), encoded)
                : decodeCurrent(json.treeToValue(root, PersistedSnapshotV3.class), encoded);
        } catch (CommandEffectStoreException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw new CommandEffectStoreException("Invalid durable Command effect record", failure);
        }
    }

    static boolean supportsSchemaVersion(int schemaVersion) {
        return schemaVersion == LEGACY_SCHEMA_VERSION || schemaVersion == OCCURRENCE_SCHEMA_VERSION
            || schemaVersion == SCHEMA_VERSION;
    }

    private DecodedSnapshot decodeCurrent(PersistedSnapshotV3 snapshot, String encoded) {
        requireText(snapshot.inputDeclaredType(), "input declared type");
        requireText(snapshot.outputDeclaredType(), "output declared type");
        try {
            CommandEffectRecord record = new CommandEffectRecord(
                snapshot.tenantId(),
                snapshot.executionId(),
                snapshot.stepId(),
                snapshot.command(),
                snapshot.commandId(),
                Objects.requireNonNull(snapshot.status(), "command effect status must not be null"),
                decodeValue(requireOptional(snapshot.input(), "input value"), snapshot.inputDeclaredType(), encoded, "/input/value")
                    .orElse(null),
                decodeValue(requireOptional(snapshot.output(), "output value"), snapshot.outputDeclaredType(), encoded, "/output/value")
                    .orElse(null),
                requireOptional(snapshot.errorClass(), "error class").orElse(null),
                requireOptional(snapshot.errorMessage(), "error message").orElse(null),
                requireOptional(snapshot.outcome(), "outcome snapshot"),
                java.util.stream.IntStream.range(0, snapshot.attempts().size())
                    .mapToObj(index -> decodeAttempt(snapshot.attempts().get(index), snapshot.outputDeclaredType(), encoded, index))
                    .toList(),
                snapshot.createdAtEpochMs(),
                snapshot.updatedAtEpochMs());
            validateRecovery(record, snapshot.inputDeclaredType(), snapshot.outputDeclaredType());
            return new DecodedSnapshot(record, snapshot.inputDeclaredType(), snapshot.outputDeclaredType());
        } catch (CommandEffectStoreException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw new CommandEffectStoreException("Invalid durable Command effect record", failure);
        }
    }

    private DecodedSnapshot decodeLegacy(PersistedSnapshotV1 snapshot, String encoded) {
        requireText(snapshot.inputDeclaredType(), "input declared type");
        requireText(snapshot.outputDeclaredType(), "output declared type");
        Object currentOutput = decodeValue(
            requireOptional(snapshot.output(), "output value"), snapshot.outputDeclaredType(), encoded, "/output/value").orElse(null);
        List<CommandEffectAttemptRecord> attempts = new java.util.ArrayList<>();
        for (PersistedAttemptSnapshotV1 attempt : snapshot.attempts()) {
            boolean current = attempt.attemptNumber() == snapshot.attempts().size();
            attempts.add(new CommandEffectAttemptRecord(
                attempt.attemptId(),
                snapshot.commandId(),
                attempt.attemptNumber(),
                attempt.executionId(),
                attempt.attemptNumber() == 1 ? CommandAttemptPurpose.INITIAL : CommandAttemptPurpose.RETRY,
                attempt.status(),
                current && attempt.status() == CommandEffectStatus.SUCCEEDED
                    ? Optional.ofNullable(currentOutput)
                    : Optional.empty(),
                requireOptional(attempt.errorClass(), "attempt error class").orElse(null),
                requireOptional(attempt.errorMessage(), "attempt error message").orElse(null),
                requireOptional(attempt.outcome(), "attempt outcome snapshot"),
                Optional.empty(),
                attempt.createdAtEpochMs(),
                attempt.updatedAtEpochMs()));
        }
        CommandEffectRecord record = new CommandEffectRecord(
            snapshot.tenantId(), snapshot.executionId(), snapshot.stepId(), snapshot.command(),
            snapshot.commandId(), Objects.requireNonNull(snapshot.status(), "command effect status must not be null"),
            decodeValue(requireOptional(snapshot.input(), "input value"), snapshot.inputDeclaredType(), encoded, "/input/value").orElse(null),
            currentOutput,
            requireOptional(snapshot.errorClass(), "error class").orElse(null),
            requireOptional(snapshot.errorMessage(), "error message").orElse(null),
            requireOptional(snapshot.outcome(), "outcome snapshot"),
            attempts,
            snapshot.createdAtEpochMs(),
            snapshot.updatedAtEpochMs());
        return new DecodedSnapshot(record, snapshot.inputDeclaredType(), snapshot.outputDeclaredType());
    }

    private PersistedAttemptSnapshotV3 encodeAttempt(
        CommandEffectAttemptRecord attempt,
        String outputDeclaredType
    ) {
        return new PersistedAttemptSnapshotV3(
            attempt.attemptId(),
            attempt.occurrenceId(),
            attempt.attemptNumber(),
            attempt.executionId(),
            attempt.purpose(),
            attempt.status(),
            encodeValue(attempt.output().orElse(null), outputDeclaredType),
            Optional.ofNullable(attempt.errorClass()),
            Optional.ofNullable(attempt.errorMessage()),
            attempt.outcome(),
            attempt.reason(),
            attempt.createdAtEpochMs(),
            attempt.updatedAtEpochMs(),
            attempt.recoveryBinding(),
            attempt.reconciliationReceipt());
    }

    private CommandEffectAttemptRecord decodeAttempt(
        PersistedAttemptSnapshotV3 attempt,
        String outputDeclaredType,
        String encoded,
        int index
    ) {
        return new CommandEffectAttemptRecord(
            attempt.attemptId(),
            attempt.occurrenceId(),
            attempt.attemptNumber(),
            attempt.executionId(),
            attempt.purpose(),
            attempt.status(),
            decodeValue(requireOptional(attempt.output(), "attempt output"), outputDeclaredType, encoded, "/attempts/" + index + "/output/value"),
            requireOptional(attempt.errorClass(), "attempt error class").orElse(null),
            requireOptional(attempt.errorMessage(), "attempt error message").orElse(null),
            requireOptional(attempt.outcome(), "attempt outcome snapshot"),
            requireOptional(attempt.reason(), "attempt reason"),
            attempt.createdAtEpochMs(),
            attempt.updatedAtEpochMs(),
            attempt.recoveryBinding() == null ? Optional.empty() : attempt.recoveryBinding(),
            attempt.reconciliationReceipt() == null ? Optional.empty() : attempt.reconciliationReceipt());
    }

    /** Stable digest of the exact typed durable value, with recursively sorted object members. */
    String digest(Object value, String declaredType) {
        resolve(declaredType); // Recovery never accepts an unresolved declared type.
        try {
            var envelope = json.createObjectNode();
            envelope.put("declaredType", declaredType);
            envelope.set("typedValue", json.valueToTree(encodeValue(value, declaredType)));
            byte[] encoded = json.writeValueAsString(canonical(envelope)).getBytes(StandardCharsets.UTF_8);
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(encoded));
        } catch (CommandEffectStoreException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new CommandEffectStoreException("Failed computing typed Command value digest", failure);
        }
    }

    private JsonNode canonical(JsonNode value) {
        if (value.isObject()) {
            var result = json.createObjectNode();
            java.util.TreeSet<String> names = new java.util.TreeSet<>();
            value.fieldNames().forEachRemaining(names::add);
            names.forEach(name -> result.set(name, canonical(value.get(name))));
            return result;
        }
        if (value.isArray()) {
            var result = json.createArrayNode();
            value.forEach(item -> result.add(canonical(item)));
            return result;
        }
        return value;
    }

    void validateRecovery(CommandEffectRecord record, String inputType, String outputType) {
        for (CommandEffectAttemptRecord attempt : record.attempts()) {
            if (attempt.recoveryBinding().isEmpty()) {
                continue;
            }
            CommandRecoveryBinding binding = attempt.recoveryBinding().orElseThrow();
            String namedCommand = "native-binding:" + binding.binding().value() + "/" + binding.operationIdentity().operationId();
            String providerCommand = "native:" + binding.operationIdentity().providerId().value() + "/" + binding.operationIdentity().operationId();
            if (!binding.inputType().equals(inputType) || !binding.outputType().equals(outputType)
                || (!record.command().equals(namedCommand) && !record.command().equals(providerCommand))
                || !binding.inputDigest().equals(digest(record.input(), inputType))) {
                throw new CommandEffectStoreException("Durable Command recovery binding does not match original typed input");
            }
            if (attempt.reconciliationReceipt().isPresent()
                && !attempt.reconciliationReceipt().orElseThrow().outputDigest()
                    .equals(digest(attempt.output().orElseThrow(), outputType))) {
                throw new CommandEffectStoreException("Durable Command receipt does not match typed output");
            }
            if (attempt.outcome().isPresent()) {
                CommandOutcomeSnapshot outcome = attempt.outcome().orElseThrow();
                if (!outcome.operationIdentity().equals(binding.operationIdentity())
                    || outcome.providerMajorVersion() != binding.providerMajorVersion()
                    || !outcome.configuration().equals(binding.operationConfiguration())) {
                    throw new CommandEffectStoreException("Durable Command outcome does not match original provider/configuration");
                }
            }
        }
        if (record.currentAttempt().recoveryBinding().isPresent() && record.status() == CommandEffectStatus.SUCCEEDED
            && (!record.outcome().equals(record.currentAttempt().outcome())
                || !digest(record.output(), outputType).equals(digest(record.currentAttempt().output().orElseThrow(), outputType)))) {
            throw new CommandEffectStoreException("Durable Command success conflicts with current attempt output/outcome");
        }
    }

    private Optional<TypedValueSnapshot> encodeValue(Object value, String declaredType) {
        if (value == null) {
            return Optional.empty();
        }
        String runtimeType = value.getClass().getName();
        verifyDeclaredType(declaredType, value.getClass());
        try {
            if (value instanceof MessageOrBuilder protobuf) {
                return Optional.of(new TypedValueSnapshot(
                    runtimeType,
                    PROTOBUF_JSON_ENCODING,
                    json.readTree(PROTOBUF_PRINTER.print(protobuf))));
            }
            return Optional.of(new TypedValueSnapshot(runtimeType, JSON_ENCODING, json.valueToTree(value)));
        } catch (Exception failure) {
            throw new CommandEffectStoreException("Failed encoding durable value of type " + runtimeType, failure);
        }
    }

    private Optional<Object> decodeValue(Optional<TypedValueSnapshot> snapshotValue, String declaredType, String encoded, String pointer) {
        if (snapshotValue.isEmpty()) {
            return Optional.empty();
        }
        TypedValueSnapshot snapshot = snapshotValue.orElseThrow();
        requireText(snapshot.runtimeType(), "runtime type");
        requireText(snapshot.encoding(), "value encoding");
        if (snapshot.value() == null) {
            throw new CommandEffectStoreException("Durable typed value is missing its JSON value");
        }
        Class<?> runtimeType = resolve(snapshot.runtimeType());
        verifyDeclaredType(declaredType, runtimeType);
        try {
            return Optional.of(switch (snapshot.encoding()) {
                // Read typed JSON directly: an intermediate JsonNode would round decimal timestamps through Double.
                // For container JsonNode types, preserve exact numeric token representations so both
                // 29-digit decimals and IEEE-754 double forms (-0.0, 1.0E7, 1.0E-4) reproduce identical digests.
                case JSON_ENCODING -> decodeJsonValue(runtimeType, encoded, pointer);
                case PROTOBUF_JSON_ENCODING -> decodeProtobuf(snapshot.value(), runtimeType);
                default -> throw new CommandEffectStoreException(
                    "Unsupported durable Command value encoding " + snapshot.encoding());
            });
        } catch (CommandEffectStoreException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new CommandEffectStoreException(
                "Failed decoding durable value as " + runtimeType.getName(), failure);
        }
    }

    private Object decodeJsonValue(Class<?> runtimeType, String encoded, String pointer) throws IOException {
        if (runtimeType == JsonNode.class || runtimeType == ObjectNode.class || runtimeType == ArrayNode.class) {
            return decodeExactJsonNode(encoded, pointer, runtimeType);
        }
        if (runtimeType == DecimalNode.class) {
            return json.readerFor(runtimeType)
                .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .at(pointer)
                .readValue(encoded);
        }
        return json.readerFor(runtimeType).at(pointer).readValue(encoded);
    }

    private JsonNode decodeExactJsonNode(String encoded, String pointer, Class<?> runtimeType) throws IOException {
        try (JsonParser raw = json.getFactory().createParser(encoded);
             JsonParser filtered = new FilteringParserDelegate(
                 raw, new JsonPointerBasedFilter(pointer), TokenFilter.Inclusion.ONLY_INCLUDE_ALL, false)) {
            JsonToken token = filtered.nextToken();
            if (token == null) {
                throw new IllegalArgumentException("Missing JSON value at pointer " + pointer);
            }
            JsonNode decoded = readExactNode(filtered, token);
            if (!runtimeType.isInstance(decoded)) {
                throw new IllegalArgumentException(
                    "Expected " + runtimeType.getName() + " at " + pointer + " but decoded " + decoded.getClass().getName());
            }
            return decoded;
        }
    }

    private JsonNode readExactNode(JsonParser parser, JsonToken token) throws IOException {
        JsonNodeFactory factory = json.getNodeFactory();
        return switch (token) {
            case START_OBJECT -> {
                ObjectNode obj = factory.objectNode();
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldName = parser.currentName();
                    JsonToken valueToken = parser.nextToken();
                    obj.set(fieldName, readExactNode(parser, valueToken));
                }
                yield obj;
            }
            case START_ARRAY -> {
                ArrayNode arr = factory.arrayNode();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    arr.add(readExactNode(parser, parser.currentToken()));
                }
                yield arr;
            }
            case VALUE_STRING -> factory.textNode(parser.getText());
            case VALUE_NUMBER_INT -> switch (parser.getNumberType()) {
                case INT -> factory.numberNode(parser.getIntValue());
                case LONG -> factory.numberNode(parser.getLongValue());
                default -> factory.numberNode(parser.getBigIntegerValue());
            };
            case VALUE_NUMBER_FLOAT -> decodeExactFloatingNode(factory, parser.getText());
            case VALUE_TRUE -> factory.booleanNode(true);
            case VALUE_FALSE -> factory.booleanNode(false);
            case VALUE_NULL -> factory.nullNode();
            default -> throw new IllegalArgumentException("Unexpected JSON token " + token);
        };
    }

    private static JsonNode decodeExactFloatingNode(JsonNodeFactory factory, String rawText) {
        BigDecimal exact = new BigDecimal(rawText);
        if (isNegativeZeroText(rawText, exact) || isCanonicalDoubleExponentText(rawText)) {
            return factory.numberNode(Double.parseDouble(rawText));
        }
        return factory.numberNode(exact);
    }

    private static boolean isNegativeZeroText(String rawText, BigDecimal exact) {
        return exact.signum() == 0 && "-0.0".equals(rawText);
    }

    private static boolean isCanonicalDoubleExponentText(String rawText) {
        if (rawText.indexOf('e') < 0 && rawText.indexOf('E') < 0) {
            return false;
        }
        double parsed = Double.parseDouble(rawText);
        return Double.isFinite(parsed) && Double.toString(parsed).equals(rawText);
    }

    private Object decodeProtobuf(JsonNode value, Class<?> runtimeType) throws Exception {
        if (!Message.class.isAssignableFrom(runtimeType)) {
            throw new CommandEffectStoreException(
                "Durable protobuf value declares non-protobuf type " + runtimeType.getName());
        }
        Message.Builder builder = (Message.Builder) runtimeType.getMethod("newBuilder").invoke(null);
        JsonFormat.parser().merge(json.writeValueAsString(value), builder);
        return builder.build();
    }

    private void verifyDeclaredType(String declaredType, Class<?> runtimeType) {
        Optional<Class<?>> resolved = tryResolve(declaredType);
        if (resolved.isPresent() && !resolved.orElseThrow().isAssignableFrom(runtimeType)) {
            throw new CommandEffectStoreException(
                "Durable value type " + runtimeType.getName()
                    + " is incompatible with declared Command type " + declaredType);
        }
    }

    private Optional<Class<?>> tryResolve(String className) {
        if (className == null || className.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(resolveClass(className));
        } catch (ClassNotFoundException ignored) {
            return Optional.empty();
        }
    }

    private Class<?> resolve(String className) {
        try {
            return resolveClass(className);
        } catch (ClassNotFoundException failure) {
            throw new CommandEffectStoreException(
                "Durable Command value type is not available: " + className, failure);
        }
    }

    private Class<?> resolveClass(String className) throws ClassNotFoundException {
        try {
            return classLoader.loadClass(className);
        } catch (ClassNotFoundException original) {
            String[] segments = className.split("\\.");
            for (int packageSegments = segments.length - 2; packageSegments >= 1; packageSegments--) {
                String packageName = String.join(".", Arrays.copyOfRange(segments, 0, packageSegments));
                String nestedTypeName = String.join("$", Arrays.copyOfRange(segments, packageSegments, segments.length));
                try {
                    return classLoader.loadClass(packageName + "." + nestedTypeName);
                } catch (ClassNotFoundException ignored) {
                    // Try the next possible package/type boundary.
                }
            }
            throw original;
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new CommandEffectStoreException(field + " must not be blank");
        }
    }

    private static <T> Optional<T> requireOptional(Optional<T> value, String field) {
        if (value == null) {
            throw new CommandEffectStoreException("Durable Command effect is missing " + field + " metadata");
        }
        return value;
    }

    record DecodedSnapshot(
        CommandEffectRecord record,
        String inputDeclaredType,
        String outputDeclaredType
    ) {
    }

    private record TypedValueSnapshot(
        String runtimeType,
        String encoding,
        JsonNode value
    ) {
    }

    private record PersistedAttemptSnapshotV3(
        String attemptId,
        String occurrenceId,
        int attemptNumber,
        String executionId,
        CommandAttemptPurpose purpose,
        CommandEffectStatus status,
        Optional<TypedValueSnapshot> output,
        Optional<String> errorClass,
        Optional<String> errorMessage,
        Optional<CommandOutcomeSnapshot> outcome,
        Optional<String> reason,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        Optional<CommandRecoveryBinding> recoveryBinding,
        Optional<CommandReconciliationReceipt> reconciliationReceipt
    ) {
    }

    private record PersistedSnapshotV3(
        int schemaVersion,
        String tenantId,
        String executionId,
        String stepId,
        String command,
        String commandId,
        CommandEffectStatus status,
        Optional<TypedValueSnapshot> input,
        Optional<TypedValueSnapshot> output,
        Optional<String> errorClass,
        Optional<String> errorMessage,
        Optional<CommandOutcomeSnapshot> outcome,
        List<PersistedAttemptSnapshotV3> attempts,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        String inputDeclaredType,
        String outputDeclaredType
    ) {
    }

    private record PersistedAttemptSnapshotV1(
        String attemptId,
        int attemptNumber,
        String executionId,
        CommandEffectStatus status,
        Optional<String> errorClass,
        Optional<String> errorMessage,
        Optional<CommandOutcomeSnapshot> outcome,
        long createdAtEpochMs,
        long updatedAtEpochMs
    ) {
    }

    private record PersistedSnapshotV1(
        int schemaVersion,
        String tenantId,
        String executionId,
        String stepId,
        String command,
        String commandId,
        CommandEffectStatus status,
        Optional<TypedValueSnapshot> input,
        Optional<TypedValueSnapshot> output,
        Optional<String> errorClass,
        Optional<String> errorMessage,
        Optional<CommandOutcomeSnapshot> outcome,
        List<PersistedAttemptSnapshotV1> attempts,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        String inputDeclaredType,
        String outputDeclaredType
    ) {
    }
}
