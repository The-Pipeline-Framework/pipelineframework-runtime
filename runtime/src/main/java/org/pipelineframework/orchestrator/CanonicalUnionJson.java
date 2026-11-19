package org.pipelineframework.orchestrator;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Codec-local polymorphism: Jackson owns object/container traversal, declared sealed types own case admission. */
final class CanonicalUnionJson {
    private CanonicalUnionJson() {
    }

    static ObjectMapper mapper() {
        return mapper(Map.of());
    }

    static ObjectMapper mapper(Map<String, Map<String, Object>> definitions) {
        ObjectMapper mapper = PipelineJson.mapper().copy();
        mapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        mapper.registerModule(new com.fasterxml.jackson.databind.module.SimpleModule("durable-canonical-unions") {
            @Override
            public void setupModule(SetupContext context) {
                super.setupModule(context);
                context.insertAnnotationIntrospector(new NopAnnotationIntrospector() {
                @Override
                public TypeResolverBuilder<?> findTypeResolver(
                    MapperConfig<?> config, AnnotatedClass annotated, JavaType baseType
                ) {
                    // Presence has its own published serializer and is not a canonical union case envelope.
                    if (baseType.getRawClass() == org.pipelineframework.type.CanonicalFieldValue.class) {
                        return super.findTypeResolver(config, annotated, baseType);
                    }
                    if (baseType.getRawClass().isSealed() && !baseType.getRawClass().isEnum()) {
                        return new UnionResolver(definitions);
                    }
                    // Canonical concrete values must not inherit application Jackson type envelopes.
                    return StdTypeResolverBuilder.noTypeInfoBuilder();
                }
                });
            }
        });
        return mapper;
    }

    private record Union(JavaType type, Map<String, Class<?>> cases, Optional<MethodHandle> discriminator,
                         Map<Class<?>, String> compiledDiscriminators) {
        static Union of(JavaType type, Map<String, Map<String, Object>> definitions) {
            // Jackson's dynamic case serializer erases a generic union's parameters. Fail before writing lossy JSON.
            // Generic records and containers whose values are non-generic unions remain supported.
            if (type.hasGenericTypes() || type.getRawClass().getTypeParameters().length > 0) {
                throw new IllegalArgumentException("Parameterized canonical unions are not supported: " + type);
            }
            Map<String, Class<?>> cases = new TreeMap<>();
            for (Class<?> candidate : type.getRawClass().getPermittedSubclasses()) {
                if (candidate.getTypeParameters().length > 0) {
                    throw new IllegalArgumentException("Parameterized canonical union cases are not supported: " + candidate);
                }
                if (cases.put(candidate.getSimpleName(), candidate) != null) {
                    throw new IllegalArgumentException("Ambiguous canonical union case in " + type);
                }
            }
            Optional<MethodHandle> discriminator;
            try {
                Method method = type.getRawClass().getMethod("discriminator");
                if (method.getReturnType() != String.class || method.getParameterCount() != 0) {
                    throw new IllegalArgumentException("Canonical union discriminator must return String: " + type);
                }
                discriminator = Optional.of(MethodHandles.privateLookupIn(type.getRawClass(), MethodHandles.lookup())
                    .unreflect(method));
            } catch (NoSuchMethodException e) {
                discriminator = Optional.empty();
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Cannot access canonical union discriminator: " + type, e);
            }
            return new Union(type, Map.copyOf(cases), discriminator, compiledDiscriminators(type, cases, definitions));
        }

        private static Map<Class<?>, String> compiledDiscriminators(
            JavaType type, Map<String, Class<?>> cases, Map<String, Map<String, Object>> definitions
        ) {
            Map<Class<?>, String> labels = new java.util.HashMap<>();
            for (var definition : definitions.values()) {
                if (!type.getRawClass().getName().equals(definition.get("runtimeClass"))
                    || !(definition.get("definition") instanceof Map<?, ?> schema)
                    || !"union".equals(schema.get("kind"))
                    || !(schema.get("variants") instanceof java.util.List<?> variants)) continue;
                for (Object raw : variants) {
                    if (!(raw instanceof Map<?, ?> variant)
                        || !(variant.get("discriminator") instanceof String label)
                        || !(variant.get("payload") instanceof Map<?, ?> payload)
                        || !(payload.get("id") instanceof String payloadId)) {
                        throw new IllegalArgumentException("Incomplete compiled union variant for " + type);
                    }
                    var target = definitions.get(payloadId);
                    if (target == null) throw new IllegalArgumentException("Missing compiled union payload " + payloadId);
                    for (Class<?> candidate : cases.values()) {
                        if (!candidate.getName().equals(target.get("runtimeClass"))) continue;
                        String previous = labels.putIfAbsent(candidate, label);
                        if (previous != null && !previous.equals(label)) {
                            throw new IllegalArgumentException("Ambiguous compiled discriminator for " + candidate.getName());
                        }
                    }
                }
            }
            return Map.copyOf(labels);
        }

        String discriminator(Object value) throws IOException {
            String compiled = compiledDiscriminators.get(value.getClass());
            if (compiled != null) return compiled;
            try {
                String result = discriminator.isPresent()
                    ? (String) discriminator.orElseThrow().invoke(value) : value.getClass().getSimpleName();
                if (result == null || result.isBlank()) {
                    throw new IllegalArgumentException("Canonical union discriminator must not be blank");
                }
                return result;
            } catch (Error e) {
                throw e;
            } catch (Throwable e) {
                throw new IOException("Failed resolving canonical union discriminator for " + type, e);
            }
        }
    }

    private static final class UnionResolver extends StdTypeResolverBuilder {
        private final Map<String, Map<String, Object>> definitions;

        private UnionResolver(Map<String, Map<String, Object>> definitions) {
            this.definitions = definitions;
        }
        @Override
        public TypeSerializer buildTypeSerializer(
            SerializationConfig config, JavaType baseType, Collection<NamedType> subtypes
        ) {
            return new UnionSerializer(Union.of(baseType, definitions));
        }

        @Override
        public TypeDeserializer buildTypeDeserializer(
            DeserializationConfig config, JavaType baseType, Collection<NamedType> subtypes
        ) {
            return new UnionDeserializer(Union.of(baseType, definitions));
        }
    }

    private static final class UnionSerializer extends TypeSerializer {
        private final Union union;

        private UnionSerializer(Union union) {
            this.union = union;
        }

        @Override public TypeSerializer forProperty(BeanProperty property) { return this; }
        @Override public JsonTypeInfo.As getTypeInclusion() { return JsonTypeInfo.As.WRAPPER_OBJECT; }
        @Override public String getPropertyName() { return "case"; }
        @Override public TypeIdResolver getTypeIdResolver() { return null; }

        @Override
        public WritableTypeId writeTypePrefix(JsonGenerator generator, WritableTypeId id) throws IOException {
            Object value = id.forValue;
            String caseName = value.getClass().getSimpleName();
            if (union.cases().get(caseName) != value.getClass()) {
                throw JsonMappingException.from(generator, "Undeclared canonical union case for " + union.type());
            }
            generator.writeStartObject();
            generator.writeStringField("discriminator", union.discriminator(value));
            generator.writeStringField("case", caseName);
            generator.writeFieldName("value");
            if (id.valueShape == JsonToken.START_OBJECT) {
                generator.writeStartObject(value);
            } else if (id.valueShape == JsonToken.START_ARRAY) {
                generator.writeStartArray(value);
            }
            return id;
        }

        @Override
        public WritableTypeId writeTypeSuffix(JsonGenerator generator, WritableTypeId id) throws IOException {
            if (id.valueShape == JsonToken.START_OBJECT) {
                generator.writeEndObject();
            } else if (id.valueShape == JsonToken.START_ARRAY) {
                generator.writeEndArray();
            }
            generator.writeEndObject();
            return id;
        }
    }

    private static final class UnionDeserializer extends TypeDeserializer {
        private final Union union;

        private UnionDeserializer(Union union) {
            this.union = union;
        }

        @Override public TypeDeserializer forProperty(BeanProperty property) { return this; }
        @Override public JsonTypeInfo.As getTypeInclusion() { return JsonTypeInfo.As.WRAPPER_OBJECT; }
        @Override public String getPropertyName() { return "case"; }
        @Override public TypeIdResolver getTypeIdResolver() { return null; }
        @Override public Class<?> getDefaultImpl() { return null; }
        @Override public Object deserializeTypedFromObject(JsonParser p, DeserializationContext c) throws IOException {
            return deserializeTypedFromAny(p, c);
        }
        @Override public Object deserializeTypedFromArray(JsonParser p, DeserializationContext c) throws IOException {
            return deserializeTypedFromAny(p, c);
        }
        @Override public Object deserializeTypedFromScalar(JsonParser p, DeserializationContext c) throws IOException {
            return deserializeTypedFromAny(p, c);
        }

        @Override
        public Object deserializeTypedFromAny(JsonParser parser, DeserializationContext context) throws IOException {
            JsonNode envelope = parser.readValueAsTree();
            JsonNode caseNode = envelope.get("case");
            JsonNode discriminator = envelope.get("discriminator");
            JsonNode valueNode = envelope.get("value");
            if (!envelope.isObject() || envelope.size() != 3 || caseNode == null || !caseNode.isTextual()
                || discriminator == null || !discriminator.isTextual() || valueNode == null || valueNode.isNull()) {
                throw JsonMappingException.from(parser, "Malformed canonical union durable payload for " + union.type());
            }
            Class<?> candidate = union.cases().get(caseNode.textValue());
            if (candidate == null) {
                throw JsonMappingException.from(parser, "Unknown canonical union case '" + caseNode.textValue()
                    + "' for " + union.type());
            }
            JavaType caseType = context.getTypeFactory().constructSpecializedType(union.type(), candidate);
            Object value;
            try (JsonParser valueParser = valueNode.traverse(parser.getCodec())) {
                valueParser.nextToken();
                value = context.readValue(valueParser, caseType);
            }
            if (value == null || value.getClass() != candidate
                || !discriminator.textValue().equals(union.discriminator(value))) {
                throw JsonMappingException.from(parser, "Canonical union discriminator mismatch for " + union.type());
            }
            return value;
        }
    }
}
