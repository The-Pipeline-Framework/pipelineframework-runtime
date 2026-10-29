package org.pipelineframework.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/** Immutable release-bound encoder/decoder plan; construction is not a steady-state payload operation. */
public record CompiledDurablePayloadPlan(
    CanonicalPayloadBinding binding,
    ObjectWriter writer,
    ObjectReader reader,
    Map<String, UnionCasePlan> unionCases,
    Optional<MethodHandle> discriminator
) {
    public record UnionCasePlan(Class<?> runtimeClass, ObjectWriter writer, ObjectReader reader) {
    }

    public static CompiledDurablePayloadPlan compile(CanonicalPayloadBinding binding) {
        ObjectMapper mapper = CanonicalUnionJson.mapper();
        Map<String, UnionCasePlan> unionCases = unionCases(binding.runtimeClass(), mapper);
        return new CompiledDurablePayloadPlan(
            binding,
            mapper.writerFor(binding.runtimeType()),
            mapper.readerFor(binding.runtimeType())
                .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS),
            unionCases,
            discriminator(binding.runtimeClass(), unionCases));
    }

    public boolean isUnion() {
        return !unionCases.isEmpty();
    }

    private static Map<String, UnionCasePlan> unionCases(Class<?> runtimeClass, ObjectMapper mapper) {
        if (!runtimeClass.isSealed() || runtimeClass.isEnum()) {
            return Map.of();
        }
        Map<String, UnionCasePlan> cases = new TreeMap<>();
        for (Class<?> candidate : runtimeClass.getPermittedSubclasses()) {
            cases.put(candidate.getSimpleName(), new UnionCasePlan(
                candidate,
                mapper.writerFor(candidate),
                mapper.readerFor(candidate)));
        }
        return Map.copyOf(cases);
    }

    private static Optional<MethodHandle> discriminator(Class<?> runtimeClass, Map<String, UnionCasePlan> unionCases) {
        if (unionCases.isEmpty()) {
            return Optional.empty();
        }
        try {
            Method method = runtimeClass.getMethod("discriminator");
            return Optional.of(MethodHandles.privateLookupIn(runtimeClass, MethodHandles.lookup()).unreflect(method));
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Canonical union " + runtimeClass.getName()
                + " does not expose its authored discriminator", e);
        }
    }
}
