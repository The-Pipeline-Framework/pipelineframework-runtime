package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.pipeline.PipelineOrderResourceLoader;
import org.pipelineframework.config.pipeline.PipelineOrderResourceLoader.StepBoundary;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.invocation.PipelineInvocationPath;

/** Selects exact canonical boundaries from this worker's compiler-emitted physical order. */
public final class CompiledTransitionBoundaryCodec implements TransitionPayloadCodec {
    private final TransitionPayloadCodec framing;
    private final CompiledCanonicalTransitionPayloadCodec values;
    private final PipelineContractDescriptor contract;
    private final String input;
    private final boolean initial;

    public CompiledTransitionBoundaryCodec(TransitionPayloadCodec framing, PipelineContractDescriptor contract,
        TransitionCommandEnvelope command) {
        this.framing = framing;
        this.contract = contract;
        List<StepBoundary> root = PipelineOrderResourceLoader.loadBoundaries().orElseThrow(() ->
            new IllegalStateException("Canonical transitions require compiled physical boundary metadata"));
        int current = command.currentStepIndex();
        int stop = command.stopBeforeStepIndex() < 0 ? root.size() : command.stopBeforeStepIndex();
        if (current < 0 || current > root.size() || stop < current || stop > root.size()) {
            throw new IllegalArgumentException("Transition cursor differs from compiled physical order");
        }
        Object framed = framing.decode(new SerializedTransitionPayload(command.payloadTypeId(), command.payloadEncoding(), command.payload()));
        Optional<PipelineInvocationPath> path = framed instanceof ExecutionInputSnapshot snapshot ? snapshot.invocationPath() : Optional.empty();
        List<StepBoundary> receiving = root;
        int inputIndex = current;
        if (path.filter(value -> !value.frames().isEmpty()).isPresent()) {
            var retained = path.orElseThrow(); retained.requireResumable();
            var definitions = PipelineOrderResourceLoader.loadDefinitionBoundaries();
            if (retained.frames().getFirst().callerStepIndex() != current) {
                throw new IllegalArgumentException("Invocation path differs from transition root cursor");
            }
            for (var frame : retained.frames()) {
                if (frame.callerStepIndex() < 0 || frame.callerStepIndex() >= receiving.size()) {
                    throw new IllegalArgumentException("Invocation caller differs from compiled physical order");
                }
                String invocationClass = receiving.get(frame.callerStepIndex()).stepClass();
                var matches = definitions.stream().filter(definition -> invocationClass.equals(definition.invocationClass())
                    && frame.definitionId().equals(definition.definitionId()) && frame.callsiteId().equals(definition.callsiteId())).toList();
                if (matches.size() != 1) throw new IllegalStateException("Invocation has no exact compiled physical boundary");
                receiving = matches.getFirst().stepBoundaries();
            }
            inputIndex = retained.stepIndex();
        }
        if (inputIndex < 0 || inputIndex > receiving.size()) throw new IllegalArgumentException("Invocation cursor is outside compiled order");
        initial = current == 0 && path.filter(value -> !value.frames().isEmpty()).isEmpty();
        String rootInput = rootIdentity(contract, true, root.getFirst().inputTypeId());
        String rootOutput = rootIdentity(contract, false, root.getLast().outputTypeId());
        String selectedInput = inputIndex == receiving.size() ? receiving.getLast().outputTypeId() : receiving.get(inputIndex).inputTypeId();
        if (initial) selectedInput = rootInput;
        else if (current == root.size() && path.filter(value -> !value.frames().isEmpty()).isEmpty()
            && storedIdentity(framed).filter(rootOutput::equals).isPresent()) selectedInput = rootOutput;
        input = selectedInput;
        String output = stop == root.size() ? rootOutput : stop == current ? input : root.get(stop - 1).outputTypeId();
        values = new CompiledCanonicalTransitionPayloadCodec(framing, contract, input, output);
    }

    public static boolean supports(PipelineContractDescriptor contract) {
        try {
            var root = PipelineOrderResourceLoader.loadBoundaries();
            if (root.isEmpty() || contract.rootBoundary().isEmpty()
                || !contract.canonicalTypes().containsKey(contract.rootBoundary().get("inputCanonicalTypeId"))
                || !contract.canonicalTypes().containsKey(contract.rootBoundary().get("outputCanonicalTypeId"))) return false;
            var nested = PipelineOrderResourceLoader.loadDefinitionBoundaries();
            if (!java.util.stream.Stream.concat(root.orElseThrow().stream(), nested.stream().flatMap(definition -> definition.stepBoundaries().stream()))
                .allMatch(boundary -> hasCanonicalIdentity(contract, boundary.inputTypeId())
                    && hasCanonicalIdentity(contract, boundary.outputTypeId()))) return false;
            if (nested.stream().flatMap(definition -> definition.stepBoundaries().stream())
                .anyMatch(boundary -> !loadableStep(boundary.stepClass()))) return false;
            if (!contract.composition().present()) return nested.isEmpty();
            record Visit(String definitionId, List<StepBoundary> order) { }
            var pending = new java.util.ArrayDeque<Visit>();
            var visitedClasses = new java.util.HashSet<String>();
            pending.add(new Visit(contract.composition().rootDefinitionId(), root.orElseThrow()));
            while (!pending.isEmpty()) {
                var visit = pending.removeFirst();
                var definition = contract.composition().definition(visit.definitionId());
                var physicalClasses = visit.order().stream().map(StepBoundary::stepClass).collect(java.util.stream.Collectors.toSet());
                for (var node : definition.nodes()) {
                    if (!node.invocation()) continue;
                    var matches = nested.stream().filter(candidate -> node.targetDefinitionId().equals(candidate.definitionId())
                        && node.nodeId().equals(candidate.callsiteId()) && physicalClasses.contains(candidate.invocationClass())).toList();
                    if (matches.size() != 1) return false;
                    var selected = matches.getFirst();
                    if (visitedClasses.add(selected.invocationClass())) pending.add(new Visit(selected.definitionId(), selected.stepBoundaries()));
                }
            }
            return visitedClasses.size() == nested.size();
        } catch (RuntimeException unavailable) { return false; }
    }

    private static boolean hasCanonicalIdentity(PipelineContractDescriptor contract, String id) {
        return contract.canonicalTypes().containsKey(id) || contract.canonicalTypes().values().stream()
            .filter(definition -> id.equals(definition.get("runtimeClass"))).limit(2).count() == 1;
    }

    private static boolean loadableStep(String name) {
        try {
            Class.forName(name, false, org.pipelineframework.config.pipeline.PipelineResources.resolveClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError unavailable) {
            return false;
        }
    }

    private static String rootIdentity(PipelineContractDescriptor contract, boolean input, String fallback) {
        if (!contract.rootBoundary().isEmpty()) return (String) contract.rootBoundary().get(input ? "inputCanonicalTypeId" : "outputCanonicalTypeId");
        if (contract.composition().present()) {
            var root = contract.composition().definition(contract.composition().rootDefinitionId());
            return input ? root.inputContractId() : root.outputContractId();
        }
        return fallback;
    }

    private static Optional<String> storedIdentity(Object value) {
        if (value instanceof ExecutionInputSnapshot snapshot) return storedIdentity(snapshot.payload());
        if (value instanceof List<?> items) return items.isEmpty() ? Optional.empty() : storedIdentity(items.getFirst());
        return TypedDurablePayload.fromDurableValue(value).map(TypedDurablePayload::canonicalTypeId)
            .map(id -> id.startsWith("List<") && id.endsWith(">") ? id.substring(5, id.length() - 1) : id);
    }

    @Override public String encoding() { return framing.encoding(); }
    @Override public SerializedTransitionPayload encode(Object value) { return values.encode(value); }

    @Override public Object decode(SerializedTransitionPayload payload) {
        if (CompiledCanonicalTransitionPayloadCodec.containsCanonicalPayload(framing, payload)) return values.decode(payload);
        if (!initial) throw new IllegalArgumentException("Canonical continuation envelope is absent");
        return values.decode(framing.encode(frameInitial(framing.decode(payload))));
    }

    private Object frameInitial(Object value) {
        if (value instanceof ExecutionInputSnapshot snapshot) {
            Object framed = snapshot.shape() == ExecutionInputShape.MULTI && snapshot.payload() instanceof List<?> items
                ? items.stream().map(this::frameValue).toList() : frameValue(snapshot.payload());
            return new ExecutionInputSnapshot(snapshot.shape(), framed, snapshot.pipelineContext(), snapshot.invocationPath());
        }
        return frameValue(value);
    }

    private TypedDurablePayload frameValue(Object value) {
        try {
            byte[] bytes = PipelineJson.mapper().writeValueAsBytes(value);
            org.pipelineframework.type.CanonicalTypeCatalogue.fromCanonicalTypes(contract.canonicalTypes())
                .validateAndCanonicalize(input, new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
            return ExecutionDurablePayloadResolver.canonicalEnvelope(contract, input, false, bytes);
        } catch (java.io.IOException failure) {
            throw new IllegalArgumentException("Execution input is not canonical JSON", failure);
        }
    }
}
