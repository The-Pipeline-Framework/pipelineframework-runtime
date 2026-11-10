package org.pipelineframework.aws.durable.model;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;

/** Existing AWS Durable SAM entry facts consumed by Release packaging and provider delivery. */
public final class AwsDurableHostedProfile {
    public static final String PROFILE_ID = "aws-durable-java21-v1";
    public static final String IN_PROCESS_PROFILE_ID = "aws-durable-java21-in-process-v1";
    public enum TransitionPlacement { FUNCTION, IN_PROCESS }
    public static final String RETAINED_ISOLATION_MODE = "RETAINED_RELEASE_VERSIONS";
    /** Original coordinator and action code, with a separately augmented optional HTTP application. */
    public enum CodeGroup { DURABLE, ACTION, HTTP }
    public record Entry(String functionLogicalId, String versionLogicalId, String entryClass, String namedHandler,
                        CodeGroup codeGroup) {
        public Entry(String functionLogicalId, String versionLogicalId, String entryClass, String namedHandler) {
            this(functionLogicalId, versionLogicalId, entryClass, namedHandler,
                "DurableFunction".equals(functionLogicalId) || "IngressFunction".equals(functionLogicalId)
                    ? CodeGroup.DURABLE : CodeGroup.ACTION);
        }

        public String lambdaHandler() {
            return (codeGroup == CodeGroup.DURABLE ? entryClass : "io.quarkus.amazon.lambda.runtime.QuarkusStreamHandler")
                + "::handleRequest";
        }
    }
    public static final String HTTP_TRIGGER = "AWS_DURABLE_HTTP";
    public static final Entry HTTP_ENTRY = new Entry("HttpFunction", "HttpVersion",
        "io.quarkus.amazon.lambda.http.LambdaHttpHandler", "", CodeGroup.HTTP);
    public static final Map<String, Entry> ENTRIES_BY_TRIGGER = Map.ofEntries(
        entry("AWS_DURABLE_COORDINATOR", "Durable", "AwsDurableCoordinatorHandler", ""),
        entry("AWS_DURABLE_INGRESS", "Ingress", "AwsDurableIngressHandler", ""),
        entry("AWS_DURABLE_ACTION", "Action", "AwsDurableActionGatewayHandler", "aws-durable-action"),
        entry("SQS_WORK", "Work", "AwsDurableWorkItemHandler", "aws-durable-work-item"),
        Map.entry("SQS_TRANSITION", new Entry("TransitionWorkerFunction", "TransitionWorkerVersion", "org.pipelineframework.aws.durable.AwsDurableTransitionWorkerHandler", "aws-durable-transition-worker")),
        entry("SQS_AWAIT_COMPLETION", "AwaitCompletion", "AwsDurableAwaitCompletionHandler", "aws-durable-await-completion"),
        entry("DYNAMO_AWAIT_WAKEUP", "AwaitWakeup", "AwsDurableAwaitWakeupHandler", "aws-durable-await-wakeup"),
        entry("DYNAMO_EXECUTION_WAKEUP", "ExecutionWakeup", "AwsDurableExecutionWakeupHandler", "aws-durable-execution-wakeup"),
        entry("SQS_DUE_WORK", "DueWork", "AwsDurableDueWorkHandler", "aws-durable-due-work"),
        entry("AWS_DURABLE_RECONCILIATION", "Reconciliation", "AwsDurableReconciliationHandler", "aws-durable-reconcile"));

    public static Entry entryForTrigger(String trigger) {
        return HTTP_TRIGGER.equals(trigger) ? HTTP_ENTRY : ENTRIES_BY_TRIGGER.get(trigger);
    }
    /** Existing SAM inputs mapped to native orchestrator settings, without a second resource model. */
    public static final Map<String, String> TABLE_PARAMETER_TO_NATIVE_SETTING = Map.ofEntries(
        Map.entry("ExecutionTable", "dynamo.execution-table"), Map.entry("ExecutionKeyTable", "dynamo.execution-key-table"),
        Map.entry("ExecutionPayloadTable", "dynamo.execution-payload-table"), Map.entry("AwaitInteractionTable", "dynamo.await-interaction-table"),
        Map.entry("AwaitInteractionKeyTable", "dynamo.await-interaction-key-table"), Map.entry("AwaitCompletionLifecycleTable", "dynamo.await-completion-lifecycle-table"),
        Map.entry("AwaitUnitTable", "dynamo.await-unit-table"), Map.entry("AwaitAdmissionTable", "dynamo.await-admission-table"),
        Map.entry("ReleaseTable", "dynamo.release-table"), Map.entry("WorkerTable", "dynamo.worker-table"));
    public enum BindingResourceKind { DYNAMODB_TABLE }
    public record BindingCapability(String samParameter, String runtimeSetting, BindingResourceKind resourceKind) {}
    public static final String COMMAND_EFFECT_CAPABILITY = org.pipelineframework.orchestrator.release.PipelineReleaseRuntimeCapabilities.COMMAND_EFFECT_DYNAMODB;
    public static final String QUERY_CAPTURE_CAPABILITY = org.pipelineframework.orchestrator.release.PipelineReleaseRuntimeCapabilities.QUERY_CAPTURE_DYNAMODB;
    public static final String CONNECTOR_CAPABILITY = "tpf.connector";
    public static final Map<String, BindingCapability> OPTIONAL_BINDING_CAPABILITIES = Map.of(
        COMMAND_EFFECT_CAPABILITY, new BindingCapability("CommandEffectTable", "pipeline.command.effect-store.dynamo.table", BindingResourceKind.DYNAMODB_TABLE),
        QUERY_CAPTURE_CAPABILITY, new BindingCapability("QueryCaptureTable", "pipeline.query.capture-store.dynamo.table", BindingResourceKind.DYNAMODB_TABLE));
    public static final String TYPED_JSON_ENCODING = "application/tpf-transition+json";

    /** A compiled connector identity approved for this immutable Release by the deployment owner. */
    public record ConnectorAuthority(String bindingName, String provider, int providerVersion) {
        public ConnectorAuthority {
            if (bindingName == null || bindingName.isBlank() || provider == null || provider.isBlank() || providerVersion < 1) {
                throw new IllegalArgumentException("Invalid connector authority identity");
            }
        }
    }

    /** Pure capability gate; package closure verification and environment/IAM observation remain separate gates. */
    public static void requireSupported(org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor release,
        org.pipelineframework.orchestrator.release.PipelineContractDescriptor contract) {
        requireSupported(release, contract, Map.of());
    }

    /** Connector metadata must be parsed from the already verified immutable Compiled Truth resource. */
    public static void requireSupported(org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor release,
        org.pipelineframework.orchestrator.release.PipelineContractDescriptor contract, Map<String, Object> connectorMetadata) {
        requireSupported(release, contract, connectorMetadata, Set.of());
    }

    /** Connector approval is environment evidence supplied by the deployment owner, never a claim from the Release. */
    public static void requireSupported(org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor release,
        org.pipelineframework.orchestrator.release.PipelineContractDescriptor contract, Map<String, Object> connectorMetadata,
        Set<ConnectorAuthority> authorizedConnectors) {
        java.util.Objects.requireNonNull(release, "release"); java.util.Objects.requireNonNull(contract, "contract");
        var approvals = Set.copyOf(authorizedConnectors);
        var deployment = release.deployment();
        var placement = transitionPlacement(release);
        require(release.pipelineId().equals(contract.pipelineId()) && release.contractVersion().equals(contract.contractVersion()), "Release/Contract identity mismatch");
        require(!contract.steps().isEmpty() && contract.capabilities().localTransitionExecution(), "Compiled local execution is required");
        require(contract.capabilityImports().isEmpty() && contract.importedDefinitions().isEmpty() && !contract.composition().present()
            && contract.httpPayloadBoundaries().isEmpty(), "Imported/composed/external capabilities are not qualified");
        if (placement == TransitionPlacement.IN_PROCESS) {
            require(deployment.bindings().isEmpty()
                && contract.steps().stream().allMatch(step -> "internal".equals(step.kind()) && step.deferredCompletion().isEmpty()),
                "In-process placement qualifies internal steps without external dependencies or Await");
        }
        var commands = contract.steps().stream().filter(step -> "command".equals(step.kind()))
            .map(PipelineBundleStepDescriptor::authoredName).collect(java.util.stream.Collectors.toSet());
        boolean hasQueries = contract.steps().stream().anyMatch(step -> "query".equals(step.kind()));
        var requiredActionBindings = new HashSet<String>();
        var declaredConnectors = new HashSet<String>();
        int effectStores = 0;
        int captureStores = 0;
        for (var binding : deployment.bindings()) {
            require(requiredActionBindings.add(binding.bindingId()), "Duplicate hosted binding identity");
            switch (binding.capability()) {
                case COMMAND_EFFECT_CAPABILITY -> effectStores++;
                case QUERY_CAPTURE_CAPABILITY -> captureStores++;
                case CONNECTOR_CAPABILITY -> declaredConnectors.add(binding.bindingId());
                default -> throw new UnsupportedOperationException("Unknown hosted binding capability");
            }
        }
        require(effectStores == (commands.isEmpty() ? 0 : 1), "Commands require exactly one durable effect-store binding");
        require(captureStores == (hasQueries ? 1 : 0), "Queries require exactly one durable capture-store binding");
        require(declaredConnectors.equals(approvals.stream().map(ConnectorAuthority::bindingName).collect(java.util.stream.Collectors.toSet())),
            "Release connector bindings must match the deployment authorization");
        if (!approvals.isEmpty()) requireAuthorizedConnectors(contract.steps(), connectorMetadata, approvals);
        else if (!commands.isEmpty()) requireConnectionFreeCommands(connectorMetadata, commands);
        var codeArtifacts = codeGroupArtifacts(release);
        String actionArtifact = codeArtifacts.get(CodeGroup.ACTION);
        // The existing closure verifier owns complete/unique step coverage. The original SAM has one semantic worker code group.
        require(release.artifacts().stream().allMatch(a -> actionArtifact.equals(a.artifactId()) || a.stepIds().isEmpty()),
            "All semantic step owners must use the original ACTION code group artifact");
        for (var step : contract.steps()) {
            require("ONE_TO_ONE".equals(step.cardinality()) && step.paging().isEmpty(), "Only unary typed JSON steps are qualified");
            require("internal".equals(step.kind()) || "command".equals(step.kind())
                || (!approvals.isEmpty() && "query".equals(step.kind())), "Step kind requires authorized connector support");
            require(step.deferredCompletion().isEmpty() || (!approvals.isEmpty() && "command".equals(step.kind())),
                "Deferred completion requires an authorized Command");
            require(javaType(step.inputTypeId()) && javaType(step.outputTypeId()), "Native typed JSON requires exact compiled Java type IDs");
        }
        java.util.Set<String> triggers = new java.util.HashSet<>();
        java.util.Set<String> entryIds = new java.util.HashSet<>();
        for (var entry : deployment.entries()) {
            var expected = entryForTrigger(entry.trigger());
            require(expected != null && expected.entryClass().equals(entry.entryClass()) && triggers.add(entry.trigger())
                && entryIds.add(entry.entryId()) && contract.module().equals(entry.runtimeId()), "Invalid existing-host entry selection");
            var requiredBindings = expected.codeGroup() != CodeGroup.DURABLE ? requiredActionBindings : Set.<String>of();
            require(entry.bindingIds().size() == requiredBindings.size() && requiredBindings.equals(new HashSet<>(entry.bindingIds())),
                "Each action entry must declare exactly the required hosted bindings");
        }
        var check = deployment.check();
        require(check != null && check.timeoutMillis() > 0, "Native execution check is required");
        require(deployment.entries().stream().anyMatch(entry -> entry.entryId().equals(check.entryId())
            && "AWS_DURABLE_INGRESS".equals(entry.trigger())), "Native check requires the ingress entry");
        require(TYPED_JSON_ENCODING.equals(check.input().get("payloadEncoding"))
            && TYPED_JSON_ENCODING.equals(check.expectedResult().get("payloadEncoding")), "Unsupported native payload encoding");
        require(contract.steps().getFirst().inputTypeId().equals(check.input().get("payloadTypeId"))
            && contract.steps().getLast().outputTypeId().equals(check.expectedResult().get("payloadTypeId")), "Check payload types must match compiled unary boundaries");
        require(check.input().get("payload") != null && check.expectedResult().get("payload") != null, "Native typed payload bytes are required");
    }

    private static void requireAuthorizedConnectors(List<PipelineBundleStepDescriptor> steps, Map<String, Object> metadata,
        Set<ConnectorAuthority> approvals) {
        require(metadata != null && version(metadata.get("schemaVersion")) == 3 && emptyList(metadata.get("capabilityImports"))
            && metadata.get("bindings") instanceof List<?>, "Connector support requires compiler-owned operation facts");
        var remaining = new HashMap<String, PipelineBundleStepDescriptor>();
        for (var step : steps) {
            if ("query".equals(step.kind()) || "command".equals(step.kind())) {
                require(remaining.putIfAbsent(step.authoredName(), step) == null, "Duplicate compiled connector step");
            }
        }
        var observed = new HashSet<ConnectorAuthority>();
        var names = new HashSet<String>();
        for (Object value : (List<?>) metadata.get("bindings")) {
            require(value instanceof Map<?, ?>, "Invalid compiled connector binding");
            var binding = (Map<?, ?>) value;
            int providerVersion = version(binding.get("providerVersion"));
            require(providerVersion > 0, "Connector provider requires an exact version");
            var identity = new ConnectorAuthority(nonBlank(binding.get("name")), nonBlank(binding.get("provider")), providerVersion);
            require(approvals.contains(identity) && names.add(identity.bindingName()), "Connector binding lacks exact deployment authorization");
            observed.add(identity);
            require(emptyList(binding.get("callables")) && absentOrEmptyReferences(binding)
                && binding.get("operations") instanceof List<?> operations
                && !operations.isEmpty(), "Connector support requires bounded compiled operations");
            for (Object operationValue : (List<?>) binding.get("operations")) {
                require(operationValue instanceof Map<?, ?>, "Invalid compiled connector operation");
                var operation = (Map<?, ?>) operationValue;
                var step = remaining.remove(nonBlank(operation.get("step")));
                require(step != null && step.kind().equals(operation.get("kind")) && absentOrEmptyReferences(operation),
                    "Connector operation does not match a compiled step or requires an unsupported configuration reference");
                String operationName = nonBlank(operation.get("operation"));
                int operationVersion = version(operation.get("operationVersion"));
                require(operationVersion > 0, "Connector operation requires an exact version");
                if (!step.deferredCompletion().isEmpty()) requireScalarCallback(step, identity, operationName, operationVersion);
            }
        }
        require(remaining.isEmpty() && observed.equals(approvals), "Connector authorization must cover exactly the compiled bindings and steps");
    }

    private static void requireScalarCallback(PipelineBundleStepDescriptor step, ConnectorAuthority identity, String operation,
        int operationVersion) {
        var deferred = step.deferredCompletion();
        require("command".equals(step.kind()) && "CONNECTOR_CALLBACK".equals(deferred.get("mode"))
            && "ACKNOWLEDGEMENT_REQUIRED".equals(deferred.get("commandCompletionMode"))
            && "signedResumeToken".equals(deferred.get("correlationStrategy"))
            && javaType(nonBlank(deferred.get("completionPayloadTypeId")))
            && javaType(nonBlank(deferred.get("completionProjector")))
            && javaType(nonBlank(deferred.get("commandAcknowledgementTypeId")))
            && deferred.get("commandAcknowledgementTypeId").equals(deferred.get("operationOutputTypeId"))
            && step.outputTypeId().equals(deferred.get("finalOutputTypeId"))
            && positiveDuration(deferred.get("timeout")) && deferred.get("callback") instanceof Map<?, ?>,
            "Only signed scalar connector callbacks with typed acknowledgement and completion are qualified");
        var callback = (Map<?, ?>) deferred.get("callback");
        require(Boolean.TRUE.equals(callback.get("required")) && identity.bindingName().equals(callback.get("binding"))
            && identity.provider().equals(callback.get("provider")) && identity.providerVersion() == version(callback.get("providerVersion"))
            && operation.equals(callback.get("operation")) && operationVersion == version(callback.get("operationVersion"))
            && javaType(nonBlank(callback.get("authenticator"))) && javaType(nonBlank(callback.get("endpointResolver"))),
            "Deferred callback does not match its authorized connector operation");
    }

    private static boolean positiveDuration(Object value) {
        if (!(value instanceof String text)) return false;
        try { return Duration.parse(text).isPositive(); }
        catch (DateTimeParseException invalid) { return false; }
    }

    private static int version(Object value) {
        return value instanceof Number number && number.doubleValue() == number.intValue() ? number.intValue() : 0;
    }

    private static String nonBlank(Object value) {
        if (value instanceof String text && !text.isBlank()) return text;
        throw new UnsupportedOperationException("Required compiled connector identity or type is missing");
    }

    private static boolean absentOrEmptyReferences(Map<?, ?> value) {
        return !value.containsKey("externalConfigurationReferenceFields") || emptyList(value.get("externalConfigurationReferenceFields"));
    }
    private static void requireConnectionFreeCommands(Map<String, Object> metadata, java.util.Set<String> commands) {
        require(metadata != null && metadata.get("schemaVersion") instanceof Number schema && schema.doubleValue() == 3.0
            && emptyList(metadata.get("capabilityImports")) && metadata.get("bindings") instanceof List<?>,
            "Commands require compiler-owned connector capability facts");
        var seen = new java.util.HashSet<String>();
        var names = new java.util.HashSet<String>();
        for (Object item : (List<?>) metadata.get("bindings")) {
            require(item instanceof Map<?, ?>, "Invalid compiled connector binding");
            var binding = (Map<?, ?>) item;
            require(binding.get("name") instanceof String name && !name.isBlank() && names.add(name)
                && emptyList(binding.get("externalConfigurationReferenceFields"))
                && emptyList(binding.get("callables")) && binding.get("operations") instanceof List<?>,
                "Connector external references, unknown requirements or callables are not qualified");
            for (Object operationItem : (List<?>) binding.get("operations")) {
                require(operationItem instanceof Map<?, ?>, "Invalid compiled connector operation");
                var operation = (Map<?, ?>) operationItem;
                require(operation.get("step") instanceof String step && commands.contains(step) && seen.add(step)
                    && "command".equals(operation.get("kind"))
                    && emptyList(operation.get("externalConfigurationReferenceFields")),
                    "Command external references or unknown requirements are not qualified");
            }
        }
        require(seen.equals(commands), "Every Command requires exact compiler-owned connector capability facts");
    }
    private static boolean emptyList(Object value) { return value instanceof List<?> list && list.isEmpty(); }

    /** Resolves owner SAM code groups from immutable Release entries. */
    public static Map<CodeGroup, String> codeGroupArtifacts(
        org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor release) {
        java.util.Objects.requireNonNull(release, "release");
        var deployment = release.deployment();
        var placement = transitionPlacement(release);
        var artifacts = new java.util.HashMap<String, org.pipelineframework.orchestrator.release.PipelineReleaseArtifactDescriptor>();
        for (var artifact : release.artifacts()) {
            require(artifacts.putIfAbsent(artifact.artifactId(), artifact) == null, "Duplicate Release artifact identity");
        }
        var groups = new java.util.EnumMap<CodeGroup, String>(CodeGroup.class);
        var triggers = new java.util.HashSet<String>();
        for (var entry : deployment.entries()) {
            var owner = entryForTrigger(entry.trigger());
            require(owner != null && triggers.add(entry.trigger()), "Unknown or duplicate existing-host role");
            var artifact = artifacts.get(entry.artifactId());
            require(artifact != null && "lambda-zip".equals(artifact.kind()), "Existing host code groups require referenced Lambda ZIP artifacts");
            String previous = groups.putIfAbsent(owner.codeGroup(), entry.artifactId());
            require(previous == null || previous.equals(entry.artifactId()), "One original SAM code group cannot select different artifacts");
        }
        var required = new HashSet<>(ENTRIES_BY_TRIGGER.keySet());
        if (placement == TransitionPlacement.IN_PROCESS) {
            required.remove("SQS_TRANSITION");
            require(!triggers.contains("SQS_TRANSITION"), "In-process placement must not declare a remote transition entry");
        }
        require(triggers.containsAll(required), "All selected host roles require exactly one entry");
        if (groups.containsKey(CodeGroup.HTTP)) {
            String httpDigest = artifacts.get(groups.get(CodeGroup.HTTP)).digest();
            require(!httpDigest.equals(artifacts.get(groups.get(CodeGroup.ACTION)).digest())
                && !httpDigest.equals(artifacts.get(groups.get(CodeGroup.DURABLE)).digest()),
                "HTTP and native handler groups require separately augmented Lambda ZIPs");
        }
        return Map.copyOf(groups);
    }

    public static TransitionPlacement transitionPlacement(
        org.pipelineframework.orchestrator.release.PipelineReleaseDescriptor release) {
        require(release.schemaVersion() == 2 && release.deployment() != null, "Unsupported hosted profile");
        return switch (release.deployment().profile()) {
            case PROFILE_ID -> TransitionPlacement.FUNCTION;
            case IN_PROCESS_PROFILE_ID -> TransitionPlacement.IN_PROCESS;
            default -> throw new UnsupportedOperationException("Unsupported hosted profile");
        };
    }

    private static boolean javaType(String id) {
        return id != null && id.matches("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+");
    }
    private static void require(boolean accepted, String reason) {
        if (!accepted) throw new UnsupportedOperationException(reason);
    }

    private static Map.Entry<String, Entry> entry(String trigger, String resource, String handler, String named) {
        return Map.entry(trigger, new Entry(resource + "Function", resource + "Version", "org.pipelineframework.aws.durable." + handler, named));
    }
    private AwsDurableHostedProfile() {}
}
