package org.pipelineframework.aws.durable.model;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.release.*;
import static org.assertj.core.api.Assertions.*;

class AwsDurableHostedProfileTest {
    @Test void colocatedTransitionHasNoRemoteEntryWhileLegacyStillRequiresIt() {
        var contract = contract("com.customer.Input", "ONE_TO_ONE");
        var original = release(contract, AwsDurableHostedProfile.TYPED_JSON_ENCODING);
        var localEntries = original.deployment().entries().stream()
            .filter(entry -> !entry.trigger().equals("SQS_TRANSITION")).toList();
        var local = withProfile(original, "aws-durable-java21-in-process-v1", localEntries);
        assertThatCode(() -> AwsDurableHostedProfile.requireSupported(local, contract)).doesNotThrowAnyException();
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(
            withProfile(original, "aws-durable-java21-in-process-v1", original.deployment().entries()), contract))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(
            withProfile(original, AwsDurableHostedProfile.PROFILE_ID, localEntries), contract))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void colocatedTransitionRejectsBindingsCommandsAndDeferredCompletion() {
        var internal = contract("com.customer.Input", "ONE_TO_ONE");
        assertInProcessRejected(internal,
            List.of(new PipelineReleaseDeployment.Binding("effects", AwsDurableHostedProfile.COMMAND_EFFECT_CAPABILITY)));
        assertInProcessRejected(contract("com.customer.Input", "ONE_TO_ONE", "command", Map.of()), List.of());
        assertInProcessRejected(
            contract("com.customer.Input", "ONE_TO_ONE", "internal", Map.of("mode", "TRANSPORT")), List.of());
    }

    private static void assertInProcessRejected(PipelineContractDescriptor contract,
        List<PipelineReleaseDeployment.Binding> bindings) {
        var original = release(contract, AwsDurableHostedProfile.TYPED_JSON_ENCODING);
        var entries = original.deployment().entries().stream()
            .filter(entry -> !"SQS_TRANSITION".equals(entry.trigger())).toList();
        var local = new PipelineReleaseDescriptor(original.schemaVersion(), original.pipelineId(),
            original.contractVersion(), original.releaseVersion(), original.compiledTruthArtifactId(),
            original.artifacts(), new PipelineReleaseDeployment(AwsDurableHostedProfile.IN_PROCESS_PROFILE_ID,
                entries, bindings, original.deployment().check()));
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(local, contract))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("In-process placement");
    }

    private static PipelineReleaseDescriptor withProfile(PipelineReleaseDescriptor original, String profile,
        List<PipelineReleaseDeployment.Entry> entries) {
        return new PipelineReleaseDescriptor(original.schemaVersion(), original.pipelineId(), original.contractVersion(),
            original.releaseVersion(), original.compiledTruthArtifactId(), original.artifacts(),
            new PipelineReleaseDeployment(profile, entries, original.deployment().bindings(), original.deployment().check()));
    }

    @Test void acceptsUnrelatedCustomerJavaTypesAndFreeEntryIdsWithExistingUniqueRoles() {
        var contract = contract("com.customer.Input", "ONE_TO_ONE");
        assertThatCode(() -> AwsDurableHostedProfile.requireSupported(release(contract, AwsDurableHostedProfile.TYPED_JSON_ENCODING), contract)).doesNotThrowAnyException();
        assertThat(AwsDurableHostedProfile.TABLE_PARAMETER_TO_NATIVE_SETTING).hasSize(10)
            .containsEntry("ReleaseTable", "dynamo.release-table");
    }
    @Test void rejectsUnknownEncodingCanonicalUrnsStreamingAndMissingRoles() {
        var contract = contract("com.customer.Input", "ONE_TO_ONE");
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(release(contract, "json"), contract)).isInstanceOf(UnsupportedOperationException.class);
        var canonical = contract("urn:customer:Input", "ONE_TO_ONE");
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(release(canonical, AwsDurableHostedProfile.TYPED_JSON_ENCODING), canonical)).isInstanceOf(UnsupportedOperationException.class);
        var stream = contract("com.customer.Input", "MANY_TO_ONE");
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(release(stream, AwsDurableHostedProfile.TYPED_JSON_ENCODING), stream)).isInstanceOf(UnsupportedOperationException.class);
        var valid = release(contract, AwsDurableHostedProfile.TYPED_JSON_ENCODING);
        var partial = new PipelineReleaseDescriptor(2, valid.pipelineId(), valid.contractVersion(), valid.releaseVersion(), valid.compiledTruthArtifactId(), valid.artifacts(),
            new PipelineReleaseDeployment(AwsDurableHostedProfile.PROFILE_ID, valid.deployment().entries().subList(1,10), List.of(), valid.deployment().check()));
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(partial, contract)).isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void acceptsTwoOriginalCodeGroupsButRejectsSplitGroupOrForeignSemanticOwner() {
        var contract = contract("com.customer.Input", "ONE_TO_ONE");
        var one = release(contract, AwsDurableHostedProfile.TYPED_JSON_ENCODING);
        var entries = one.deployment().entries().stream().map(e -> new PipelineReleaseDeployment.Entry(e.entryId(),
            AwsDurableHostedProfile.ENTRIES_BY_TRIGGER.get(e.trigger()).codeGroup() == AwsDurableHostedProfile.CodeGroup.DURABLE ? "driver" : "zip",
            e.runtimeId(), e.packagePath(), e.entryClass(), e.trigger(), e.bindingIds(), e.entryPackagePath())).toList();
        var two = with(one, entries, List.of(artifact("driver", List.of()), artifact("zip", List.of("Transform"))));
        assertThat(AwsDurableHostedProfile.codeGroupArtifacts(two)).containsExactlyInAnyOrderEntriesOf(Map.of(
            AwsDurableHostedProfile.CodeGroup.DURABLE, "driver", AwsDurableHostedProfile.CodeGroup.ACTION, "zip"));
        assertThatCode(() -> AwsDurableHostedProfile.requireSupported(two, contract)).doesNotThrowAnyException();
        var split = entries.stream().map(e -> e.trigger().equals("AWS_DURABLE_INGRESS")
            ? new PipelineReleaseDeployment.Entry(e.entryId(), "zip", e.runtimeId(), e.packagePath(), e.entryClass(), e.trigger(), e.bindingIds(), e.entryPackagePath()) : e).toList();
        assertThatThrownBy(() -> AwsDurableHostedProfile.codeGroupArtifacts(with(two, split, two.artifacts())))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("code group");
        var wrongOwner = with(two, entries, List.of(artifact("driver", List.of("Transform")), artifact("zip", List.of())));
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(wrongOwner, contract))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("semantic step owners");
        assertThatThrownBy(() -> AwsDurableHostedProfile.codeGroupArtifacts(with(two, entries, List.of(artifact("zip", List.of("Transform"))))))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("referenced Lambda ZIP");
    }

    @Test void acceptsSeparatelyAugmentedHttpAlongsideTheTenOriginalRoles() {
        var contract = contract("com.customer.Input", "ONE_TO_ONE");
        var release = release(contract, AwsDurableHostedProfile.TYPED_JSON_ENCODING);
        var http = AwsDurableHostedProfile.HTTP_ENTRY;
        var httpArtifact = new PipelineReleaseArtifactDescriptor("http", "lambda-zip", "maven:example:http:zip:1",
            "sha256:" + "b".repeat(64), List.of(), List.of());
        var entries = new java.util.ArrayList<>(release.deployment().entries());
        entries.add(new PipelineReleaseDeployment.Entry("http", "http", contract.module(), ".", http.entryClass(),
            AwsDurableHostedProfile.HTTP_TRIGGER, List.of()));
        var combined = with(release, entries, List.of(release.artifacts().getFirst(), httpArtifact));

        assertThat(AwsDurableHostedProfile.ENTRIES_BY_TRIGGER).hasSize(10);
        assertThat(AwsDurableHostedProfile.entryForTrigger(AwsDurableHostedProfile.HTTP_TRIGGER)).isEqualTo(http);
        assertThat(http.lambdaHandler()).isEqualTo("io.quarkus.amazon.lambda.runtime.QuarkusStreamHandler::handleRequest");
        assertThat(AwsDurableHostedProfile.codeGroupArtifacts(combined)).containsEntry(AwsDurableHostedProfile.CodeGroup.HTTP, "http");
        assertThatCode(() -> AwsDurableHostedProfile.requireSupported(combined, contract)).doesNotThrowAnyException();
        assertThatThrownBy(() -> AwsDurableHostedProfile.codeGroupArtifacts(with(release, entries,
            List.of(release.artifacts().getFirst(), artifact("http", List.of())))))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("separately augmented");
    }
    @Test void requiresExplicitConnectionFreeCommandFactsAndEveryActionBinding() {
        var original = contract("com.customer.Input", "ONE_TO_ONE");
        var command = new PipelineContractDescriptor(original.schemaVersion(), original.pipelineId(), original.contractVersion(), original.contractHash(),
            original.platform(), original.transport(), original.module(), false, original.runtimeLayout(),
            List.of(new PipelineBundleStepDescriptor(0,"Transform","command","ONE_TO_ONE","com.customer.Input","com.customer.Output","app.Command","app.Generated",Map.of())),
            original.capabilities(),Map.of(),"",null,List.of(),List.of(),List.of());
        var plain = release(command, AwsDurableHostedProfile.TYPED_JSON_ENCODING);
        var entries = plain.deployment().entries().stream().map(e -> new PipelineReleaseDeployment.Entry(e.entryId(), e.artifactId(), e.runtimeId(),
            e.packagePath(), e.entryClass(), e.trigger(), AwsDurableHostedProfile.ENTRIES_BY_TRIGGER.get(e.trigger()).codeGroup() == AwsDurableHostedProfile.CodeGroup.ACTION
                ? List.of("effects") : List.of(), e.entryPackagePath())).toList();
        var bound = new PipelineReleaseDescriptor(2,plain.pipelineId(),plain.contractVersion(),plain.releaseVersion(),plain.compiledTruthArtifactId(),plain.artifacts(),
            new PipelineReleaseDeployment(plain.deployment().profile(),entries,List.of(new PipelineReleaseDeployment.Binding("effects",AwsDurableHostedProfile.COMMAND_EFFECT_CAPABILITY)),plain.deployment().check()));
        var operation = new java.util.HashMap<String,Object>(Map.of("step","Transform","kind","command","externalConfigurationReferenceFields",List.of()));
        var binding = new java.util.HashMap<String,Object>(Map.of("name","connector","externalConfigurationReferenceFields",List.of(),"operations",List.of(operation),"callables",List.of()));
        Map<String,Object> facts = Map.of("schemaVersion",3,"bindings",List.of(binding),"capabilityImports",List.of());
        assertThatCode(() -> AwsDurableHostedProfile.requireSupported(bound,command,facts)).doesNotThrowAnyException();
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(bound,command)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(plain,command,facts)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(with(bound,plain.deployment().entries(),plain.artifacts()),command,facts)).isInstanceOf(UnsupportedOperationException.class);
        for (var location : List.of(binding,operation)) {
            location.remove("externalConfigurationReferenceFields");
            assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(bound,command,facts)).isInstanceOf(UnsupportedOperationException.class);
            location.put("externalConfigurationReferenceFields",List.of("connection"));
            assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(bound,command,facts)).isInstanceOf(UnsupportedOperationException.class);
            location.put("externalConfigurationReferenceFields",List.of());
        }
        operation.put("step","foreign");
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(bound,command,facts)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void acceptsOnlyExactlyAuthorizedQueriesAndSignedScalarCallbacks() {
        var base = contract("com.customer.Input", "ONE_TO_ONE");
        var callback = new java.util.HashMap<String, Object>(Map.of(
            "required", true, "binding", "provider", "provider", "customer.provider", "providerVersion", 1,
            "operation", "run", "operationVersion", 2, "authenticator", "com.customer.Authenticator",
            "endpointResolver", "com.customer.Endpoint"));
        Map<String, Object> deferred = Map.of(
            "mode", "CONNECTOR_CALLBACK", "commandCompletionMode", "ACKNOWLEDGEMENT_REQUIRED",
            "correlationStrategy", "signedResumeToken", "completionPayloadTypeId", "com.customer.Completion",
            "completionProjector", "com.customer.Projector", "commandAcknowledgementTypeId", "com.customer.Ack",
            "operationOutputTypeId", "com.customer.Ack", "finalOutputTypeId", "com.customer.Output",
            "timeout", "PT10M", "callback", callback);
        var steps = List.of(
            new PipelineBundleStepDescriptor(0, "Observe", "query", "ONE_TO_ONE", "com.customer.Input", "com.customer.Observed",
                "com.customer.Query", "com.customer.GeneratedQuery", Map.of()),
            new PipelineBundleStepDescriptor(1, "Run", "command", "ONE_TO_ONE", "com.customer.Observed", "com.customer.Output",
                "com.customer.Command", "com.customer.GeneratedCommand", deferred));
        var contract = new PipelineContractDescriptor(base.schemaVersion(), base.pipelineId(), base.contractVersion(), base.contractHash(),
            base.platform(), base.transport(), base.module(), false, base.runtimeLayout(), steps, base.capabilities(), Map.of(), "", null,
            List.of(), List.of(), List.of());
        var plain = release(contract, AwsDurableHostedProfile.TYPED_JSON_ENCODING);
        var entries = plain.deployment().entries().stream().map(entry -> new PipelineReleaseDeployment.Entry(entry.entryId(),
            entry.artifactId(), entry.runtimeId(), entry.packagePath(), entry.entryClass(), entry.trigger(),
            AwsDurableHostedProfile.ENTRIES_BY_TRIGGER.get(entry.trigger()).codeGroup() == AwsDurableHostedProfile.CodeGroup.ACTION
                ? List.of("effects", "capture", "provider") : List.of(), entry.entryPackagePath())).toList();
        var release = new PipelineReleaseDescriptor(2, plain.pipelineId(), plain.contractVersion(), plain.releaseVersion(), "zip",
            List.of(artifact("zip", List.of("Observe", "Run"))), new PipelineReleaseDeployment(plain.deployment().profile(), entries,
                List.of(new PipelineReleaseDeployment.Binding("effects", AwsDurableHostedProfile.COMMAND_EFFECT_CAPABILITY),
                    new PipelineReleaseDeployment.Binding("capture", AwsDurableHostedProfile.QUERY_CAPTURE_CAPABILITY),
                    new PipelineReleaseDeployment.Binding("provider", AwsDurableHostedProfile.CONNECTOR_CAPABILITY)), plain.deployment().check()));
        var query = new java.util.HashMap<String, Object>(Map.of("step", "Observe", "kind", "query", "operation", "read", "operationVersion", 1));
        var command = Map.of("step", "Run", "kind", "command", "operation", "run", "operationVersion", 2);
        Map<String, Object> facts = Map.of("schemaVersion", 3, "capabilityImports", List.of(), "bindings", List.of(Map.of(
            "name", "provider", "provider", "customer.provider", "providerVersion", 1, "operations", List.of(query, command), "callables", List.of())));
        var authorization = Set.of(new AwsDurableHostedProfile.ConnectorAuthority("provider", "customer.provider", 1));

        assertThatCode(() -> AwsDurableHostedProfile.requireSupported(release, contract, facts, authorization)).doesNotThrowAnyException();
        var missingCapture = new PipelineReleaseDescriptor(2, release.pipelineId(), release.contractVersion(), release.releaseVersion(), "zip",
            release.artifacts(), new PipelineReleaseDeployment(release.deployment().profile(), entries,
                release.deployment().bindings().stream().filter(binding -> !"capture".equals(binding.bindingId())).toList(),
                release.deployment().check()));
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(missingCapture, contract, facts, authorization))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("durable capture-store");
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(release, contract, facts))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("deployment authorization");
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(release, contract, facts,
            Set.of(new AwsDurableHostedProfile.ConnectorAuthority("provider", "customer.provider", 2))))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("exact deployment authorization");
        query.put("externalConfigurationReferenceFields", List.of("token"));
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(release, contract, facts, authorization))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("configuration reference");
        query.remove("externalConfigurationReferenceFields");
        callback.put("providerVersion", 2);
        assertThatThrownBy(() -> AwsDurableHostedProfile.requireSupported(release, contract, facts, authorization))
            .isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("authorized connector operation");
    }

    private static PipelineReleaseDescriptor with(PipelineReleaseDescriptor old, List<PipelineReleaseDeployment.Entry> entries,
        List<PipelineReleaseArtifactDescriptor> artifacts) {
        return new PipelineReleaseDescriptor(2, old.pipelineId(), old.contractVersion(), old.releaseVersion(), old.compiledTruthArtifactId(), artifacts,
            new PipelineReleaseDeployment(old.deployment().profile(), entries, old.deployment().bindings(), old.deployment().check()));
    }
    private static PipelineReleaseArtifactDescriptor artifact(String id, List<String> steps) {
        return new PipelineReleaseArtifactDescriptor(id, "lambda-zip", "maven:example:" + id + ":zip:1", "sha256:" + "a".repeat(64), steps, List.of());
    }
    private static PipelineContractDescriptor contract(String input, String cardinality) {
        return contract(input, cardinality, "internal", Map.of());
    }

    private static PipelineContractDescriptor contract(String input, String cardinality, String kind,
        Map<String, Object> deferredCompletion) {
        return new PipelineContractDescriptor(1, "customer.pipeline", "contract-sha", "hash", "COMPUTE", "LOCAL", "customer-runtime", false, "monolith",
            List.of(new PipelineBundleStepDescriptor(0, "Transform", kind, cardinality, input, "com.customer.Output",
                "com.customer.Service", "com.customer.Client", deferredCompletion)),
            null,Map.of(),"",null,List.of(),List.of(),List.of());
    }
    private static PipelineReleaseDescriptor release(PipelineContractDescriptor c, String encoding) {
        var entries = AwsDurableHostedProfile.ENTRIES_BY_TRIGGER.entrySet().stream().map(e -> new PipelineReleaseDeployment.Entry(
            "free-"+e.getKey().toLowerCase().replace('_','-'), "zip", c.module(), ".", e.getValue().entryClass(), e.getKey(), List.of())).toList();
        var ingress = entries.stream().filter(e -> e.trigger().equals("AWS_DURABLE_INGRESS")).findFirst().orElseThrow();
        var check = new PipelineReleaseDeployment.Check(ingress.entryId(),Map.of("payloadTypeId",c.steps().getFirst().inputTypeId(),"payloadEncoding",encoding,"payload","{}"),
            Map.of("payloadTypeId","com.customer.Output","payloadEncoding",encoding,"payload","{}"),1000);
        return new PipelineReleaseDescriptor(2,c.pipelineId(),c.contractVersion(),"release","zip",List.of(artifact("zip", List.of("Transform"))),new PipelineReleaseDeployment(AwsDurableHostedProfile.PROFILE_ID,entries,List.of(),check));
    }
}
