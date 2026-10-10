package org.pipelineframework.command;

import org.pipelineframework.connector.CommandRecoveryBinding;
import org.pipelineframework.connector.CommandRecoveryTarget;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;

/** Package-private validation shared by native dispatch and receipt settlement. */
final class CommandRecoverySupport {
    private CommandRecoverySupport() {}

    static CommandRecoveryBinding binding(
        CommandRequest<?> request, NativeCommandSelector selector,
        ConnectorConfigurationSnapshot configuration, CommandRecoveryTarget target
    ) {
        var context = request.executionContext();
        return new CommandRecoveryBinding(context.tenantId(), request.commandId(), request.occurrenceId(),
            request.attemptId(), context.executionId(), context.pipelineId(), context.contractVersion(),
            context.releaseVersion(), request.descriptor().stepId(), selector.operationIdentity(),
            selector.providerMajorVersion(), selector.binding().orElseGet(() ->
                ConnectorBindingName.of(selector.operationIdentity().providerId().value())),
            request.descriptor().inputType(), request.descriptor().outputType(),
            new CommandEffectRecordCodec().digest(request.input(), request.descriptor().inputType()), configuration, target);
    }

    static void verify(CommandEffectRecord record, CommandRequest<?> request, CommandRecoveryBinding proposed) {
        var retained = record.currentAttempt().recoveryBinding();
        if (request.callbackContext().isPresent() || retained.isEmpty()
            || !record.tenantId().equals(request.executionContext().tenantId())
            || !record.commandId().equals(request.commandId())
            || !record.stepId().equals(request.descriptor().stepId())
            || !record.command().equals(request.descriptor().command())
            || !request.descriptor().nativeSelector().orElseThrow().commandName().equals(record.command())
            || !retained.orElseThrow().equals(proposed)) {
            throw new CommandRecoveryConflictException("Original native Command request/configuration/target binding does not match");
        }
        new CommandEffectRecordCodec().validateRecovery(record, request.descriptor().inputType(), request.descriptor().outputType());
    }

    static boolean sameSuccess(CommandEffectRecord record, CommandRequest<?> request, Object output, CommandOutcomeSnapshot snapshot) {
        if (record.status() != CommandEffectStatus.SUCCEEDED
            || record.currentAttempt().recoveryBinding().isEmpty()
            || !record.tenantId().equals(request.executionContext().tenantId())
            || !record.commandId().equals(request.commandId())
            || !record.command().equals(request.descriptor().command())
            || !record.stepId().equals(request.descriptor().stepId())
            || !record.currentAttempt().attemptId().equals(request.attemptId())
            || !record.currentAttempt().occurrenceId().equals(request.occurrenceId())
            || !record.outcome().filter(snapshot::equals).isPresent()) {
            return false;
        }
        var codec = new CommandEffectRecordCodec();
        codec.validateRecovery(record, request.descriptor().inputType(), request.descriptor().outputType());
        var binding = record.currentAttempt().recoveryBinding().orElseThrow();
        return binding.inputDigest().equals(codec.digest(request.input(), request.descriptor().inputType()))
            && binding.executionId().equals(request.executionContext().executionId())
            && binding.pipelineId().equals(request.executionContext().pipelineId())
            && binding.contractVersion().equals(request.executionContext().contractVersion())
            && binding.releaseVersion().equals(request.executionContext().releaseVersion())
            && codec.digest(record.output(), request.descriptor().outputType())
            .equals(codec.digest(output, request.descriptor().outputType()));
    }
}
