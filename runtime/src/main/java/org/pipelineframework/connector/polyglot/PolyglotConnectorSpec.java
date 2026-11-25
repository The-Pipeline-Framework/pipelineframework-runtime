package org.pipelineframework.connector.polyglot;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Immutable declaration of custom polyglot Query and Command operations exposed by one
 * {@link PolyglotConnectorProvider} instance, validated against a canonical v3
 * {@link CanonicalTypeCatalogue}.
 */
public record PolyglotConnectorSpec(
    CanonicalTypeCatalogue catalogue,
    List<QueryOperationSpec> queries,
    List<CommandOperationSpec> commands,
    int maxResponseBytes
) {
    public static final int DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

    public PolyglotConnectorSpec {
        Objects.requireNonNull(catalogue, "catalogue must not be null");
        queries = List.copyOf(Objects.requireNonNull(queries, "queries must not be null"));
        commands = List.copyOf(Objects.requireNonNull(commands, "commands must not be null"));
        if (maxResponseBytes <= 0) {
            throw new IllegalArgumentException("maxResponseBytes must be positive");
        }
    }

    public PolyglotConnectorSpec(
        CanonicalTypeCatalogue catalogue,
        List<QueryOperationSpec> queries,
        List<CommandOperationSpec> commands
    ) {
        this(catalogue, queries, commands, DEFAULT_MAX_RESPONSE_BYTES);
    }

    /** Immutable spec for one custom polyglot Query operation. */
    public record QueryOperationSpec(
        String operationId,
        int majorVersion,
        String inputCanonicalTypeId,
        String outputCanonicalTypeId
    ) {
        public QueryOperationSpec {
            operationId = requireNonBlank(operationId, "operationId");
            if (majorVersion <= 0) {
                throw new IllegalArgumentException("majorVersion must be positive");
            }
            inputCanonicalTypeId = requireNonBlank(inputCanonicalTypeId, "inputCanonicalTypeId");
            outputCanonicalTypeId = requireNonBlank(outputCanonicalTypeId, "outputCanonicalTypeId");
        }
    }

    /** Immutable spec for one custom polyglot Command operation (dispatch + optional reconcile). */
    public record CommandOperationSpec(
        String operationId,
        int majorVersion,
        String inputCanonicalTypeId,
        String outputCanonicalTypeId,
        boolean reconciliationSupported,
        Optional<String> receiptReferenceKind
    ) {
        public CommandOperationSpec {
            operationId = requireNonBlank(operationId, "operationId");
            if (majorVersion <= 0) {
                throw new IllegalArgumentException("majorVersion must be positive");
            }
            inputCanonicalTypeId = requireNonBlank(inputCanonicalTypeId, "inputCanonicalTypeId");
            outputCanonicalTypeId = requireNonBlank(outputCanonicalTypeId, "outputCanonicalTypeId");
            receiptReferenceKind = Objects.requireNonNull(receiptReferenceKind, "receiptReferenceKind")
                .filter(v -> !v.isBlank());
            if (reconciliationSupported && receiptReferenceKind.isEmpty()) {
                throw new IllegalArgumentException(
                    "reconciliationSupported Command operation '" + operationId + "' requires a receiptReferenceKind");
            }
        }
    }

    private static String requireNonBlank(String value, String name) {
        if (Objects.requireNonNull(value, name + " must not be null").isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
