package org.pipelineframework.awsproof;

final class ProofInjectedFaultException extends RuntimeException {
    ProofInjectedFaultException(String point) {
        super("Injected deployed-proof fault: " + point);
    }
}
