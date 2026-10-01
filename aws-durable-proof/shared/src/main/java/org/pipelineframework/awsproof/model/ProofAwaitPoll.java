package org.pipelineframework.awsproof.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record ProofAwaitPoll(List<ProofAwaitIdentity> candidates) {
    public ProofAwaitPoll {
        candidates = List.copyOf(Objects.requireNonNull(candidates, "candidates"));
        if (candidates.size() > 1) {
            throw new IllegalArgumentException("proof Await polling expects at most one candidate");
        }
    }

    public static ProofAwaitPoll from(Optional<ProofAwaitIdentity> candidate) {
        Objects.requireNonNull(candidate, "candidate");
        return new ProofAwaitPoll(candidate.stream().toList());
    }

    public Optional<ProofAwaitIdentity> candidate() {
        return candidates.stream().findFirst();
    }
}
