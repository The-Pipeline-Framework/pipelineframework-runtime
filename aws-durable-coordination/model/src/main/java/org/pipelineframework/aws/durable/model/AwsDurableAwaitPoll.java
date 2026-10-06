package org.pipelineframework.aws.durable.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record AwsDurableAwaitPoll(List<AwsDurableAwaitIdentity> candidates) {
    public AwsDurableAwaitPoll {
        candidates = List.copyOf(Objects.requireNonNull(candidates, "candidates"));
        if (candidates.size() > 1) {
            throw new IllegalArgumentException("AWS Durable Await polling expects at most one candidate");
        }
    }

    public static AwsDurableAwaitPoll from(Optional<AwsDurableAwaitIdentity> candidate) {
        Objects.requireNonNull(candidate, "candidate");
        return new AwsDurableAwaitPoll(candidate.stream().toList());
    }

    public Optional<AwsDurableAwaitIdentity> candidate() {
        return candidates.stream().findFirst();
    }
}
