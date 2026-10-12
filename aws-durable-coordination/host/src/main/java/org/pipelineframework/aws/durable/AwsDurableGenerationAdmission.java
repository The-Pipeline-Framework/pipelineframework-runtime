package org.pipelineframework.aws.durable;

/** Outcome of claiming immutable provider generation authority. */
public enum AwsDurableGenerationAdmission {
    CREATED, REPLAYED, STALE
}
