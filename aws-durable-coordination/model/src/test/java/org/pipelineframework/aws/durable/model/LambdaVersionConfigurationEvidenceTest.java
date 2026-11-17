package org.pipelineframework.aws.durable.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LambdaVersionConfigurationEvidenceTest {
    @Test
    void prefersTheConfigurationHashWhenAvailable() {
        var pin = LambdaVersionConfigurationEvidence.capture("hash", "revision");
        assertEquals("hash", pin);
        assertTrue(LambdaVersionConfigurationEvidence.matches(pin, "hash", "revision"));
        assertFalse(LambdaVersionConfigurationEvidence.matches(pin, null, "revision"));
        assertFalse(LambdaVersionConfigurationEvidence.matches(pin, "changed", "revision"));
    }

    @Test
    void pinsTheNumberedVersionRevisionWhenAwsOmitsTheHash() {
        var pin = LambdaVersionConfigurationEvidence.capture(null, "revision");
        assertTrue(LambdaVersionConfigurationEvidence.matches(pin, null, "revision"));
        assertTrue(LambdaVersionConfigurationEvidence.matches(pin, "later-hash", "revision"));
        assertFalse(LambdaVersionConfigurationEvidence.matches(pin, null, "changed"));
        assertFalse(LambdaVersionConfigurationEvidence.matches(pin, null, null));
        assertThrows(IllegalStateException.class, () -> LambdaVersionConfigurationEvidence.capture(null, null));
    }
}
