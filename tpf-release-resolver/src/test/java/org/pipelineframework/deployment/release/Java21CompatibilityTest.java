package org.pipelineframework.deployment.release;

import java.io.DataInputStream;
import java.io.IOException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The shared resolver must load in the Java 21 runtime, independently of CLI build JDK. */
class Java21CompatibilityTest {
    @ParameterizedTest
    @ValueSource(strings = {"ReleaseVerifier", "ReleaseLoader", "ResolverProfile", "CredentialResolver",
            "MavenResolverConfiguration", "MavenArtifactResolverProvider", "OciArtifactResolverProvider"})
    void sharedResolverBytecodeLoadsOnJava21(String name) throws IOException {
        var resource = getClass().getResourceAsStream(name + ".class");
        assertNotNull(resource, "Compiled resolver class must exist");
        try (var input = new DataInputStream(resource)) {
            assertEquals(0xCAFEBABE, input.readInt(), "Class file magic");
            input.readUnsignedShort(); // Minor version.
            int major = input.readUnsignedShort();
            assertTrue(major <= 65, name + " requires newer than Java 21: class-file major " + major);
        }
    }
}
