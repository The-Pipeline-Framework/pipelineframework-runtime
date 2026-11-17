package org.pipelineframework.aws.durable.model;

/** Evidence from the configuration of an exact numbered Lambda version. */
public final class LambdaVersionConfigurationEvidence {
    private static final String REVISION_PREFIX = "aws-lambda-revision:";

    private LambdaVersionConfigurationEvidence() {
    }

    public static String capture(String configurationSha256, String revisionId) {
        if (present(configurationSha256)) {
            return configurationSha256;
        }
        if (present(revisionId)) {
            return REVISION_PREFIX + revisionId;
        }
        throw new IllegalStateException("Numbered Lambda version has no configuration hash or revision");
    }

    public static boolean matches(String expected, String configurationSha256, String revisionId) {
        if (expected.startsWith(REVISION_PREFIX)) {
            return present(revisionId) && expected.equals(REVISION_PREFIX + revisionId);
        }
        return present(configurationSha256) && expected.equals(configurationSha256);
    }

    private static boolean present(String value) {
        return value != null && !value.isBlank();
    }
}
