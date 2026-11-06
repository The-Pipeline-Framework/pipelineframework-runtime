package org.pipelineframework.release.maven;

import java.util.List;
import java.util.Map;
import org.pipelineframework.orchestrator.release.PipelineReleaseDeployment;

/** Maven build configuration for physical facts emitted in Release schema 2. */
public final class ReleaseDeploymentConfiguration {
    private String profile;
    private List<Entry> entries = List.of();
    private List<Binding> bindings = List.of();
    private Check check;

    PipelineReleaseDeployment toDescriptor() {
        return new PipelineReleaseDeployment(profile, entries.stream().map(Entry::toDescriptor).toList(),
            bindings.stream().map(b -> new PipelineReleaseDeployment.Binding(b.bindingId, b.capability)).toList(),
            check == null ? null : new PipelineReleaseDeployment.Check(check.entryId, check.input, check.expectedResult, check.timeoutMillis));
    }

    public static final class Entry {
        private String entryId;
        private String artifactId;
        private String runtimeId;
        private String packagePath;
        private String entryClass;
        private String entryPackagePath;
        private String trigger;
        private List<String> bindingIds = List.of();
        PipelineReleaseDeployment.Entry toDescriptor() {
            return new PipelineReleaseDeployment.Entry(entryId, artifactId, runtimeId, packagePath, entryClass, trigger, bindingIds, entryPackagePath);
        }
    }
    public static final class Binding {
        private String bindingId;
        private String capability;
    }
    public static final class Check {
        private String entryId;
        private Map<String,String> input;
        private Map<String,String> expectedResult;
        private long timeoutMillis;
    }
}
