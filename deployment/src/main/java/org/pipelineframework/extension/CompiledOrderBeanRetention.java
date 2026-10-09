package org.pipelineframework.extension;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.Optional;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.ApplicationArchivesBuildItem;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Retains only beans selected by the application's statically compiled execution metadata. */
public class CompiledOrderBeanRetention {
    @BuildStep
    UnremovableBeanBuildItem retainCompiledSteps(ApplicationArchivesBuildItem archives) {
        Set<String> classes = new HashSet<>();
        for (var archive : archives.getAllApplicationArchives()) {
            var resource = Optional.ofNullable(archive.getChildPath("META-INF/pipeline/order.json"))
                .filter(Files::isRegularFile);
            if (resource.isEmpty()) continue;
            try (var input = Files.newInputStream(resource.orElseThrow())) {
                var order = PipelineJson.mapper().readTree(input).path("order");
                if (!order.isArray()) throw new IllegalStateException("Compiled pipeline order must contain an order array");
                for (var name : order) {
                    if (!name.isTextual() || name.asText().isBlank())
                        throw new IllegalStateException("Compiled pipeline order contains an invalid execution class");
                    classes.add(name.asText());
                }
            } catch (IOException failure) {
                throw new IllegalStateException("Cannot read compiled pipeline order metadata", failure);
            }
        }
        return UnremovableBeanBuildItem.beanClassNames(classes);
    }
}
