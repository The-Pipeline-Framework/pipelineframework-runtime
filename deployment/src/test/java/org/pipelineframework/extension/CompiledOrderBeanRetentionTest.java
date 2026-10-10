package org.pipelineframework.extension;

import io.quarkus.arc.Arc;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.pipelineframework.service.ReactiveService;
import static org.junit.jupiter.api.Assertions.*;

class CompiledOrderBeanRetentionTest {
    @RegisterExtension
    static final QuarkusExtensionTest APPLICATION = new QuarkusExtensionTest()
        .withApplicationRoot(archive -> archive.addClasses(Ordinary.class, Unlisted.class, Handler.class)
            .addAsResource(new StringAsset("{\"order\":[\"" + Ordinary.class.getName() + "\"]}"),
                "META-INF/pipeline/order.json"))
        .overrideConfigKey("quarkus.devservices.enabled", "false")
        .overrideConfigKey("quarkus.http.test-port", "0");

    @Test
    void dynamicallyResolvedCompiledStepSurvivesNormalBeanRemoval() throws Exception {
        var type = Class.forName(Ordinary.class.getName(), true, Thread.currentThread().getContextClassLoader());
        var bean = Arc.container().instance(type);
        assertTrue(bean.isAvailable(), "Compiled execution metadata must retain its ordinary service");
        @SuppressWarnings("unchecked") var service = (ReactiveService<String, String>) bean.get();
        assertEquals("result:input", service.process("input").await().indefinitely());
    }

    @Test
    void unrelatedServiceIsNotGloballyRetained() {
        assertFalse(Arc.container().instance(Unlisted.class).isAvailable());
    }

    @ApplicationScoped
    public static class Ordinary implements ReactiveService<String, String> {
        public Uni<String> process(String input) { return Uni.createFrom().item("result:" + input); }
    }

    @ApplicationScoped
    public static class Unlisted implements ReactiveService<String, String> {
        public Uni<String> process(String input) { return Uni.createFrom().item(input); }
    }

    public static class Handler implements com.amazonaws.services.lambda.runtime.RequestHandler<String, String> {
        public String handleRequest(String input, com.amazonaws.services.lambda.runtime.Context context) { return input; }
    }
}
