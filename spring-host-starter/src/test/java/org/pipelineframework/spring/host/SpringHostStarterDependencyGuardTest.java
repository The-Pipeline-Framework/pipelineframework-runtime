/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.spring.host;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Verifies that {@code pipelineframework-spring-host-starter} has zero Quarkus, Vert.x, Mutiny,
 * CDI, or JAX-RS dependencies on its runtime/test classpath.
 */
class SpringHostStarterDependencyGuardTest {

    private static final List<String> FORBIDDEN_CLASSES = List.of(
        "io.quarkus.runtime.Quarkus",
        "io.quarkus.arc.Arc",
        "io.vertx.core.Vertx",
        "io.smallrye.mutiny.Uni",
        "io.smallrye.mutiny.Multi",
        "jakarta.enterprise.context.ApplicationScoped",
        "jakarta.ws.rs.Path",
        "org.eclipse.microprofile.config.Config"
    );

    @Test
    void starterHasZeroQuarkusVertxMutinyOrCdiClasses() {
        ClassLoader loader = SpringHostStarterDependencyGuardTest.class.getClassLoader();
        for (String className : FORBIDDEN_CLASSES) {
            boolean present = isClassPresent(loader, className);
            assertFalse(present, "Forbidden framework class found on spring-host-starter classpath: " + className);
        }
    }

    private static boolean isClassPresent(ClassLoader loader, String className) {
        try {
            Class.forName(className, false, loader);
            return true;
        } catch (ClassNotFoundException expected) {
            return false;
        }
    }
}
