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

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

/**
 * Spring Boot auto-configuration that exposes ordinary Spring beans annotated with
 * {@link TpfFunction}, {@link TpfQueryConnector}, {@link TpfCommandConnector}, and
 * {@link TpfCommandReconciler} over ADR-0092 authenticated HTTP/1.1 endpoints.
 */
@AutoConfiguration
@EnableConfigurationProperties(TpfSpringHostProperties.class)
@ConditionalOnProperty(prefix = "tpf.host", name = "enabled", havingValue = "true", matchIfMissing = true)
public class TpfSpringHostAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public TpfHostRequestAuthenticator tpfHostRequestAuthenticator(
        TpfSpringHostProperties properties,
        Environment environment
    ) {
        String secret = properties.getSharedSecret();
        if (secret == null || secret.isBlank()) {
            String envSecret = environment.getProperty("TPF_FUNCTION_HOST_SECRET");
            secret = envSecret == null ? "" : envSecret;
        }
        return new TpfHostRequestAuthenticator(
            secret,
            properties.getMaxClockSkew(),
            properties.getMaxNonceCacheSize());
    }

    @Bean
    @ConditionalOnMissingBean
    public TpfHostRegistry tpfHostRegistry(ObjectProvider<TpfHostRegistryCustomizer> customizers) {
        TpfHostRegistry registry = new TpfHostRegistry();
        customizers.orderedStream().forEach(customizer -> customizer.customize(registry));
        return registry;
    }

    @Bean
    public SmartInitializingSingleton tpfSpringBeanRegistrarInitializer(
        ApplicationContext applicationContext,
        TpfHostRegistry registry,
        TpfSpringHostProperties properties
    ) {
        return () -> new TpfSpringBeanRegistrar(applicationContext, properties.getStepOutputTypes())
            .registerAll(registry);
    }

    @Bean
    @ConditionalOnMissingBean
    public TpfHostWebHandler tpfHostWebHandler(
        TpfHostRegistry registry,
        TpfHostRequestAuthenticator authenticator,
        TpfSpringHostProperties properties
    ) {
        return new TpfHostWebHandler(
            registry,
            authenticator,
            properties.getMaxRequestBytes(),
            properties.getMaxFrameBytes());
    }

    @Bean
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
    public RouterFunction<ServerResponse> tpfHostRouterFunction(TpfHostWebHandler handler) {
        return handler.routes();
    }
}
