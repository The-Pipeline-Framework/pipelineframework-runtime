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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import org.reactivestreams.Publisher;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Discovers {@link TpfFunction}, {@link TpfQueryConnector}, {@link TpfCommandConnector}, and
 * {@link TpfCommandReconciler} methods on Spring beans and binds them into {@link TpfHostRegistry}.
 */
public final class TpfSpringBeanRegistrar {

    private final ApplicationContext applicationContext;
    private final Map<String, String> configuredStepOutputTypes;

    public TpfSpringBeanRegistrar(ApplicationContext applicationContext, Map<String, String> configuredStepOutputTypes) {
        this.applicationContext = Objects.requireNonNull(applicationContext, "applicationContext must not be null");
        this.configuredStepOutputTypes = configuredStepOutputTypes == null
            ? Map.of()
            : Map.copyOf(configuredStepOutputTypes);
    }

    public void registerAll(TpfHostRegistry registry) {
        Objects.requireNonNull(registry, "registry must not be null");
        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            Class<?> beanType = applicationContext.getType(beanName);
            if (beanType == null || isInfrastructureBean(beanType)) {
                continue;
            }
            Object bean;
            try {
                bean = applicationContext.getBean(beanName);
            } catch (RuntimeException unavailable) {
                continue;
            }
            Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);
            registerBean(bean, targetClass, registry);
        }
    }

    void registerBean(Object bean, Class<?> targetClass, TpfHostRegistry registry) {
        Map<Method, TpfFunction> functions = MethodIntrospector.selectMethods(
            targetClass,
            (MethodIntrospector.MetadataLookup<TpfFunction>) method ->
                AnnotatedElementUtils.findMergedAnnotation(method, TpfFunction.class));
        functions.forEach((method, annotation) -> registerFunction(bean, method, annotation, registry));

        Map<Method, TpfQueryConnector> queries = MethodIntrospector.selectMethods(
            targetClass,
            (MethodIntrospector.MetadataLookup<TpfQueryConnector>) method ->
                AnnotatedElementUtils.findMergedAnnotation(method, TpfQueryConnector.class));
        queries.forEach((method, annotation) -> registerQuery(bean, method, annotation, registry));

        Map<Method, TpfCommandConnector> commands = MethodIntrospector.selectMethods(
            targetClass,
            (MethodIntrospector.MetadataLookup<TpfCommandConnector>) method ->
                AnnotatedElementUtils.findMergedAnnotation(method, TpfCommandConnector.class));
        commands.forEach((method, annotation) -> registerCommand(bean, method, annotation, registry));

        Map<Method, TpfCommandReconciler> reconcilers = MethodIntrospector.selectMethods(
            targetClass,
            (MethodIntrospector.MetadataLookup<TpfCommandReconciler>) method ->
                AnnotatedElementUtils.findMergedAnnotation(method, TpfCommandReconciler.class));
        reconcilers.forEach((method, annotation) -> registerReconciler(bean, method, annotation, registry));
    }

    private void registerFunction(Object bean, Method targetMethod, TpfFunction annotation, TpfHostRegistry registry) {
        Method invocable = AopUtils.selectInvocableMethod(targetMethod, bean.getClass());
        invocable.setAccessible(true);

        String stepId = annotation.stepId().isBlank() ? toKebabCase(targetMethod.getName()) : annotation.stepId().trim();
        Type[] paramTypes = targetMethod.getGenericParameterTypes();
        if (paramTypes.length < 1 || paramTypes.length > 2) {
            throw new IllegalStateException("@TpfFunction method " + describe(targetMethod)
                + " must declare 1 input parameter and an optional trailing TpfFunctionContext parameter");
        }
        boolean acceptsContext = false;
        if (paramTypes.length == 2) {
            if (!TpfFunctionContext.class.equals(rawClass(paramTypes[1]))) {
                throw new IllegalStateException("@TpfFunction method " + describe(targetMethod)
                    + " second parameter must be TpfFunctionContext");
            }
            acceptsContext = true;
        }

        Type inputGenericType = paramTypes[0];
        Class<?> inputRaw = rawClass(inputGenericType);
        boolean manyInput = isStreamingInput(inputRaw);

        Type returnGenericType = targetMethod.getGenericReturnType();
        Class<?> returnRaw = rawClass(returnGenericType);
        if (returnRaw == void.class || returnRaw == Void.class) {
            throw new IllegalStateException("@TpfFunction method " + describe(targetMethod) + " must not return void");
        }
        boolean manyOutput = isStreamingOutput(returnRaw);

        TpfCardinality inferred = resolveCardinality(manyInput, manyOutput);
        TpfCardinality effective = annotation.cardinality() == TpfCardinality.AUTO
            ? inferred
            : annotation.cardinality();
        if (effective != inferred) {
            throw new IllegalStateException("@TpfFunction method " + describe(targetMethod)
                + " declares cardinality " + effective + " but signature matches " + inferred);
        }

        JavaType inputElementType = manyInput
            ? extractSingleTypeArg(inputGenericType, targetMethod, "input stream")
            : toJavaType(inputGenericType);
        JavaType outputElementType = extractOutputElementType(returnGenericType, returnRaw, targetMethod);
        Optional<String> explicitOutputTypeId = resolveExplicitOutputTypeId(stepId, annotation.outputTypeId());
        String streamingOutputTypeId = explicitOutputTypeId.orElseGet(() -> outputElementType.getRawClass().getSimpleName());
        boolean passContext = acceptsContext;

        switch (effective) {
            case ONE_TO_ONE -> registry.registerOneToOne(new TpfHostRegistry.OneToOneHandler(
                stepId,
                explicitOutputTypeId,
                (inputNode, ctx) -> invokeOneToOne(bean, invocable, inputNode, inputElementType, ctx, passContext, returnRaw)));
            case ONE_TO_MANY -> registry.registerOneToMany(new TpfHostRegistry.OneToManyHandler(
                stepId,
                explicitOutputTypeId,
                (inputNode, ctx) -> invokeOneToMany(bean, invocable, inputNode, inputElementType, ctx, passContext, returnRaw)));
            case MANY_TO_ONE -> registry.registerManyToOne(new TpfHostRegistry.ManyToOneHandler(
                stepId,
                streamingOutputTypeId,
                (inputFlux, ctx) -> invokeManyToOne(bean, invocable, inputFlux, inputRaw, inputElementType, ctx, passContext, returnRaw)));
            case MANY_TO_MANY -> registry.registerManyToMany(new TpfHostRegistry.ManyToManyHandler(
                stepId,
                streamingOutputTypeId,
                (inputFlux, ctx) -> invokeManyToMany(bean, invocable, inputFlux, inputRaw, inputElementType, ctx, passContext, returnRaw)));
            case AUTO -> throw new IllegalStateException("Unresolved AUTO cardinality");
        }
    }

    private void registerQuery(Object bean, Method targetMethod, TpfQueryConnector annotation, TpfHostRegistry registry) {
        Method invocable = AopUtils.selectInvocableMethod(targetMethod, bean.getClass());
        invocable.setAccessible(true);

        String operation = annotation.operation().isBlank()
            ? toKebabCase(targetMethod.getName())
            : annotation.operation().trim();
        Type[] paramTypes = targetMethod.getGenericParameterTypes();
        if (paramTypes.length < 1 || paramTypes.length > 2) {
            throw new IllegalStateException("@TpfQueryConnector method " + describe(targetMethod)
                + " must declare 1 input parameter and an optional trailing TpfConnectorContext parameter");
        }
        boolean acceptsContext = false;
        if (paramTypes.length == 2) {
            if (!TpfConnectorContext.class.equals(rawClass(paramTypes[1]))) {
                throw new IllegalStateException("@TpfQueryConnector method " + describe(targetMethod)
                    + " second parameter must be TpfConnectorContext");
            }
            acceptsContext = true;
        }
        JavaType inputType = toJavaType(paramTypes[0]);
        Class<?> returnRaw = rawClass(targetMethod.getGenericReturnType());
        boolean passContext = acceptsContext;

        registry.registerQuery(new TpfHostRegistry.QueryHandler(
            operation,
            (inputNode, ctx) -> invokeAsyncOrBlocking(bean, invocable,
                () -> buildArgs(TpfHostJson.decodeValue(inputNode, inputType), ctx, passContext),
                returnRaw)
                .map(TpfSpringBeanRegistrar::normalizeQueryResult)));
    }

    private void registerCommand(Object bean, Method targetMethod, TpfCommandConnector annotation, TpfHostRegistry registry) {
        Method invocable = AopUtils.selectInvocableMethod(targetMethod, bean.getClass());
        invocable.setAccessible(true);

        String operation = annotation.operation().isBlank()
            ? toKebabCase(targetMethod.getName())
            : annotation.operation().trim();
        Type[] paramTypes = targetMethod.getGenericParameterTypes();
        if (paramTypes.length < 1 || paramTypes.length > 2) {
            throw new IllegalStateException("@TpfCommandConnector method " + describe(targetMethod)
                + " must declare 1 input parameter and an optional trailing TpfCommandContext parameter");
        }
        boolean acceptsContext = false;
        if (paramTypes.length == 2) {
            if (!TpfCommandContext.class.equals(rawClass(paramTypes[1]))) {
                throw new IllegalStateException("@TpfCommandConnector method " + describe(targetMethod)
                    + " second parameter must be TpfCommandContext");
            }
            acceptsContext = true;
        }
        JavaType inputType = toJavaType(paramTypes[0]);
        Class<?> returnRaw = rawClass(targetMethod.getGenericReturnType());
        boolean passContext = acceptsContext;

        registry.registerCommand(new TpfHostRegistry.CommandHandler(
            operation,
            (inputNode, ctx) -> invokeAsyncOrBlocking(bean, invocable,
                () -> buildArgs(TpfHostJson.decodeValue(inputNode, inputType), ctx, passContext),
                returnRaw)
                .map(TpfSpringBeanRegistrar::normalizeCommandResult)));
    }

    private void registerReconciler(Object bean, Method targetMethod, TpfCommandReconciler annotation, TpfHostRegistry registry) {
        Method invocable = AopUtils.selectInvocableMethod(targetMethod, bean.getClass());
        invocable.setAccessible(true);

        String operation = annotation.operation().trim();
        if (operation.isBlank()) {
            throw new IllegalStateException("@TpfCommandReconciler on " + describe(targetMethod)
                + " must declare a non-blank operation");
        }
        Type[] paramTypes = targetMethod.getGenericParameterTypes();
        if (paramTypes.length != 1 || !TpfReconcileContext.class.equals(rawClass(paramTypes[0]))) {
            throw new IllegalStateException("@TpfCommandReconciler method " + describe(targetMethod)
                + " must declare exactly 1 parameter of type TpfReconcileContext");
        }
        Class<?> returnRaw = rawClass(targetMethod.getGenericReturnType());

        registry.registerReconciler(new TpfHostRegistry.ReconcilerHandler(
            operation,
            ctx -> invokeAsyncOrBlocking(bean, invocable, () -> new Object[] {ctx}, returnRaw)
                .map(TpfSpringBeanRegistrar::normalizeReconcileResult)));
    }

    private Mono<JsonNode> invokeOneToOne(
        Object bean,
        Method invocable,
        JsonNode inputNode,
        JavaType inputType,
        TpfFunctionContext ctx,
        boolean passContext,
        Class<?> returnRaw
    ) {
        return invokeAsyncOrBlocking(bean, invocable,
            () -> buildArgs(TpfHostJson.decodeValue(inputNode, inputType), ctx, passContext),
            returnRaw)
            .map(TpfHostJson::encodeValue);
    }

    private Flux<JsonNode> invokeOneToMany(
        Object bean,
        Method invocable,
        JsonNode inputNode,
        JavaType inputType,
        TpfFunctionContext ctx,
        boolean passContext,
        Class<?> returnRaw
    ) {
        if (Flux.class.isAssignableFrom(returnRaw) || Publisher.class.isAssignableFrom(returnRaw)) {
            return Flux.defer(() -> {
                try {
                    Object[] args = buildArgs(TpfHostJson.decodeValue(inputNode, inputType), ctx, passContext);
                    Object result = invokeMethod(bean, invocable, args);
                    if (result == null) {
                        return Flux.error(new IllegalStateException("Streaming @TpfFunction returned null Publisher"));
                    }
                    return Flux.from((Publisher<?>) result).map(TpfHostJson::encodeValue);
                } catch (Throwable failure) {
                    return Flux.error(failure);
                }
            });
        }
        if (Flow.Publisher.class.isAssignableFrom(returnRaw)) {
            return Flux.defer(() -> {
                try {
                    Object[] args = buildArgs(TpfHostJson.decodeValue(inputNode, inputType), ctx, passContext);
                    Object result = invokeMethod(bean, invocable, args);
                    if (result == null) {
                        return Flux.error(new IllegalStateException("Streaming @TpfFunction returned null Flow.Publisher"));
                    }
                    return JdkFlowAdapter.flowPublisherToFlux((Flow.Publisher<?>) result).map(TpfHostJson::encodeValue);
                } catch (Throwable failure) {
                    return Flux.error(failure);
                }
            });
        }
        if (Stream.class.isAssignableFrom(returnRaw)) {
            return Flux.using(
                () -> {
                    Object[] args = buildArgs(TpfHostJson.decodeValue(inputNode, inputType), ctx, passContext);
                    Object result = invokeMethod(bean, invocable, args);
                    if (result == null) {
                        throw new IllegalStateException("Streaming @TpfFunction returned null Stream");
                    }
                    return (Stream<?>) result;
                },
                stream -> Flux.fromStream(stream).map(TpfHostJson::encodeValue),
                Stream::close
            ).subscribeOn(Schedulers.boundedElastic());
        }
        return Flux.defer(() -> {
            try {
                Object[] args = buildArgs(TpfHostJson.decodeValue(inputNode, inputType), ctx, passContext);
                Object result = invokeMethod(bean, invocable, args);
                if (result == null) {
                    return Flux.error(new IllegalStateException("Streaming @TpfFunction returned null Iterable"));
                }
                return Flux.fromIterable((Iterable<?>) result).map(TpfHostJson::encodeValue);
            } catch (Throwable failure) {
                return Flux.error(failure);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<JsonNode> invokeManyToOne(
        Object bean,
        Method invocable,
        Flux<JsonNode> inputFlux,
        Class<?> inputRaw,
        JavaType inputElementType,
        TpfFunctionContext ctx,
        boolean passContext,
        Class<?> returnRaw
    ) {
        Flux<Object> decodedFlux = inputFlux.flatMapSequential(node -> {
            try {
                return Mono.just(TpfHostJson.decodeValue(node, inputElementType));
            } catch (Throwable decodeErr) {
                return Mono.error(decodeErr);
            }
        }, 1);

        if (Stream.class.isAssignableFrom(inputRaw)) {
            return Mono.fromCallable(() -> {
                Stream<Object> stream = decodedFlux.toStream();
                Object[] args = buildArgs(stream, ctx, passContext);
                Object result = invokeMethod(bean, invocable, args);
                if (result == null) {
                    throw new IllegalStateException("Reducer @TpfFunction returned null");
                }
                return TpfHostJson.encodeValue(result);
            }).subscribeOn(Schedulers.boundedElastic());
        }

        Object streamArg = adaptInputStream(decodedFlux, inputRaw);
        return invokeAsyncOrBlocking(bean, invocable, () -> buildArgs(streamArg, ctx, passContext), returnRaw)
            .map(TpfHostJson::encodeValue);
    }

    private Flux<JsonNode> invokeManyToMany(
        Object bean,
        Method invocable,
        Flux<JsonNode> inputFlux,
        Class<?> inputRaw,
        JavaType inputElementType,
        TpfFunctionContext ctx,
        boolean passContext,
        Class<?> returnRaw
    ) {
        if (!Flux.class.isAssignableFrom(inputRaw) && !Publisher.class.isAssignableFrom(inputRaw)
            && !Flow.Publisher.class.isAssignableFrom(inputRaw)) {
            return Flux.error(new IllegalStateException(
                "MANY_TO_MANY @TpfFunction requires Flux/Publisher/Flow.Publisher input"));
        }
        Flux<Object> decodedFlux = inputFlux.concatMap(node -> {
            try {
                return Mono.just(TpfHostJson.decodeValue(node, inputElementType));
            } catch (Throwable decodeErr) {
                return Mono.error(decodeErr);
            }
        });
        Object streamArg = adaptInputStream(decodedFlux, inputRaw);
        return Flux.defer(() -> {
            try {
                Object result = invokeMethod(bean, invocable, buildArgs(streamArg, ctx, passContext));
                if (result == null) {
                    return Flux.error(new IllegalStateException("Duplex @TpfFunction returned null stream"));
                }
                if (Flow.Publisher.class.isAssignableFrom(returnRaw)) {
                    return JdkFlowAdapter.flowPublisherToFlux((Flow.Publisher<?>) result).map(TpfHostJson::encodeValue);
                }
                return Flux.from((Publisher<?>) result).map(TpfHostJson::encodeValue);
            } catch (Throwable failure) {
                return Flux.error(failure);
            }
        });
    }

    private static Object adaptInputStream(Flux<Object> decodedFlux, Class<?> inputRaw) {
        if (Flow.Publisher.class.isAssignableFrom(inputRaw)) {
            return JdkFlowAdapter.publisherToFlowPublisher(decodedFlux);
        }
        return decodedFlux;
    }

    @FunctionalInterface
    private interface ArgSupplier {
        Object[] get() throws Exception;
    }

    private static Mono<Object> invokeAsyncOrBlocking(
        Object bean,
        Method invocable,
        ArgSupplier argSupplier,
        Class<?> returnRaw
    ) {
        if (Mono.class.isAssignableFrom(returnRaw)) {
            return Mono.defer(() -> {
                try {
                    Object result = invokeMethod(bean, invocable, argSupplier.get());
                    if (result == null) {
                        return Mono.error(new IllegalStateException("Reactive TPF handler returned null Mono"));
                    }
                    return ((Mono<?>) result).map(v -> (Object) v);
                } catch (Throwable failure) {
                    return Mono.error(failure);
                }
            });
        }
        if (CompletionStage.class.isAssignableFrom(returnRaw)) {
            return Mono.defer(() -> {
                try {
                    Object result = invokeMethod(bean, invocable, argSupplier.get());
                    if (result == null) {
                        return Mono.error(new IllegalStateException("Async TPF handler returned null CompletionStage"));
                    }
                    return Mono.fromCompletionStage((CompletionStage<?>) result).map(v -> (Object) v);
                } catch (Throwable failure) {
                    return Mono.error(failure);
                }
            });
        }
        return Mono.fromCallable(() -> {
            Object result = invokeMethod(bean, invocable, argSupplier.get());
            if (result == null) {
                throw new IllegalStateException("Synchronous TPF handler returned null");
            }
            return result;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private static Object invokeMethod(Object bean, Method invocable, Object[] args) throws Exception {
        try {
            return invocable.invoke(bean, args);
        } catch (InvocationTargetException wrapped) {
            Throwable target = wrapped.getTargetException();
            if (target instanceof Exception ex) {
                throw ex;
            }
            if (target instanceof Error err) {
                throw err;
            }
            throw new IllegalStateException("Invocation failed", target == null ? wrapped : target);
        }
    }

    private static Object[] buildArgs(Object first, Object context, boolean passContext) {
        return passContext ? new Object[] {first, context} : new Object[] {first};
    }

    @SuppressWarnings("unchecked")
    private static TpfQueryResult<JsonNode> normalizeQueryResult(Object raw) {
        if (raw instanceof TpfQueryResult<?> result) {
            return switch (result) {
                case TpfQueryResult.Found<?> found -> TpfQueryResult.found(TpfHostJson.encodeValue(found.payload()));
                case TpfQueryResult.NotFound<?> notFound -> TpfQueryResult.notFound(notFound.code());
                case TpfQueryResult.AuthRequired<?> auth -> TpfQueryResult.authRequired(auth.code());
                case TpfQueryResult.TemporarilyUnavailable<?> unavail -> TpfQueryResult.temporarilyUnavailable(unavail.code());
                case TpfQueryResult.TerminalFailure<?> term -> TpfQueryResult.terminalFailure(term.code());
            };
        }
        return TpfQueryResult.found(TpfHostJson.encodeValue(raw));
    }

    private static TpfCommandResult<JsonNode> normalizeCommandResult(Object raw) {
        if (raw instanceof TpfCommandResult<?> result) {
            return switch (result) {
                case TpfCommandResult.Succeeded<?> ok -> new TpfCommandResult.Succeeded<>(
                    TpfHostJson.encodeValue(ok.payload()), ok.receiptReference());
                case TpfCommandResult.RetryableFailure<?> retry -> TpfCommandResult.retryableFailure(retry.code());
                case TpfCommandResult.TerminalFailure<?> term -> TpfCommandResult.terminalFailure(term.code());
                case TpfCommandResult.Ambiguous<?> amb -> TpfCommandResult.ambiguous(amb.code());
            };
        }
        return TpfCommandResult.succeeded(TpfHostJson.encodeValue(raw));
    }

    private static TpfReconcileResult<JsonNode> normalizeReconcileResult(Object raw) {
        if (raw instanceof TpfReconcileResult<?> result) {
            return switch (result) {
                case TpfReconcileResult.ConfirmedSucceeded<?> ok -> TpfReconcileResult.confirmedSucceeded(
                    TpfHostJson.encodeValue(ok.payload()), ok.receiptReference());
                case TpfReconcileResult.Unresolved<?> unresolved -> TpfReconcileResult.unresolved(unresolved.code());
            };
        }
        throw new IllegalStateException("@TpfCommandReconciler method must return TpfReconcileResult");
    }

    private Optional<String> resolveExplicitOutputTypeId(String stepId, String annotationValue) {
        if (annotationValue != null && !annotationValue.isBlank()) {
            return Optional.of(annotationValue.trim());
        }
        String configured = configuredStepOutputTypes.get(stepId);
        if (configured != null && !configured.isBlank()) {
            return Optional.of(configured.trim());
        }
        return Optional.empty();
    }

    private static boolean isStreamingInput(Class<?> raw) {
        return Flux.class.isAssignableFrom(raw)
            || Publisher.class.isAssignableFrom(raw)
            || Flow.Publisher.class.isAssignableFrom(raw)
            || Stream.class.isAssignableFrom(raw);
    }

    private static boolean isStreamingOutput(Class<?> raw) {
        return Flux.class.isAssignableFrom(raw)
            || (Publisher.class.isAssignableFrom(raw) && !Mono.class.isAssignableFrom(raw))
            || Flow.Publisher.class.isAssignableFrom(raw)
            || Stream.class.isAssignableFrom(raw)
            || Iterable.class.isAssignableFrom(raw);
    }

    private static TpfCardinality resolveCardinality(boolean manyInput, boolean manyOutput) {
        if (!manyInput && !manyOutput) {
            return TpfCardinality.ONE_TO_ONE;
        }
        if (!manyInput) {
            return TpfCardinality.ONE_TO_MANY;
        }
        if (!manyOutput) {
            return TpfCardinality.MANY_TO_ONE;
        }
        return TpfCardinality.MANY_TO_MANY;
    }

    private static JavaType extractOutputElementType(Type returnGenericType, Class<?> returnRaw, Method method) {
        if (Mono.class.isAssignableFrom(returnRaw)
            || CompletionStage.class.isAssignableFrom(returnRaw)
            || isStreamingOutput(returnRaw)) {
            return extractSingleTypeArg(returnGenericType, method, "return wrapper");
        }
        return toJavaType(returnGenericType);
    }

    private static JavaType extractSingleTypeArg(Type containerType, Method method, String role) {
        if (containerType instanceof ParameterizedType pt && pt.getActualTypeArguments().length == 1) {
            return toJavaType(pt.getActualTypeArguments()[0]);
        }
        throw new IllegalStateException("Method " + describe(method) + " must parameterize its " + role + " type");
    }

    private static JavaType toJavaType(Type type) {
        return TpfHostJson.mapper().getTypeFactory().constructType(type);
    }

    private static Class<?> rawClass(Type type) {
        if (type instanceof Class<?> cls) {
            return cls;
        }
        if (type instanceof ParameterizedType pt && pt.getRawType() instanceof Class<?> raw) {
            return raw;
        }
        return toJavaType(type).getRawClass();
    }

    private static boolean isInfrastructureBean(Class<?> beanType) {
        String name = beanType.getName();
        return name.startsWith("org.springframework.") || name.startsWith("reactor.");
    }

    private static String describe(Method method) {
        return method.getDeclaringClass().getSimpleName() + "#" + method.getName();
    }

    static String toKebabCase(String methodName) {
        StringBuilder sb = new StringBuilder(methodName.length() + 4);
        for (int i = 0; i < methodName.length(); i++) {
            char c = methodName.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    sb.append('-');
                }
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString().toLowerCase(Locale.ROOT);
    }
}
