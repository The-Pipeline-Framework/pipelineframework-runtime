package com.example.grpcpeer;

import com.example.grpcclient.grpc.PipelineTypes;
import com.example.grpcclient.grpc.ProcessRemoteServiceGrpc;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import org.pipelineframework.runtime.core.TpfContextHeaders;
import org.pipelineframework.runtime.spring.SpringRuntimeAdaptersAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.server.GlobalServerInterceptor;

@Configuration(proxyBeanMethods = false)
@EnableAutoConfiguration(exclude = SpringRuntimeAdaptersAutoConfiguration.class)
public class PeerApplication {
    private static final Context.Key<Seen> SEEN = Context.key("smoke-tpf-metadata");

    @Bean
    public PeerService peerService() { return new PeerService(); }

    @Bean
    @GlobalServerInterceptor
    public ServerInterceptor captureMetadata() {
        return new ServerInterceptor() {
            @Override
            public <I, O> ServerCall.Listener<I> interceptCall(
                    ServerCall<I, O> call, Metadata metadata, ServerCallHandler<I, O> next) {
                return Contexts.interceptCall(Context.current().withValue(SEEN, new Seen(
                    header(metadata, TpfContextHeaders.VERSION), header(metadata, TpfContextHeaders.REPLAY),
                    header(metadata, TpfContextHeaders.CACHE_POLICY))), call, metadata, next);
            }
        };
    }

    private Optional<String> header(Metadata metadata, String name) {
        return Optional.ofNullable(metadata.get(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER)));
    }

    public record Seen(Optional<String> version, Optional<String> replay, Optional<String> cache) {}

    public static class PeerService extends ProcessRemoteServiceGrpc.ProcessRemoteServiceImplBase {
        public final ConcurrentMap<String, Seen> seen = new ConcurrentHashMap<>();
        public final CountDownLatch started = new CountDownLatch(1);
        public final CountDownLatch cancelled = new CountDownLatch(1);

        @Override
        public void remoteProcess(PipelineTypes.Prepared request, StreamObserver<PipelineTypes.RemoteReply> observer) {
            seen.put(request.getValue(), SEEN.get());
            if (request.getValue().contains("rpc-failure")) {
                observer.onError(Status.INVALID_ARGUMENT.withDescription("downstream rejected input").asRuntimeException());
            } else if (request.getValue().contains("cancel-call")) {
                ((ServerCallStreamObserver<PipelineTypes.RemoteReply>) observer).setOnCancelHandler(cancelled::countDown);
                started.countDown();
            } else {
                observer.onNext(PipelineTypes.RemoteReply.newBuilder().setValue(request.getValue() + "|remote").build());
                observer.onCompleted();
            }
        }
    }
}
