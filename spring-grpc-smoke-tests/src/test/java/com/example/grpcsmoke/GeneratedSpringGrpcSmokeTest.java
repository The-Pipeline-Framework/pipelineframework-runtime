package com.example.grpcsmoke;

import com.example.grpcsmoke.grpc.PipelineServiceGrpc;
import com.example.grpcsmoke.grpc.PipelineTypes;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.grpc.test.autoconfigure.LocalGrpcServerPort;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(properties = "spring.grpc.server.port=0")
class GeneratedSpringGrpcSmokeTest {
    @LocalGrpcServerPort
    private int port;

    @Test
    void runsEveryLocalStepAndReturnsFinalType() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        try {
            var request = PipelineTypes.PaymentRequest.newBuilder()
                .setPaymentId("payment-1").setCents(125).build();
            PipelineTypes.PaymentReceipt response = PipelineServiceGrpc.newBlockingStub(channel).run(request);
            assertEquals("payment-1", response.getPaymentId());
            assertEquals("accepted", response.getStatus());
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void completesFailureWithGrpcError() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        try {
            var request = PipelineTypes.PaymentRequest.newBuilder()
                .setPaymentId("payment-2").setCents(0).build();
            StatusRuntimeException failure = assertThrows(StatusRuntimeException.class,
                () -> PipelineServiceGrpc.newBlockingStub(channel).run(request));
            assertEquals(Status.Code.INTERNAL, failure.getStatus().getCode());
        } finally {
            channel.shutdownNow();
        }
    }
}
