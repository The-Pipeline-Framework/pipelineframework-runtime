package com.example.grpcsmoke.mapper;

import com.example.grpcsmoke.domain.PaymentRequest;
import com.example.grpcsmoke.grpc.PipelineTypes;
import org.pipelineframework.mapper.Mapper;
import org.springframework.stereotype.Component;

@Component
public class PaymentRequestMapper implements Mapper<PaymentRequest, PipelineTypes.PaymentRequest> {
    @Override
    public PaymentRequest fromExternal(PipelineTypes.PaymentRequest external) {
        return new PaymentRequest(external.getPaymentId(), external.getCents());
    }

    @Override
    public PipelineTypes.PaymentRequest toExternal(PaymentRequest domain) {
        return PipelineTypes.PaymentRequest.newBuilder()
            .setPaymentId(domain.paymentId()).setCents(domain.cents()).build();
    }
}
