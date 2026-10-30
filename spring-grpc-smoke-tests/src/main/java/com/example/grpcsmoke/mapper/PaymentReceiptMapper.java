package com.example.grpcsmoke.mapper;

import com.example.grpcsmoke.domain.PaymentReceipt;
import com.example.grpcsmoke.grpc.PipelineTypes;
import org.pipelineframework.mapper.Mapper;
import org.springframework.stereotype.Component;

@Component
public class PaymentReceiptMapper implements Mapper<PaymentReceipt, PipelineTypes.PaymentReceipt> {
    @Override
    public PaymentReceipt fromExternal(PipelineTypes.PaymentReceipt external) {
        return new PaymentReceipt(external.getPaymentId(), external.getStatus());
    }

    @Override
    public PipelineTypes.PaymentReceipt toExternal(PaymentReceipt domain) {
        return PipelineTypes.PaymentReceipt.newBuilder()
            .setPaymentId(domain.paymentId()).setStatus(domain.status()).build();
    }
}
