package com.example.grpcsmoke.service;

import com.example.grpcsmoke.domain.PaymentRequest;
import com.example.grpcsmoke.domain.ValidatedPayment;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.springframework.stereotype.Service;

@Service
public class ValidatePaymentService {
    public CompletionStage<ValidatedPayment> process(PaymentRequest request) {
        if (request.cents() <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("cents must be positive"));
        }
        return CompletableFuture.completedFuture(new ValidatedPayment(request.paymentId(), request.cents()));
    }
}
