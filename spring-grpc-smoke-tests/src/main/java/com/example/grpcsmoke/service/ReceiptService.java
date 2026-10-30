package com.example.grpcsmoke.service;

import com.example.grpcsmoke.domain.PaymentReceipt;
import com.example.grpcsmoke.domain.ValidatedPayment;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.springframework.stereotype.Service;

@Service
public class ReceiptService {
    public CompletionStage<PaymentReceipt> process(ValidatedPayment payment) {
        return CompletableFuture.completedFuture(new PaymentReceipt(payment.paymentId(), "accepted"));
    }
}
