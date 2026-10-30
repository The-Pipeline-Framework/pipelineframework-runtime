package com.example.grpcsmoke.domain;

public record PaymentRequest(String paymentId, int cents) {}
