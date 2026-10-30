package com.example.grpcsmoke.domain;

public record ValidatedPayment(String paymentId, int cents) {}
