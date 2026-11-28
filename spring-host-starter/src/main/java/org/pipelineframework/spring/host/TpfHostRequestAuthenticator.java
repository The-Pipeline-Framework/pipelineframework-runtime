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

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.pipelineframework.orchestrator.TransitionWorkerSignature;

/**
 * Verifies ADR-0092 HMAC-SHA256 request signatures (with timestamp skew and replay-nonce
 * protection) and computes mutual loopback connector authentication challenge proofs.
 */
public final class TpfHostRequestAuthenticator {

    public static final String STREAMING_BODY_DIGEST_SENTINEL = "STREAMING-NDJSON-V1";
    public static final String LOOPBACK_PROOF_DOMAIN = "tpf-connector-listener-auth-v1\n";

    private static final HexFormat HEX = HexFormat.of();

    private final String sharedSecret;
    private final Duration maxClockSkew;
    private final int maxNonceCacheSize;
    private final Clock clock;
    private final Object nonceLock = new Object();
    private final ConcurrentHashMap<String, Long> seenNonces = new ConcurrentHashMap<>();

    public TpfHostRequestAuthenticator(String sharedSecret, Duration maxClockSkew, int maxNonceCacheSize) {
        this(sharedSecret, maxClockSkew, maxNonceCacheSize, Clock.systemUTC());
    }

    TpfHostRequestAuthenticator(String sharedSecret, Duration maxClockSkew, int maxNonceCacheSize, Clock clock) {
        this.sharedSecret = sharedSecret == null ? "" : sharedSecret;
        Objects.requireNonNull(maxClockSkew, "maxClockSkew must not be null");
        if (maxClockSkew.isZero() || maxClockSkew.isNegative()) {
            throw new IllegalArgumentException("maxClockSkew must be positive");
        }
        if (maxNonceCacheSize <= 0) {
            throw new IllegalArgumentException("maxNonceCacheSize must be positive");
        }
        this.maxClockSkew = maxClockSkew;
        this.maxNonceCacheSize = maxNonceCacheSize;
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * Verifies an incoming HTTP request signature.
     *
     * @return empty if authorized, or a safe rejection reason if unauthorized
     */
    public Optional<String> verify(
        String method,
        String path,
        String timestamp,
        String nonce,
        String signature,
        byte[] bodyForSignature
    ) {
        if (sharedSecret.isBlank()) {
            return Optional.of("shared secret is not configured");
        }
        if (timestamp == null || timestamp.isBlank()
            || nonce == null || nonce.isBlank()
            || signature == null || signature.isBlank()) {
            return Optional.of("missing required signature headers");
        }
        long timestampMs;
        try {
            timestampMs = TransitionWorkerSignature.parseTimestamp(timestamp);
        } catch (IllegalArgumentException invalid) {
            return Optional.of("invalid signature timestamp");
        }
        long nowMs = clock.millis();
        long skewMs = maxClockSkew.toMillis();
        if (Math.abs(nowMs - timestampMs) > skewMs) {
            return Optional.of("signature timestamp outside allowed skew window");
        }
        String expected = TransitionWorkerSignature.sign(
            sharedSecret, method, path, timestamp, nonce, bodyForSignature);
        if (!TransitionWorkerSignature.matches(expected, signature)) {
            return Optional.of("invalid request signature");
        }
        long expiresAtMs = timestampMs > Long.MAX_VALUE - skewMs ? Long.MAX_VALUE : timestampMs + skewMs;
        synchronized (nonceLock) {
            evictExpiredNonces(nowMs);
            if (seenNonces.containsKey(nonce)) {
                return Optional.of("replayed request nonce");
            }
            if (seenNonces.size() >= maxNonceCacheSize) {
                return Optional.of("nonce replay cache capacity reached");
            }
            seenNonces.put(nonce, expiresAtMs);
        }
        return Optional.empty();
    }

    /**
     * Computes the mutual loopback authentication challenge proof expected by
     * {@code LoopbackConnectorAuthenticator}.
     */
    public String computeLoopbackChallengeProof(String challenge) {
        if (sharedSecret.isBlank()) {
            throw new IllegalStateException("shared secret is not configured");
        }
        if (challenge == null || challenge.isBlank()) {
            throw new IllegalArgumentException("challenge must not be blank");
        }
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(sharedSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] digest = mac.doFinal((LOOPBACK_PROOF_DOMAIN + challenge).getBytes(StandardCharsets.UTF_8));
            return HEX.formatHex(digest);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is unavailable", e);
        }
    }

    private void evictExpiredNonces(long nowMs) {
        if (seenNonces.isEmpty()) {
            return;
        }
        // Retain nonces through the inclusive expiry boundary (expiresAtMs == nowMs).
        seenNonces.entrySet().removeIf(entry -> entry.getValue() < nowMs);
    }
}
