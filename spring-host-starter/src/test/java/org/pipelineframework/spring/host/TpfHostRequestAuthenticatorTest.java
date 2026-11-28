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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.TransitionWorkerSignature;

class TpfHostRequestAuthenticatorTest {

    private static final String SECRET = "unit-test-shared-secret-0123456789";

    @Test
    void acceptsValidSignatureAndRejectsReplayedNonce() {
        Instant now = Instant.parse("2026-11-28T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        TpfHostRequestAuthenticator auth = new TpfHostRequestAuthenticator(SECRET, Duration.ofMinutes(5), 16, clock);

        byte[] body = "{\"payload\":{\"id\":\"1\"}}".getBytes(StandardCharsets.UTF_8);
        String ts = now.toString();
        String nonce = "nonce-1";
        String sig = TransitionWorkerSignature.sign(SECRET, "POST", "/tpf/functions/step-a", ts, nonce, body);

        assertEquals(Optional.empty(), auth.verify("POST", "/tpf/functions/step-a", ts, nonce, sig, body));
        Optional<String> replay = auth.verify("POST", "/tpf/functions/step-a", ts, nonce, sig, body);
        assertTrue(replay.isPresent());
        assertTrue(replay.orElseThrow().contains("replayed"));
    }

    @Test
    void rejectsWrongSecretExpiredTimestampAndBlankSecretConfig() {
        Instant now = Instant.parse("2026-11-28T12:00:00Z");
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        TpfHostRequestAuthenticator auth = new TpfHostRequestAuthenticator(SECRET, Duration.ofMinutes(2), 16, clock);
        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);

        String wrongSig = TransitionWorkerSignature.sign("wrong-secret", "POST", "/tpf/functions/s", now.toString(), "n1", body);
        assertTrue(auth.verify("POST", "/tpf/functions/s", now.toString(), "n1", wrongSig, body).isPresent());

        String oldTs = now.minus(Duration.ofMinutes(5)).toString();
        String oldSig = TransitionWorkerSignature.sign(SECRET, "POST", "/tpf/functions/s", oldTs, "n2", body);
        assertTrue(auth.verify("POST", "/tpf/functions/s", oldTs, "n2", oldSig, body).isPresent());

        TpfHostRequestAuthenticator unconfigured = new TpfHostRequestAuthenticator("", Duration.ofMinutes(5), 16, clock);
        String validSig = TransitionWorkerSignature.sign(SECRET, "POST", "/tpf/functions/s", now.toString(), "n3", body);
        assertTrue(unconfigured.verify("POST", "/tpf/functions/s", now.toString(), "n3", validSig, body).isPresent());
    }

    @Test
    void computesDeterministicLoopbackChallengeProof() {
        TpfHostRequestAuthenticator auth = new TpfHostRequestAuthenticator(SECRET, Duration.ofMinutes(5), 16);
        String proof1 = auth.computeLoopbackChallengeProof("abcdef0123456789");
        String proof2 = auth.computeLoopbackChallengeProof("abcdef0123456789");
        assertEquals(64, proof1.length());
        assertEquals(proof1, proof2);
    }
}
