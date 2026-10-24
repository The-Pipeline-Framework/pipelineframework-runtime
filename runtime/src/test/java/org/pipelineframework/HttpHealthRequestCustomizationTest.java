package org.pipelineframework;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import com.sun.net.httpserver.HttpsConfigurator;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class HttpHealthRequestCustomizationTest {
    @TempDir static Path directory;
    private static SSLContext tls;
    private final AtomicInteger received = new AtomicInteger();

    @BeforeAll static void fixtureTls() throws Exception {
        Path store = directory.resolve("fixture.p12");
        var tool = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
            "-genkeypair", "-alias", "fixture", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
            "-dname", "CN=localhost", "-ext", "SAN=IP:127.0.0.1,DNS:localhost", "-storetype", "PKCS12",
            "-keystore", store.toString(), "-storepass", "fixture-only", "-keypass", "fixture-only")
            .redirectErrorStream(true).start();
        tool.getOutputStream().close();
        assertTrue(tool.waitFor(30, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(0, tool.exitValue(), new String(tool.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
        var keys = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(store)) { keys.load(in, "fixture-only".toCharArray()); }
        var km = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        km.init(keys, "fixture-only".toCharArray());
        var tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tm.init(keys);
        tls = SSLContext.getInstance("TLS");
        tls.init(km.getKeyManagers(), tm.getTrustManagers(), new java.security.SecureRandom());
    }

    private boolean probe(HealthCheckService service, int status, boolean secure) throws Exception {
        var address = new InetSocketAddress("127.0.0.1", 0);
        HttpServer server;
        if (secure) {
            var https = HttpsServer.create(address, 0);
            https.setHttpsConfigurator(new HttpsConfigurator(tls));
            server = https;
        } else { server = HttpServer.create(address, 0); }
        server.createContext("/q/health", exchange -> {
            received.incrementAndGet();
            int result = "fixture-health-auth".equals(exchange.getRequestHeaders().getFirst("Authorization")) ? status : 403;
            exchange.sendResponseHeaders(result, -1);
            exchange.close();
        });
        server.start();
        try {
            var uri = URI.create((secure ? "https" : "http") + "://127.0.0.1:" + server.getAddress().getPort() + "/q/health");
            var method = HealthCheckService.class.getDeclaredMethod("checkHttpServiceHealth",
                String.class, String.class, URI.class, String.class, Supplier.class);
            method.setAccessible(true);
            return (boolean) method.invoke(service, "REST", "fixture", uri, uri.toString(), (Supplier<SSLContext>) () -> tls);
        } finally { server.stop(0); }
    }

    private HealthCheckService authorized() { return authorized("Authorization"); }

    private HealthCheckService authorized(String header) {
        return new HealthCheckService() {
            protected HttpRequest customizeHttpHealthRequest(String type, String name, HttpRequest request) {
                assertEquals("REST", type);
                assertEquals("fixture", name);
                return HttpRequest.newBuilder(request, (n, v) -> true)
                    .header(header, "fixture-health-auth").build();
            }
        };
    }

    @Test void protectedEndpointUsesConfiguredHealthAuthentication() throws Exception {
        assertTrue(probe(authorized(), 200, true));
    }
    @Test void authenticationDoesNotTurnUnreadyIntoHealthy() throws Exception {
        assertFalse(probe(authorized(), 503, true));
    }
    @Test void defaultProbeDoesNotInventAuthentication() throws Exception {
        assertFalse(probe(new HealthCheckService(), 200, false));
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"Authorization", "Cookie", "X-API-Key"})
    void credentialsNeverReachAPlainHttpEndpoint(String header) throws Exception {
        assertFalse(probe(authorized(header), 200, false));
        assertEquals(0, received.get());
    }
}
