package org.pipelineframework;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HttpHealthRequestCustomizationTest {
    private boolean probe(HealthCheckService service, int status) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/q/health", exchange -> {
            int result = "fixture-health-auth".equals(exchange.getRequestHeaders().getFirst("Authorization"))
                ? status : 403;
            exchange.sendResponseHeaders(result, -1);
            exchange.close();
        });
        server.start();
        try {
            var uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/q/health");
            var method = HealthCheckService.class.getDeclaredMethod("checkHttpServiceHealth",
                String.class, String.class, URI.class, String.class, Supplier.class);
            method.setAccessible(true);
            return (boolean) method.invoke(service, "REST", "fixture", uri, uri.toString(), (Supplier<SSLContext>) null);
        } finally { server.stop(0); }
    }

    private HealthCheckService authorized() {
        return new HealthCheckService() {
            // Deliberately compiles before the hook exists, so RED proves actual dispatch is missing.
            protected HttpRequest customizeHttpHealthRequest(String type, String name, HttpRequest request) {
                assertEquals("REST", type);
                assertEquals("fixture", name);
                return HttpRequest.newBuilder(request, (n, v) -> true)
                    .header("Authorization", "fixture-health-auth").build();
            }
        };
    }

    @Test void protectedEndpointUsesConfiguredHealthAuthentication() throws Exception {
        assertTrue(probe(authorized(), 200));
    }
    @Test void authenticationDoesNotTurnUnreadyIntoHealthy() throws Exception {
        assertFalse(probe(authorized(), 503));
    }
    @Test void defaultProbeDoesNotInventAuthentication() throws Exception {
        assertFalse(probe(new HealthCheckService(), 200));
    }
}
