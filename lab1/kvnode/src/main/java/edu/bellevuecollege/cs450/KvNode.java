package edu.bellevuecollege.cs450;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

public final class KvNode {
    private static final String NODE_ID = resolveNodeId();

    public static void main(String[] args) throws IOException {
        Store store = new Store();

        int httpPort = readPort("NODE_PORT", 8080);
        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        server.setExecutor(Executors.newFixedThreadPool(8));

        server.createContext("/whoami", exchange -> {
            if (!exchange.getRequestURI().getPath().equals("/whoami")) {
                respond(exchange, 404, "not found");
                return;
            }
            respond(exchange, 200, NODE_ID + "\n");
        });

        server.createContext("/health", exchange -> {
            if (!exchange.getRequestURI().getPath().equals("/health")) {
                respond(exchange, 404, "not found");
                return;
            }
            respond(exchange, 200, "ok\n");
        });

        server.createContext("/kv", exchange -> kv(exchange, store));
        server.createContext("/stats", exchange -> stats(exchange, store));
        server.createContext("/", exchange -> respond(exchange, 404, "not found"));

        int kvPort = readPort("KV_PORT", 9090);
        Thread tcp = new Thread(new TcpServer(kvPort, store, NODE_ID), "tcp-listener");
        tcp.setDaemon(false);
        tcp.start();

        server.start();
        System.out.println("kvnode " + NODE_ID + " listening for HTTP on port " + httpPort);
    }

    private static void kv(HttpExchange exchange, Store store) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();

        if (path.equals("/kv")) {
            if (!method.equals("GET")) {
                respond(exchange, 405, "method not allowed");
                return;
            }

            List<String> keys = store.keys();
            respond(exchange, 200, String.join("\n", keys));
            return;
        }

        // The HTTP router matches prefixes, so /kvhello needs this check.
        if (!path.startsWith("/kv/")) {
            respond(exchange, 404, "not found");
            return;
        }

        if (!method.equals("PUT") && !method.equals("GET") && !method.equals("DELETE")) {
            respond(exchange, 405, "method not allowed");
            return;
        }

        String key = path.substring(4);
        if (!Store.validKey(key)) {
            int status = method.equals("PUT") ? 400 : 404;
            respond(exchange, status, "invalid key");
            return;
        }

        if (method.equals("PUT")) {
            // Read one extra byte so oversized values can be rejected.
            byte[] body = exchange.getRequestBody().readNBytes(1025);
            if (body.length > 1024) {
                respond(exchange, 400, "value too long");
                return;
            }

            String value = new String(body, StandardCharsets.UTF_8);
            if (!Store.validValue(value)) {
                respond(exchange, 400, "invalid value");
                return;
            }

            store.put(key, value);
            respond(exchange, 200, "OK");
            return;
        }

        if (method.equals("GET")) {
            Optional<String> value = store.get(key);
            if (value.isPresent()) {
                respond(exchange, 200, value.get());
            } else {
                respond(exchange, 404, "not found");
            }
            return;
        }

        if (store.delete(key)) {
            respond(exchange, 200, "OK");
        } else {
            respond(exchange, 404, "not found");
        }
    }

    private static void stats(HttpExchange exchange, Store store) throws IOException {
        if (!exchange.getRequestURI().getPath().equals("/stats")) {
            respond(exchange, 404, "not found");
            return;
        }
        if (!exchange.getRequestMethod().equals("GET")) {
            respond(exchange, 405, "method not allowed");
            return;
        }

        respond(exchange, 200, "keys=" + store.size() + " node=" + NODE_ID);
    }

    static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.getResponseHeaders().set("X-Node-Id", NODE_ID);

        // Empty values are allowed. HEAD responses also have no body.
        if (bytes.length == 0 || exchange.getRequestMethod().equals("HEAD")) {
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
            return;
        }

        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        } finally {
            exchange.close();
        }
    }

    private static int readPort(String var, int fallback) {
        String configured = System.getenv(var);
        if (configured == null || configured.isBlank()) {
            return fallback;
        }
        try {
            int port = Integer.parseInt(configured.trim());
            if (port >= 1 && port <= 65535) {
                return port;
            }
        } catch (NumberFormatException e) {
            // Use the normal port if the setting isn't a number.
        }
        System.err.println("Invalid " + var + "; using " + fallback);
        return fallback;
    }

    private static String resolveNodeId() {
        String fromEnv = System.getenv("NODE_ID");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv.trim();
        }
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-node";
        }
    }

    private KvNode() {
    }
}
