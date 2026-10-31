package edu.bellevuecollege.cs450;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public final class TcpServer implements Runnable {
    private final int port;
    private final Store store;
    private final String nodeId;

    public TcpServer(int port, Store store, String nodeId) {
        this.port = port;
        this.store = store;
        this.nodeId = nodeId;
    }

    @Override
    public void run() {
        try (ServerSocket listener = new ServerSocket(port)) {
            System.out.println("TCP listening on port " + port);
            while (true) {
                Socket socket = listener.accept();
                // An idle connection should not stop other clients.
                Thread worker = new Thread(() -> serve(socket));
                worker.setDaemon(true);
                worker.start();
            }
        } catch (IOException e) {
            System.err.println("TCP server stopped: " + e.getMessage());
        }
    }

    private void serve(Socket socket) {
        try (Socket client = socket;
             BufferedReader in = new BufferedReader(new InputStreamReader(
                     client.getInputStream(), StandardCharsets.UTF_8));
             OutputStream out = client.getOutputStream()) {

            String line;
            // readLine handles both LF and CRLF.
            while ((line = in.readLine()) != null) {
                String response = handle(line);
                out.write((response + "\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        } catch (IOException e) {
            System.err.println("TCP connection ended: " + e.getMessage());
        }
    }

    String handle(String line) {
        if (line == null || line.isEmpty()) {
            return "ERR empty request";
        }

        int space = line.indexOf(' ');
        String command = space == -1 ? line : line.substring(0, space);
        String rest = space == -1 ? "" : line.substring(space + 1);

        if (command.equals("PUT")) {
            int valueStart = rest.indexOf(' ');
            if (valueStart == -1) {
                return "ERR usage: PUT <key> <value>";
            }

            String key = rest.substring(0, valueStart);
            // Don't trim this: spaces can be part of the value.
            String value = rest.substring(valueStart + 1);
            if (!Store.validKey(key)) {
                return "ERR invalid key";
            }
            if (!Store.validValue(value)) {
                return "ERR invalid value";
            }

            store.put(key, value);
            return "OK";
        }

        if (command.equals("GET")) {
            if (!Store.validKey(rest)) {
                return "ERR invalid key";
            }
            Optional<String> value = store.get(rest);
            return value.isPresent() ? "VALUE " + value.get() : "NOT_FOUND";
        }

        if (command.equals("DEL")) {
            if (!Store.validKey(rest)) {
                return "ERR invalid key";
            }
            return store.delete(rest) ? "OK" : "NOT_FOUND";
        }

        if (command.equals("KEYS")) {
            if (space != -1) {
                return "ERR usage: KEYS";
            }
            List<String> keys = store.keys();
            return keys.isEmpty() ? "KEYS" : "KEYS " + String.join(" ", keys);
        }

        if (command.equals("STATS")) {
            if (space != -1) {
                return "ERR usage: STATS";
            }
            return "STATS keys=" + store.size() + " node=" + nodeId;
        }

        return "ERR unknown command";
    }
}
