package edu.bellevuecollege.cs450;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * CS 450 Lab 2 — the line protocol on TCP 9090.
 *
 * Lab 1's five commands are unchanged except for PUT, which now runs a
 * transaction instead of writing straight to the map.
 *
 * The internode commands share this port and this parser deliberately: a peer
 * is just another client that happens to know more verbs.
 */
public final class TcpServer implements Runnable {

    private static final int MAX_KEY = 64;
    private static final int MAX_VALUE = 1024;

    private final int port;
    private final Store store;
    private final String nodeId;
    private final Membership membership;
    private final LamportClock clock;
    private final LockManager locks;
    private final TwoPhase txn;
    private final Peers peers;

    private final ExecutorService workers = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    public TcpServer(int port, Store store, String nodeId, Membership membership,
                     LamportClock clock, LockManager locks, TwoPhase txn,
                     Peers peers) {
        this.port = port;
        this.store = store;
        this.nodeId = nodeId;
        this.membership = membership;
        this.clock = clock;
        this.locks = locks;
        this.txn = txn;
        this.peers = peers;
    }

    @Override
    public void run() {
        try (ServerSocket listener = new ServerSocket(port)) {
            System.out.println("kvnode " + nodeId + " listening for TCP on port " + port);
            while (true) {
                Socket socket = listener.accept();
                workers.submit(() -> serve(socket));
            }
        } catch (IOException e) {
            System.err.println("TCP listener on port " + port + " stopped: " + e.getMessage());
        }
    }

    private void serve(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             OutputStream out = socket.getOutputStream()) {
            String line;
            while ((line = in.readLine()) != null) {
                String response = handle(line);
                out.write((response + "\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        } catch (IOException e) {
            System.err.println("connection ended: " + e.getMessage());
        }
    }

    /** One request line in, one response line out. */
    String handle(String line) {
        if (line.endsWith("\r")) line = line.substring(0, line.length() - 1);

        int sp = line.indexOf(' ');
        String command = sp < 0 ? line : line.substring(0, sp);
        String rest = sp < 0 ? "" : line.substring(sp + 1);

        switch (command) {
            // ----------------------------------------------- Lab 1, unchanged
            case "GET" -> {
                String bad = checkKey(rest);
                if (bad != null) return bad;
                Optional<String> found = store.get(rest);
                return found.map(v -> "VALUE " + v).orElse("NOT_FOUND");
            }
            case "DEL" -> {
                String bad = checkKey(rest);
                if (bad != null) return bad;
                return store.delete(rest) ? "OK" : "NOT_FOUND";
            }
            case "KEYS" -> {
                if (!rest.isEmpty()) return "ERR KEYS takes no arguments";
                List<String> keys = store.keys();
                return keys.isEmpty() ? "KEYS" : "KEYS " + String.join(" ", keys);
            }
            case "STATS" -> {
                if (!rest.isEmpty()) return "ERR STATS takes no arguments";
                return "STATS keys=" + store.size() + " node=" + nodeId;
            }

            // -------------------------------------------- PUT, now a transaction
            case "PUT" -> {
                int s2 = rest.indexOf(' ');
                if (s2 < 0) return "ERR PUT needs a key and a value";
                String key = rest.substring(0, s2);
                String value = rest.substring(s2 + 1);
                String bad = checkKey(key);
                if (bad != null) return bad;
                if (value.getBytes(StandardCharsets.UTF_8).length > MAX_VALUE) {
                    return "ERR value is longer than " + MAX_VALUE + " bytes";
                }
                if (!locks.acquire(key)) {
                    return "ERR could not acquire lock for " + key;
                }
                try {
                    store.put(key, value);
                    return "OK";
                } finally {
                    locks.release(key);
                }
            }


                        // ------------------------------------------------------- CP3
            case "MEMBERS" -> {
                if (!rest.isEmpty()) return "ERR MEMBERS takes no arguments";
                List<String> ids = membership.ids();
                return ids.isEmpty() ? "MEMBERS" : "MEMBERS " + String.join(" ", ids);
            }
            case "RELOAD" -> {
                if (!rest.isEmpty()) return "ERR RELOAD takes no arguments";
                try {
                    membership.reload();
                    return "OK";
                } catch (IOException e) {
                    return "ERR " + e.getMessage();
                }
            }
            case "CLOCK" -> {
                if (!rest.isEmpty()) return "ERR CLOCK takes no arguments";
                return "CLOCK " + clock.now() + " node=" + nodeId;
            }
            case "TICK" -> {
                if (!rest.isEmpty()) return "ERR TICK takes no arguments";
                return "CLOCK " + clock.tick() + " node=" + nodeId;
            }
            case "PING" -> {   // PING <ts> <node>
                String[] parts = rest.trim().split("\\s+");
                if (rest.isBlank() || parts.length != 2) {
                    return "ERR PING needs a timestamp and a node";
                }
                long received;
                try {
                    received = Long.parseLong(parts[0]);
                } catch (NumberFormatException e) {
                    return "ERR invalid timestamp";
                }
                long updated = clock.update(received);
                return "PONG " + updated + " " + nodeId;
            }
            case "GOSSIP" -> {
                if (!rest.isEmpty()) return "ERR GOSSIP takes no arguments";
                long sent = clock.tick();
                List<String> replies = peers.askAll("PING " + sent + " " + nodeId);
                for (String reply : replies) {
                    if (reply == null || !reply.startsWith("PONG ")) continue;
                    String[] parts = reply.split("\\s+");
                    if (parts.length < 2) continue;
                    try {
                        clock.update(Long.parseLong(parts[1]));
                    } catch (NumberFormatException ignored) {
                        // ignore malformed peer reply
                    }
                }
                return "OK sent=" + sent + " now=" + clock.now();
            }


            // ------------------------------------------------------- CP4
            case "LOCK" -> {
                String[] parts = rest.trim().split("\\s+");
                if (rest.isBlank() || parts.length != 3) {
                    return "ERR LOCK needs a key, a timestamp, and a node";
                }
                String bad = checkKey(parts[0]);
                if (bad != null) return bad;
                long ts;
                try {
                    ts = Long.parseLong(parts[1]);
                } catch (NumberFormatException e) {
                    return "ERR invalid timestamp";
                }
                return locks.onLockRequest(parts[0], ts, parts[2]);
            }
            case "RELEASE" -> {
                String[] parts = rest.trim().split("\\s+");
                if (rest.isBlank() || parts.length != 3) {
                    return "ERR RELEASE needs a key, a timestamp, and a node";
                }
                String bad = checkKey(parts[0]);
                if (bad != null) return bad;
                long ts;
                try {
                    ts = Long.parseLong(parts[1]);
                } catch (NumberFormatException e) {
                    return "ERR invalid timestamp";
                }
                locks.onRelease(parts[0], ts, parts[2]);
                return "OK";
            }


            // ------------------------------------------------------- CP5
            case "PREPARE" -> {
                // TODO(cp5): PREPARE <txid> <key> <value>. Split into three --
                // the value is last and may contain spaces.
                return "ERR not implemented";
            }
            case "COMMIT" -> {
                // TODO(cp5)
                return "ERR not implemented";
            }
            case "ABORT" -> {
                // TODO(cp5)
                return "ERR not implemented";
            }

            default -> {
                return "ERR unknown command: " + command;
            }
        }
    }

    private static String checkKey(String key) {
        if (key.isEmpty())          return "ERR missing key";
        if (key.indexOf(' ') >= 0)  return "ERR key contains a space";
        if (key.length() > MAX_KEY) return "ERR key is longer than " + MAX_KEY + " characters";
        return null;
    }
}
