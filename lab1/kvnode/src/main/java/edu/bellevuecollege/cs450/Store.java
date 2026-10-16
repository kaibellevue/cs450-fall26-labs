package edu.bellevuecollege.cs450;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CS 450 Lab 1 — the store.
 *
 * This is the whole of your data layer, and it is the only place the data
 * lives. Both protocols reach it: the line-based TCP server on port 9090 and
 * the REST endpoints on 8080. If you ever find yourself writing storage logic
 * in one of those, it belongs here instead.
 *
 * In memory only. Nothing survives a restart, and nothing is shared with the
 * other replicas — each container has its own. Making several nodes agree on
 * what they hold is Lab 2, and it is a much harder problem than this file.
 *
 * CP1 needs this working for one caller at a time.
 * CP2 needs it correct when several threads call it at once, which is a
 * different question and not one you can answer by being careful.
 */
public final class Store {

    // Using ConcurrentHashMap so we're safe for CP2 multi-threading
    private final Map<String, String> map = new ConcurrentHashMap<>();

    /**
     * Store a value, replacing anything already under that key.
     */
    public void put(String key, String value) {
        map.put(key, value);
    }

    /**
     * Fetch a value. Empty when the key is not present.
     *
     * Optional rather than null, so that "not found" is something the caller
     * has to think about rather than something they trip over. Both protocols
     * have to turn this into their own idea of absence: NOT_FOUND over TCP,
     * 404 over REST.
     */
    public Optional<String> get(String key) {
        String val = map.get(key);
        if (val == null) {
            return Optional.empty();
        }
        return Optional.of(val);
    }

    /**
     * Remove a key. True if it was there, false if it was not.
     */
    public boolean delete(String key) {
        return map.remove(key) != null;
    }

    /**
     * Every key currently held, in any order.
     *
     * Careful here at CP2. Handing back a view of your live data lets a caller
     * read it while another thread is writing. Hand back a copy.
     */
    public List<String> keys() {
        return new ArrayList<>(map.keySet());
    }

    /**
     * How many keys are held. Used by STATS and by /stats.
     */
    public int size() {
        return map.size();
    }
}
