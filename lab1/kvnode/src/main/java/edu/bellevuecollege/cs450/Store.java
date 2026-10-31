package edu.bellevuecollege.cs450;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class Store {
    private final ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    public void put(String key, String value) {
        map.put(key, value);
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    public boolean delete(String key) {
        return map.remove(key) != null;
    }

    public List<String> keys() {
        List<String> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys);
        return keys;
    }

    public int size() {
        return map.size();
    }

    // Both servers use the same checks.
    static boolean validKey(String key) {
        if (key.isEmpty() || key.length() > 64) {
            return false;
        }

        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (Character.isWhitespace(c) || Character.isSpaceChar(c)) {
                return false;
            }
        }
        return true;
    }

    static boolean validValue(String value) {
        return !value.contains("\n") && !value.contains("\r")
                && value.getBytes(StandardCharsets.UTF_8).length <= 1024;
    }
}
