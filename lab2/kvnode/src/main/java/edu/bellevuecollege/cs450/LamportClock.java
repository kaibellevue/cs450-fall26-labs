package edu.bellevuecollege.cs450;

/**
 * CS 450 Lab 2 — a Lamport clock.
 *
 * Three rules, and you worked all three by hand in the written exercise:
 *
 *   tick()        a local event happens here: advance, then use the new value
 *   update(rx)    a message arrived carrying rx: max(own, rx) + 1
 *   now()         read the value without advancing it
 */
public final class LamportClock {

    private long time = 0;

    /** A local event: advance and return the new value. */
    public synchronized long tick() {
        return ++time;
    }

    /** A message arrived carrying 'received'. Returns the new value. */
    public synchronized long update(long received) {
        time = Math.max(time, received) + 1;
        return time;
    }

    /** The current value, unchanged by the read. */
    public synchronized long now() {
        return time;
    }
}
