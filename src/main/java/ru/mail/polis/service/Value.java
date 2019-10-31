package ru.mail.polis.service;

import one.nio.http.Response;

public class Value {
    private static final Value ABSENT = new Value(null, -1, State.ABSENT);

    private final byte[] data;
    private final long timestamp;
    private final State state;

    private Value(byte[] data, long timestamp, State state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = state;
    }

    static Value present(final byte[] data, final long timestamp) {
        return new Value(data, timestamp, State.PRESENT);
    }

    static Value removed(final long timestamp) {
        return new Value(null, timestamp, State.REMOVED);
    }

    static Value absent() {
        return ABSENT;
    }

    public byte[] getData() {
        if (data == null) {
            throw new IllegalStateException();
        }
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public State getState() {
        return state;
    }

    enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }
}
