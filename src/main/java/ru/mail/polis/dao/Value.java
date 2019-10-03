package ru.mail.polis.dao;

import java.nio.ByteBuffer;

final class Value implements Comparable<Value> {

    private final long timeStamp;
    private final boolean tombstone;
    private final ByteBuffer data;

    private Value(final long timeStamp, final boolean tombstone, final ByteBuffer data) {
        this.timeStamp = timeStamp;
        this.tombstone = tombstone;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(System.currentTimeMillis(), false, data);
    }

    public static Value of(final long timeStamp, final ByteBuffer data) {
        return new Value(timeStamp, false, data);
    }

    public static Value tombstone() {
        return new Value(System.currentTimeMillis(), true, null);
    }

    public static Value tombstone(final long timeStamp) {
        return new Value(timeStamp, true, null);
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public boolean isRemoved() {
        return tombstone;
    }

    public ByteBuffer getData() {
        return data;
    }

    @Override
    public int compareTo(final Value o) {
        // "this" is lower if his timestamp is bigger.
        return Long.compare(o.getTimeStamp(), this.getTimeStamp());
    }

}
