package ru.mail.polis.dao;

import java.nio.ByteBuffer;
import java.util.Comparator;

final class Cell implements Comparable<Cell> {
    private static final Comparator<Cell> CELL_COMPARATOR = Comparator
            .comparing(Cell::getKey)
            .thenComparing(Cell::getValue)
            .thenComparing(Comparator.comparingLong(Cell::getVersion).reversed());

    private final ByteBuffer key;
    private final Value value;
    private final long version;

    private Cell(final ByteBuffer key, final Value value, final long version) {
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public static Cell create(final ByteBuffer key, final Value value, final long version) {
        return new Cell(key, value, version);
    }

    public ByteBuffer getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    public long getVersion() {
        return version;
    }

    @Override
    public int compareTo(final Cell o) {
        return CELL_COMPARATOR.compare(this, o);
    }
}
