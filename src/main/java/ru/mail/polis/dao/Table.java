package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table extends Comparable<Table> {

    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    void remove(@NotNull ByteBuffer key);

    long getSize();

    long getVersion();

    @Override
    default int compareTo(@NotNull Table table) {
        return SSTable.TABLE_COMPARATOR.compare(this, table);
    }
}
