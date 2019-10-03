package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {

    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    void remove(@NotNull ByteBuffer key);

    long getSize();
    
    long getVersion();
}
