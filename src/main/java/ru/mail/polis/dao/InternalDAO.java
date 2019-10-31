package ru.mail.polis.dao;

import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface InternalDAO {

    Value getValue(final ByteBuffer key) throws IOException;

    void upsertValue(final ByteBuffer key, final ByteBuffer value);

    void removeValue(final ByteBuffer key);

    Iterator<Record> cellRange(final ByteBuffer from, final ByteBuffer to) throws IOException;

}
