package ru.mail.polis.dao;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface InternalDAO {

    Value getValue(final ByteBuffer key) throws IOException, NoSuchElementLiteException;

}
