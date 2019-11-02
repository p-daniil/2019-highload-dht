package ru.mail.polis.dao;

import java.io.IOException;

public interface Flushable {
    void flush(final Table table) throws IOException;
}
