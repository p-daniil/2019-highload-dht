package ru.mail.polis.service;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Set;

@ThreadSafe
public interface Topology<T> {
    Set<T> primaryFor(ByteBuffer key, int from);

    boolean isMe(T node);

    T getMe();

    Set<T> all();
}
