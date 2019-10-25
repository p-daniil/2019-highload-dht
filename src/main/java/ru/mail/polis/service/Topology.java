package ru.mail.polis.service;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Set;

@ThreadSafe
public interface Topology<T> {
    T primaryFor(ByteBuffer key);

    boolean isMe(T node);

    Set<T> all();
}
