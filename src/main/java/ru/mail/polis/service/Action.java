package ru.mail.polis.service;

import java.io.IOException;

@FunctionalInterface
interface Action<T> {
    T act() throws IOException;
}
