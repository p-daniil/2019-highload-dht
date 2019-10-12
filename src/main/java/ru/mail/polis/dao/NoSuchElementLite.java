package ru.mail.polis.dao;

import java.util.NoSuchElementException;

public class NoSuchElementLite extends NoSuchElementException {

    public NoSuchElementLite(final String s) {
        super(s);
    }

    /**
     * Fills in the execution stack trace. This method records within this
     * {@code Throwable} object information about the current state of
     * the stack frames for the current thread.
     *
     * <p>If the stack trace of this {@code Throwable} {@linkplain
     * Throwable#Throwable(String, Throwable, boolean, boolean) is not
     * writable}, calling this method has no effect.
     *
     * @return  a reference to this {@code Throwable} instance.
     * @see     java.lang.Throwable#printStackTrace()
     */
    @Override
    public Throwable fillInStackTrace() {
        synchronized (this) {
            return this;
        }
    }
}
