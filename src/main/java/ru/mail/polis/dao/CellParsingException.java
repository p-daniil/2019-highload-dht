package ru.mail.polis.dao;

public class CellParsingException extends RuntimeException {
    private static final long serialVersionUID = 7526471155622776147L;

    public CellParsingException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
