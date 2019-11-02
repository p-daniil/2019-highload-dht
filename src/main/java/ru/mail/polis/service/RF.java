package ru.mail.polis.service;

import com.google.common.base.Splitter;

import java.util.List;

public final class RF {
    final int ack;
    final int from;

    private RF(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    static RF parse(final String rf) {
        final List<String> splited = Splitter.on('/').splitToList(rf);

        int ack = -1;
        final String ackParam = splited.get(0);
        if (ackParam != null) {
            ack = Integer.parseInt(ackParam);
        }

        int from = -1;
        final String fromParam = splited.get(1);
        if (fromParam != null) {
            from = Integer.parseInt(fromParam);
        }

        if (ack <= 0 || from <= 0) {
            throw new IllegalArgumentException("ack and from should be larger than 0");
        }
        if (ack > from) {
            throw new IllegalArgumentException("ack shouldn't be larger than from");
        }
        return new RF(ack, from);
    }

    static RF def(final int from) {
        assert from > 0;
        final int ack = from / 2 + 1;
        return new RF(ack, from);
    }

    @Override
    public String toString() {
        return ack + "/" + from;
    }
}
