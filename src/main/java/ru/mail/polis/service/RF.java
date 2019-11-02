package ru.mail.polis.service;

import com.google.common.base.Splitter;

import java.util.List;

public class RF {
    final int ack;
    final int from;

    private RF(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    static RF parse(final String rf) {
        final List<String> splited = Splitter.on('/').splitToList(rf);

        final int ack;
        try {
            ack = Integer.parseInt(splited.get(0));
        } catch (Exception e) {
            throw new IllegalArgumentException("ack parameter is invalid");
        }

        final int from;
        try {
            from = Integer.parseInt(splited.get(1));
        } catch (Exception e) {
            throw new IllegalArgumentException("from parameter is invalid");
        }
        if (ack > from) {
            throw new IllegalArgumentException("ack shouldn't be larger than from");
        }
        if (ack <= 0) {
            throw new IllegalArgumentException("ack should be larger than 0");
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
