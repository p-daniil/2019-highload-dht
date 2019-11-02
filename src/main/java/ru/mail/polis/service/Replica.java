package ru.mail.polis.service;

import one.nio.http.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static ru.mail.polis.service.HttpApiBase.TIMESTAMP_HEADER;

public final class Replica {
    private static final Replica ABSENT = new Replica(null, -1, State.ABSENT);

    private byte[] data;
    private final long timestamp;
    private final State state;

    private Replica(final byte[] data, final long timestamp, final State state) {
        if (data != null) {
            this.data = Arrays.copyOf(data, data.length);
        }
        this.timestamp = timestamp;
        this.state = state;
    }

    private static Replica present(final byte[] data, final long timestamp) {
        return new Replica(data, timestamp, State.PRESENT);
    }

    private static Replica removed(final long timestamp) {
        return new Replica(null, timestamp, State.REMOVED);
    }

    private static Replica absent() {
        return ABSENT;
    }

    static Replica fromResponse(final Response response) throws IOException {
        final String timestamp = response.getHeader(TIMESTAMP_HEADER);
        if (response.getStatus() == 200) {
            if (timestamp == null) {
                throw new IllegalArgumentException("Wrong input data");
            }
            return Replica.present(response.getBody(), Long.parseLong(timestamp));
        } else if (response.getStatus() == 404) {
            if (timestamp == null) {
                return Replica.absent();
            } else {
                return Replica.removed(Long.parseLong(timestamp));
            }
        } else {
            throw new IOException();
        }
    }

    static Response toResponse(final Replica replica) {
        switch (replica.getState()) {
            case PRESENT:
                return new Response(Response.OK, replica.getData());
            case REMOVED:
            case ABSENT:
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            default:
                throw new IllegalArgumentException("Wrong input data");
        }
    }

    static Replica merge(final List<Replica> replicas) {
        return replicas.stream()
                .filter(replica -> replica.getState() != Replica.State.ABSENT)
                .max(Comparator.comparingLong(Replica::getTimestamp))
                .orElseGet(Replica::absent);
    }

    private byte[] getData() {
        if (data == null) {
            throw new IllegalStateException();
        }
        return data;
    }

    private long getTimestamp() {
        return timestamp;
    }

    private State getState() {
        return state;
    }

    enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }
}
