package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.RejectedSessionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.InternalDAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ShardedHttpApi extends HttpApiBase {
    private static final Logger LOG = LoggerFactory.getLogger(ShardedHttpApi.class);

    private static final String PROXY_HEADER = "Node polling: true";
    private static final String TIMESTAMP_HEADER = "Timestamp: ";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Executor executor;
    private final Topology<String> topology;
    private final Map<String, HttpClient> pool;

    /**
     * Asynchronous API for database.
     *
     * @param port     port
     * @param dao      database DAO
     * @param executor pool of worker threads
     * @param topology topology of cluster
     * @throws IOException if I/O errors occurred
     */
    ShardedHttpApi(final int port,
                   final InternalDAO dao,
                   final Executor executor,
                   final Topology<String> topology) throws IOException {
        super(port, dao);
        this.executor = executor;
        this.topology = topology;

        this.pool = new HashMap<>();
        for (final String node : topology.all()) {
            if (topology.isMe(node)) {
                continue;
            }
            assert !pool.containsKey(node);
            this.pool.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
    }

    /**
     * Method for handling "/v0/entity" requests.
     *
     * @param request received request
     * @param session current http session
     * @throws IOException if I/O errors occurred
     */
    private void entity(final Request request,
                        final HttpSession session) throws IOException {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No id");
            return;
        }

        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));

        final String nodePollingHeader = request.getHeader("Node polling");
        if (nodePollingHeader != null && nodePollingHeader.trim().equals("true")) {
            final Action<Response> action = getRequestHandler(request, session, key);
            if (action == null) {
                session.sendError(Response.METHOD_NOT_ALLOWED, "Allowed only get, put and delete");
                return;
            }
            executeAsync(action, ar -> {
                if (ar.succeeded()) {
                    try {
                        session.sendResponse(ar.result());
                    } catch (IOException e) {
                        try {
                            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                        } catch (IOException ex) {
                            LOG.error("Failed to send error to client: {}", ex.getMessage());
                        }
                    }
                } else {
                    try {
                        session.sendError(Response.INTERNAL_ERROR, ar.cause().getMessage());
                    } catch (IOException ex) {
                        LOG.error("Failed to send error to client: {}", ex.getMessage());
                    }
                }
            });
        }

        final String replicas = request.getParameter("replicas=");
        final AF af;
        if (replicas != null) {
            af = AF.parse(replicas);
        } else {
            af = AF.def(topology.all().size());
        }

        final Set<String> primaryNodes = topology.primaryFor(key, af.from);
        final List<Response> responses = new CopyOnWriteArrayList<>();
        final CountDownLatch latch = new CountDownLatch(af.ack);

        for (String node : primaryNodes) {
            if (!topology.isMe(node)) {
                executeAsync(() -> proxy(request, node), ar -> handleResponse(session, responses, latch, ar));
            } else {
                try {
                    final Action<Response> action = getRequestHandler(request, session, key);
                    if (action == null) {
                        session.sendError(Response.METHOD_NOT_ALLOWED, "Allowed only get, put and delete");
                        return;
                    }
                    executeAsync(action, ar -> handleResponse(session, responses, latch, ar));
                } catch (NoSuchElementException e) {
                    session.sendError(Response.NOT_FOUND, "Key not found");
                }
            }
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("CountDownLatch was interrupted", e);
            Thread.currentThread().interrupt();
        }
        lock.readLock();
        try {
            LOG.info("Received {} responses from nodes. Process them.", af.ack);
            responses.iterator();
        } finally {
            lock.readLock().unlock();
        }

    }

    private void handleResponse(HttpSession session, List<Response> responses, CountDownLatch latch, AsyncResult<Response> ar) {
        LOG.info("Received response from node");
        if (ar.succeeded()) {
            final Response response = ar.result();
            responses.add(response);
            latch.countDown();
        } else {
            try {
                session.sendError(Response.INTERNAL_ERROR, ar.cause().getMessage());
            } catch (IOException ex) {
                LOG.error("Failed to send error to client: {}", ex.getMessage());
            }
        }
    }

    private void handleRequest(Request request, HttpSession session, ByteBuffer key) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET: {
                    executeAsync(session, () -> get(key));
                    return;
                }
                case Request.METHOD_PUT: {
                    executeAsync(session, () -> put(request, key));
                    return;
                }
                case Request.METHOD_DELETE: {
                    executeAsync(session, () -> delete(key));
                    return;
                }
                default: {
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Allowed only get, put and delete");
                    return;
                }
            }
        } catch (NoSuchElementException e) {
            session.sendError(Response.NOT_FOUND, "Key not found");
        }
    }

//    private Response processNodesResponses(List<Response> nodesResponses, Request request, int ack) {
//        if (nodesResponses.size() < ack) {
//            return new Response("504 Not Enough Replicas", Response.EMPTY);
//        }
//        switch (request.getMethod()) {
//            case Request.METHOD_GET: {
//            }
//            case Request.METHOD_PUT: {
//            }
//            case Request.METHOD_DELETE: {
//            }
//            default: {
//                return null;
//            }
//        }
//        for (Response response : nodesResponses) {
//            final int statusCode = response.getStatus();
//            if (statusCode == 200) {
//                final byte[] body = response.getBody();
//                ByteBuffer.wrap(body);
//
//            }
//        }
//        return
//    }

    private static Value from(final Response response) throws IOException {
        final String timestamp = response.getHeader(TIMESTAMP_HEADER);
        if (response.getStatus() == 200) {
            if (timestamp == null) {
                throw new IllegalArgumentException("Wrong input data");
            }
            return Value.present(response.getBody(), Long.parseLong(timestamp));
        } else if (response.getStatus() == 404) {
            if (timestamp == null) {
                return Value.absent();
            } else {
                return Value.removed(Long.parseLong(timestamp));
            }
        } else {
            throw new IOException();
        }
    }

    private static Response from(final Value value, final boolean proxy) {
        Response result;
        switch (value.getState()) {
            case PRESENT:
                result = new Response(Response.OK, value.getData());
                if (proxy) {
                    result.addHeader(TIMESTAMP_HEADER + value.getTimestamp());
                }
                return result;
            case REMOVED:
                result = new Response(Response.NOT_FOUND, Response.EMPTY);
                if (proxy) {
                    result.addHeader(TIMESTAMP_HEADER + value.getTimestamp());
                }
                return result;
            case ABSENT:
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            default:
                throw new IllegalArgumentException("Wrong input data");
        }
    }

    private static Value merge(final List<Value> values) {
        return values.stream()
                .filter(value -> value.getState() != Value.State.ABSENT)
                .max(Comparator.comparingLong(Value::getTimestamp))
                .orElseGet(Value::absent);
    }

    private Action<Response> getRequestHandler(Request request, HttpSession session, ByteBuffer key) {
        switch (request.getMethod()) {
            case Request.METHOD_GET: {
                return () -> get(key);
            }
            case Request.METHOD_PUT: {
                return () -> put(request, key);
            }
            case Request.METHOD_DELETE: {
                return () -> delete(key);
            }
            default: {
                return null;
            }
        }
    }

    /**
     * Method for handling "/v0/entities" requests.
     *
     * @param request received request
     * @param session current http session
     * @throws IOException if I/O errors occurred
     */
    private void entities(final Request request,
                          final HttpSession session) throws IOException {
        final String start = request.getParameter("start=");

        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No start");
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
            return;
        }
        String end = request.getParameter("end=");
        if (end != null && end.isEmpty()) {
            end = null;
        }

        try {
            final Iterator<Record> records = dao.cellRange(
                    ByteBuffer.wrap(start.getBytes(Charsets.UTF_8)),
                    end == null ? null : ByteBuffer.wrap(end.getBytes(Charsets.UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public HttpSession createSession(final Socket socket) throws RejectedSessionException {
        return new StorageSession(socket, this);
    }

    private void executeAsync(final HttpSession session, final Action action) {
        executeAsync(session, action, null);
    }

    private void executeAsync(final HttpSession session, final Action<Response> action, final Handler<Response> handler) {
        executor.execute(() -> {
            try {
                final Response response = action.act();
                if (handler != null) {
                    handler.handle(response);
                }
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    LOG.error("Failed to send error to client: {}", ex.getMessage());
                }
            }
        });
    }

    private <T> void executeAsync(final Action<T> action, final Handler<AsyncResult<T>> handler) {
        final AsyncResult<T> ar = new AsyncResult<>();
        executor.execute(() -> {
            try {
                final T result = action.act();
                ar.setResult(result);
            } catch (Throwable th) {
                ar.setCause(th);
            }
            handler.handle(ar);
        });
    }

    private Response proxy(final Request request, final String node) throws IOException {
        request.addHeader(PROXY_HEADER);
        try {
            return pool.get(node).invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            throw new IOException("Failed to proxy", e);
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        switch (request.getPath()) {
            case "/v0/entity": {
                entity(request, session);
                break;
            }
            case "/v0/entities": {
                entities(request, session);
                break;
            }
            default: {
                session.sendError(Response.BAD_REQUEST, "Wrong path");
                break;
            }
        }
    }

    @FunctionalInterface
    interface Action<T> {
        T act() throws IOException;
    }

    @FunctionalInterface
    interface Handler<T> {
        void handle(T result);
    }

    private static class AsyncResult<T> {
        private T succeeded;
        private Throwable failed;

        public boolean succeeded() {
            return succeeded != null;
        }

        public boolean failed() {
            return failed != null;
        }

        public T result() {
            return succeeded;
        }

        public void setResult(T result) {
            this.succeeded = result;
        }

        public Throwable cause() {
            return failed;
        }

        public void setCause(Throwable cause) {
            this.failed = cause;
        }
    }

    private static class AF {
        public final int ack;
        public final int from;

        private AF(final int ack, final int from) {
            this.ack = ack;
            this.from = from;
        }

        public static AF parse(final String af) {
            final String[] splited = af.split("/");

            final int ack;
            try {
                ack = Integer.parseInt(splited[0]);
            } catch (Exception e) {
                throw new IllegalArgumentException("ack parameter is invalid");
            }

            final int from;
            try {
                from = Integer.parseInt(splited[1]);
            } catch (Exception e) {
                throw new IllegalArgumentException("from parameter is invalid");
            }

            return new AF(ack, from);
        }

        public static AF def(final int from) {
            assert from > 0;
            final int ack = (from / 2) + 1;
            return new AF(ack, from);
        }
    }
}
