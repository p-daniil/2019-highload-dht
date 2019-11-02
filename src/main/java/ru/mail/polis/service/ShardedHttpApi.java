package ru.mail.polis.service;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.InternalDAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class ShardedHttpApi extends HttpApiBase {
    private static final Logger LOG = LoggerFactory.getLogger(ShardedHttpApi.class);

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

        final String nodePollingHeader = request.getHeader(PROXY_HEADER);

        if (nodePollingHeader != null) {
            final Action<Response> action = getRequestHandler(request, key);
            if (action == null) {
                session.sendError(Response.METHOD_NOT_ALLOWED, "Allowed only get, put and delete");
                return;
            }
            executeAsync(session, action);
            return;
        }

        final String replicas = request.getParameter("replicas=");
        final RF rf;
        if (replicas != null) {
            try {
                rf = RF.parse(replicas);
            } catch (IllegalArgumentException e) {
                session.sendError(Response.BAD_REQUEST, e.getMessage());
                return;
            }
            if (rf.ack > topology.all().size()) {
                session.sendError("504", "Unreachable replication factor");
                return;
            }
        } else {
            rf = RF.def(topology.all().size());
        }
        LOG.info("New client request with RF {}", rf);

        final Set<String> primaryNodes = topology.primaryFor(key, rf.from);
        final List<Response> responses = new CopyOnWriteArrayList<>();
        final CountDownLatch latch = new CountDownLatch(rf.ack);
        LOG.info("Node {} started to poll nodes", topology.getMe());

        for (String node : primaryNodes) {
            if (!topology.isMe(node)) {
                executor.execute(() -> {
                    try {
                        final Response response = proxy(request, node);
                        LOG.info("Received response from node {}", node);
                        responses.add(response);
                        latch.countDown();
                    } catch (IOException e) {
                        LOG.error("Failed to execute action on node {}", node, e);
                    } catch (PoolException pe) {
                        LOG.error("Node unavailable: {}", node);
                    }
                });
            } else {
                final Action<Response> action = getRequestHandler(request, key);
                if (action == null) {
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Allowed only get, put and delete");
                    return;
                }
                executor.execute(() -> {
                    try {
                        final Response response = action.act();
                        LOG.info("Received response from coordinator node");
                        responses.add(response);
                        latch.countDown();
                    } catch (IOException e) {
                        LOG.error("Failed to execute action on coordinator node", e);
                    }
                });
            }
        }

        try {
            if (!latch.await(500, TimeUnit.MILLISECONDS)) {
                LOG.error("Node polling timeout: not enough replicas");
                session.sendError("504", "Not Enough Replicas");
                return;
            }
        } catch (InterruptedException e) {
            LOG.error("Node polling was interrupted", e);
            Thread.currentThread().interrupt();
        }
        LOG.info("Received {} responses from nodes. Process them.", rf.ack);
        processNodesResponses(responses, session, request, rf.ack);
    }

    private Action<Response> getRequestHandler(Request request, ByteBuffer key) {
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

    private void processNodesResponses(final List<Response> nodesResponses,
                                       final HttpSession session,
                                       final Request request,
                                       final int ack) throws IOException {
        if (nodesResponses.size() < ack) {
            LOG.info("Not enough responses received");
            session.sendError("504", "Not Enough Replicas");
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET: {
                final List<Replica> replicas = new ArrayList<>();
                for (int i = 0; i < ack; i++) {
                    replicas.add(Replica.fromResponse(nodesResponses.get(i)));
                }
                session.sendResponse(Replica.toResponse(Replica.merge(replicas)));
                LOG.info("Sended response to client on GET");
                break;
            }
            case Request.METHOD_PUT: {
                session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                LOG.info("Successfully send response to client on PUT request");
                break;
            }
            case Request.METHOD_DELETE: {
                session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                LOG.info("Successfully send response to client on DELETE request");
                break;
            }
            default: {
                session.sendError(Response.METHOD_NOT_ALLOWED, "Method not allowed");
                break;
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
            final Iterator<Record> records = dao.recordRange(
                    ByteBuffer.wrap(start.getBytes(Charsets.UTF_8)),
                    end == null ? null : ByteBuffer.wrap(end.getBytes(Charsets.UTF_8)));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    private void executeAsync(final HttpSession session, final Action<Response> action) {
        executor.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    LOG.error("Failed to send error to client: {}", ex.getMessage());
                }
            }
        });
    }

    private Response proxy(final Request request, final String node) throws IOException, PoolException {
        request.addHeader(PROXY_HEADER);
        try {
            return pool.get(node).invoke(request);
        } catch (InterruptedException | HttpException e) {
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

    private static class RF {
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
            final int ack = (from / 2) + 1;
            return new RF(ack, from);
        }

        @Override
        public String toString() {
            return ack + "/" + from;
        }
    }
}
