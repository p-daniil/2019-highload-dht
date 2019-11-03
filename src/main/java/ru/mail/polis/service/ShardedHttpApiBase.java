package ru.mail.polis.service;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.InternalDAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

class ShardedHttpApiBase extends HttpApiBase {
    private static final Logger LOG = LoggerFactory.getLogger(ShardedHttpApiBase.class);

    final Executor executor;
    final Topology<String> topology;
    private final Map<String, HttpClient> pool;

    ShardedHttpApiBase(final int port,
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

    @Nullable
    RF getRf(final Request request, final HttpSession session) throws IOException {
        final String replicas = request.getParameter("replicas=");
        final RF rf;
        if (replicas == null) {
            rf = RF.def(topology.all().size());
        } else {
            try {
                rf = RF.parse(replicas);
            } catch (IllegalArgumentException e) {
                session.sendError(Response.BAD_REQUEST, e.getMessage());
                return null;
            }
            if (rf.ack > topology.all().size()) {
                session.sendError("504", "Unreachable replication factor");
                return null;
            }
        }
        return rf;
    }

    void processNodeRequest(final Request request,
                            final HttpSession session,
                            final ByteBuffer key) throws IOException {
        final Action<Response> action = getRequestHandler(request, key);
        if (action == null) {
            session.sendError(Response.METHOD_NOT_ALLOWED, "Allowed only get, put and delete");
        }
        executeAsync(session, action);
    }

    Response processClientRequest(final Request request,
                                  final ByteBuffer key,
                                  final RF rf) throws IOException {
        final Set<String> primaryNodes = topology.primaryFor(key, rf.from);
        final List<Response> responses = new CopyOnWriteArrayList<>();
        final CountDownLatch latch = new CountDownLatch(rf.ack);
        LOG.info("Node {} started to poll nodes", topology.getMe());

        for (final String node : primaryNodes) {
            if (topology.isMe(node)) {
                final Action<Response> action = getRequestHandler(request, key);
                if (action == null) {
                    return new Response(Response.METHOD_NOT_ALLOWED,
                            "Allowed only get, put and delete".getBytes(StandardCharsets.UTF_8));
                }
                processLocally(responses, latch, action);
            } else {
                pollNode(request, responses, latch, node);
            }
        }

        try {
            if (!latch.await(500, TimeUnit.MILLISECONDS)) {
                LOG.error("Node polling timeout: not enough replicas");
                return new Response("504", "Not Enough Replicas".getBytes(StandardCharsets.UTF_8));
            }
        } catch (InterruptedException e) {
            LOG.error("Node polling was interrupted", e);
            Thread.currentThread().interrupt();
        }
        LOG.info("Received {} responses from nodes. Process them.", rf.ack);
        return processNodesResponses(responses, request, rf.ack);
    }

    private void processLocally(final List<Response> responses,
                                final CountDownLatch latch,
                                final Action<Response> action) {
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

    private void pollNode(final Request request,
                          final List<Response> responses,
                          final CountDownLatch latch,
                          final String node) {
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
    }

    private Action<Response> getRequestHandler(final Request request, final ByteBuffer key) {
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

    private Response processNodesResponses(final List<Response> nodesResponses,
                                           final Request request,
                                           final int ack) throws IOException {
        if (nodesResponses.size() < ack) {
            LOG.info("Not enough responses received");
            return new Response("504", "Not Enough Replicas".getBytes(StandardCharsets.UTF_8));
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET: {

                final List<Replica> replicas = new ArrayList<>();
                for (int i = 0; i < ack; i++) {
                    replicas.add(Replica.fromResponse(nodesResponses.get(i)));
                }
                LOG.info("Send response to client on GET");
                return Replica.toResponse(Replica.merge(replicas));
            }
            case Request.METHOD_PUT: {
                LOG.info("Send response to client on PUT request");
                return new Response(Response.CREATED, Response.EMPTY);
            }
            case Request.METHOD_DELETE: {
                LOG.info("Send response to client on DELETE request");
                return new Response(Response.ACCEPTED, Response.EMPTY);
            }
            default: {
                return new Response(Response.METHOD_NOT_ALLOWED,
                        "Method not allowed".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    void executeAsync(final HttpSession session, final Action<Response> action) {
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

    private synchronized Response proxy(final Request request, final String node) throws IOException, PoolException {
        request.addHeader(PROXY_HEADER);
        try {
            return pool.get(node).invoke(request);
        } catch (InterruptedException | HttpException e) {
            throw new IOException("Failed to proxy", e);
        }
    }
}
