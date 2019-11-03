package ru.mail.polis.service;

import one.nio.http.*;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

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

    void processLocally(final List<Response> responses,
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

    void pollNode(final Request request,
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

    Action<Response> getRequestHandler(final Request request, final ByteBuffer key) {
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

    Response processNodesResponses(final List<Response> nodesResponses,
                                   final HttpSession session,
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
                LOG.info("Sended response to client on GET");
                return Replica.toResponse(Replica.merge(replicas));
            }
            case Request.METHOD_PUT: {
                LOG.info("Successfully send response to client on PUT request");
                return new Response(Response.CREATED, Response.EMPTY);
            }
            case Request.METHOD_DELETE: {
                LOG.info("Successfully send response to client on DELETE request");
                return new Response(Response.ACCEPTED, Response.EMPTY);
            }
            default: {
                return new Response(Response.METHOD_NOT_ALLOWED, "Method not allowed".getBytes(StandardCharsets.UTF_8));
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
