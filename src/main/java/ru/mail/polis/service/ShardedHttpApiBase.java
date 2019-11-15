package ru.mail.polis.service;

import one.nio.http.Request;
import one.nio.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.InternalDAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static java.nio.charset.StandardCharsets.UTF_8;

abstract class ShardedHttpApiBase extends HttpApiBase {
    private static final Logger LOG = LoggerFactory.getLogger(ShardedHttpApiBase.class);

    private final Executor executor;
    private final Topology<String> topology;
    private final Map<String, AsyncClient> clientPool;

    ShardedHttpApiBase(final int port,
                       final InternalDAO dao,
                       final Executor executor,
                       final Topology<String> topology) throws IOException {
        super(port, dao);
        this.executor = executor;
        this.topology = topology;
        this.clientPool = new HashMap<>();
        for (final String node : topology.all()) {
            if (topology.isMe(node)) {
                continue;
            }
            assert !clientPool.containsKey(node);
            this.clientPool.put(node, new AsyncClient(node, executor));
        }
    }

    RF getRf(final Request request) throws IllegalArgumentException {
        final String replicas = request.getParameter("replicas=");
        final RF rf;
        if (replicas == null) {
            rf = RF.def(topology.all().size());
        } else {
            rf = RF.parse(replicas);
        }
        if (rf.ack > topology.all().size()) {
            throw new IllegalArgumentException("Unreachable replication factor");
        }
        return rf;
    }

    CompletableFuture<Response> processNodeRequest(final Request request,
                                                   final ByteBuffer key) {
        final Action<Response> action = getRequestHandler(request, key);
        if (action == null) {
            return CompletableFuture.completedFuture(
                    new Response(Response.METHOD_NOT_ALLOWED, "Allowed only get, put and delete".getBytes(UTF_8)));
        }
        final CompletableFuture<Response> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                final Response response = action.act();
                future.complete(response);
            } catch (IOException e) {
                LOG.error("Failed to handle node request");
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    CompletableFuture<Response> processClientRequest(final Request request,
                                                     final ByteBuffer key,
                                                     final RF rf) {
        final Action<Response> action = getRequestHandler(request, key);
        if (action == null) {
            return CompletableFuture.completedFuture(new Response(Response.METHOD_NOT_ALLOWED,
                    "Allowed only get, put and delete".getBytes(UTF_8)));
        }

        final Set<String> primaryNodes = topology.primaryFor(key, rf.from);
        LOG.info("Node {} started to poll nodes", topology.getMe());

        final List<CompletableFuture<Response>> nodesResponsesFutures = new ArrayList<>();
        for (final String node : primaryNodes) {
            if (topology.isMe(node)) {
                nodesResponsesFutures.add(processLocallyAsync(action));
            } else {
                nodesResponsesFutures.add(pollNodeAsync(request, node));
            }
        }
        return processNodesResponsesAsync(nodesResponsesFutures, request, rf.ack);
    }

    private CompletableFuture<Response> processLocallyAsync(final Action<Response> action) {
        final CompletableFuture<Response> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                final Response response = action.act();
                LOG.info("Received response from coordinator node");
                future.complete(response);
            } catch (IOException e) {
                LOG.error("Failed to handle request on coordinator node", e);
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private CompletableFuture<Response> pollNodeAsync(final Request request,
                                                      final String node) {
        final CompletableFuture<Response> future = new CompletableFuture<>();
        proxy(request, node).whenCompleteAsync((response, fail) -> {
            if (fail == null) {
                LOG.info("Received response from node {}", node);
                future.complete(response);
            } else if (fail instanceof IOException) {
                LOG.error("Failed to handle request on node {}", node, fail);
                future.completeExceptionally(fail);
            } else {
                LOG.error("Failed to receive response from node: {}", node);
                future.completeExceptionally(fail);
            }
        });
        return future;
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private CompletableFuture<Response> processNodesResponsesAsync(final List<CompletableFuture<Response>> futures,
                                                                   final Request request,
                                                                   final int ack) {
        final CompletableFuture<Response> future = new CompletableFuture<>();
        ExtendedCompletableFuture.firstN(futures, ack).whenCompleteAsync((responses, fail) -> {
            if (fail == null) {
                try {
                    final Response response = processNodesResponses(responses, request, ack);
                    future.complete(response);
                } catch (IllegalArgumentException | IOException e) {
                    LOG.error("Failed to process nodes responses", e);
                    future.completeExceptionally(e);
                }
            } else {
                LOG.error("Not enough nodes responses received");
                future.complete(new Response(Response.GATEWAY_TIMEOUT, "Not enough replicas".getBytes(UTF_8)));
            }
        });
        return future;
    }

    private Response processNodesResponses(final List<Response> nodesResponses,
                                           final Request request,
                                           final int ack) throws IOException {
        if (nodesResponses.size() < ack) {
            LOG.error("Not enough nodes responses received");
            return new Response(Response.GATEWAY_TIMEOUT, "Not Enough Replicas".getBytes(UTF_8));
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                final List<Replica> replicas = new ArrayList<>();
                for (int i = 0; i < ack; i++) {
                    replicas.add(Replica.fromResponse(nodesResponses.get(i)));
                }
                LOG.info("Created response for client on GET request");
                return Replica.toResponse(Replica.merge(replicas));
            case Request.METHOD_PUT:
                LOG.info("Created response for client on PUT request");
                return new Response(Response.CREATED, Response.EMPTY);
            case Request.METHOD_DELETE:
                LOG.info("Created response for client on DELETE request");
                return new Response(Response.ACCEPTED, Response.EMPTY);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED, "Method not allowed".getBytes(UTF_8));
        }
    }

    private Action<Response> getRequestHandler(final Request request, final ByteBuffer key) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return () -> get(key);
            case Request.METHOD_PUT:
                return () -> put(request, key);
            case Request.METHOD_DELETE:
                return () -> delete(key);
            default:
                return null;
        }
    }

    private CompletableFuture<Response> proxy(final Request request, final String node) {
        return clientPool.get(node).proxy(request);
    }
}
