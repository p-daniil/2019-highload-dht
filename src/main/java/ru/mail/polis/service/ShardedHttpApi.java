package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.InternalDAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class ShardedHttpApi extends ShardedHttpApiBase {
    private static final Logger LOG = LoggerFactory.getLogger(ShardedHttpApi.class);

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
        super(port, dao, executor, topology);
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
            final Action<Response> action = getAction(request, session, key);
            if (action == null) return;
            executeAsync(session, action);
            return;
        }

        final RF rf = getRf(request, session);
        if (rf == null) return;
        LOG.info("New client request with RF {}", rf);

        final Set<String> primaryNodes = topology.primaryFor(key, rf.from);
        final List<Response> responses = new CopyOnWriteArrayList<>();
        final CountDownLatch latch = new CountDownLatch(rf.ack);
        LOG.info("Node {} started to poll nodes", topology.getMe());

        for (final String node : primaryNodes) {
            if (topology.isMe(node)) {
                final Action<Response> action = getAction(request, session, key);
                if (action == null) return;
                processLocally(responses, latch, action);
            } else {
                pollNode(request, responses, latch, node);
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
}
