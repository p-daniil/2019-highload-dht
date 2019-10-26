package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.RejectedSessionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

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
                   final DAO dao,
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

        final String primaryNode = topology.primaryFor(key);
        if (!topology.isMe(primaryNode)) {
            executeAsync(session, () -> proxy(request, primaryNode));
            return;
        }

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
            final Iterator<Record> records = dao.range(
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

    private Response proxy(final Request request, final String node) throws IOException {
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
    interface Action {
        Response act() throws IOException;
    }

}
