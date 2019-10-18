package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.NoSuchElementLite;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

import static one.nio.http.Response.*;

public class AsyncHttpApi extends HttpServer implements Service {

    private final int port;
    private final DAO dao;
    private final Executor executor;

    public AsyncHttpApi(int port, DAO dao, Executor executor) throws IOException {
        super(getConfig(port));
        this.port = port;
        this.dao = dao;
        this.executor = executor;
    }

    /**
     * Method for handling "/v0/status" requests.
     *
     * @param request received request
     * @return response to client
     */
    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response status(final Request request) {
        return new Response(Response.OK, Response.EMPTY);
    }

    /**
     * Method for handling "/v0/entity" requests.
     *
     * @param request received request
     */
    public void entity(final Request request,
                       final HttpSession session) throws IOException {
        String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No id");
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
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
                    session.sendError(METHOD_NOT_ALLOWED, "Allowed only get, put and delete");
                }
            }
        } catch (NoSuchElementException e) {
            session.sendError(NOT_FOUND, "Key not found");
        }
    }

    public void entities(final Request request,
                         final HttpSession session) throws IOException {
        final String start = request.getParameter("start=");

        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No start");
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            session.sendError(METHOD_NOT_ALLOWED, "Wrong method");
            return;
        }
        String end = request.getParameter("end=");
        if (end != null && end.isEmpty()) {
            end = null;
        }

        try {
            final Iterator<Record> records = dao.range(
                    ByteBuffer.wrap(start.getBytes()),
                    end == null ? null : ByteBuffer.wrap(end.getBytes()));
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(INTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public HttpSession createSession(Socket socket) throws RejectedSessionException {
        return new StorageSession(socket, this);
    }

    @NotNull
    private Response delete(ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @NotNull
    private Response put(Request request, ByteBuffer key) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @NotNull
    private Response get(ByteBuffer key) throws IOException {
        final ByteBuffer value;
        try {
            value = dao.get(key);
        } catch (NoSuchElementLite e) {
            return new Response(NOT_FOUND, EMPTY);
        }
        final ByteBuffer duplicate = value.duplicate();
        final byte[] body = new byte[duplicate.remaining()];
        duplicate.get(body);
        return new Response(Response.OK, body);
    }

    private void executeAsync(final HttpSession session, final Action action) {
        executor.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (Exception e) {
                try {
                    session.sendError(INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        });
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65535) {
            throw new IllegalArgumentException("Invalid port");
        }
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final HttpServerConfig serverConfig = new HttpServerConfig();
        serverConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        return serverConfig;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
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
                session.sendError(BAD_REQUEST, "Wrong path");
            }
        }
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }

}
