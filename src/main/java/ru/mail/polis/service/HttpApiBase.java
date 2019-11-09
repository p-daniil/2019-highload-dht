package ru.mail.polis.service;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.InternalDAO;
import ru.mail.polis.dao.NoSuchElementLiteException;
import ru.mail.polis.dao.Value;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class HttpApiBase extends HttpServer implements Service {
    static final String PROXY_HEADER = "Node-polling: true";
    static final String TIMESTAMP_HEADER = "Timestamp: ";

    protected final InternalDAO dao;

    /**
     * Common part of API for database.
     *
     * @param port port
     * @param dao  database DAO
     * @throws IOException if I/O errors occurred
     */
    HttpApiBase(final int port, final InternalDAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
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

    @NotNull
    protected Response get(final ByteBuffer key) throws IOException {
        final Value value;
        try {
            value = dao.getValue(key);
        } catch (NoSuchElementLiteException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        if (value.isRemoved()) {
            final Response response = new Response(Response.NOT_FOUND, Response.EMPTY);
            response.addHeader(TIMESTAMP_HEADER + value.getTimeStamp());
            return response;
        }
        final ByteBuffer duplicate = value.getData().duplicate();
        final byte[] body = new byte[duplicate.remaining()];
        duplicate.get(body);
        final Response response = new Response(Response.OK, body);
        response.addHeader(TIMESTAMP_HEADER + value.getTimeStamp());
        return response;
    }

    protected Response put(@NotNull final Request request, final ByteBuffer key) {
        dao.upsertValue(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    protected Response delete(final ByteBuffer key) {
        dao.removeValue(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
