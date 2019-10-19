package ru.mail.polis.service;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.NoSuchElementLite;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class HttpApiBase extends HttpServer implements Service {
    protected final DAO dao;

    /**
     * Common part of API for database.
     *
     * @param port port
     * @param dao  database DAO
     * @throws IOException if I/O errors occurred
     */
    public HttpApiBase(final int port, final DAO dao) throws IOException {
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
    protected Response delete(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @NotNull
    protected Response put(final Request request, final ByteBuffer key) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @NotNull
    protected Response get(final ByteBuffer key) throws IOException {
        final ByteBuffer value;
        try {
            value = dao.get(key);
        } catch (NoSuchElementLite e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        final ByteBuffer duplicate = value.duplicate();
        final byte[] body = new byte[duplicate.remaining()];
        duplicate.get(body);
        return new Response(Response.OK, body);
    }
}
