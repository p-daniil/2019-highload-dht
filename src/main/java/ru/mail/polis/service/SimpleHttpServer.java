package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class SimpleHttpServer extends HttpServer implements Service {

    private final DAO dao;

    public SimpleHttpServer(final int port, final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response status(final Request request) {
        return new Response(Response.OK, Response.EMPTY);
    }

    @Path("/v0/entity")
    public Response entity(@Param("id") final String id, final Request request) {

        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET: {
                    final ByteBuffer value = dao.get(key);
                    final ByteBuffer duplicate = value.duplicate();
                    final byte[] body = new byte[duplicate.remaining()];
                    duplicate.get(body);
                    return new Response(Response.OK, body);
                }
                case Request.METHOD_PUT: {
                    dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                    return new Response(Response.CREATED, Response.EMPTY);
                }
                case Request.METHOD_DELETE: {
                    dao.remove(key);
                    return new Response(Response.ACCEPTED, Response.EMPTY);
                }
                default: {
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
                }
            }
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
        }
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
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }
}
