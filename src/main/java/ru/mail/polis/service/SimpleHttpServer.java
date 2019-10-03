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

    @Path("/v0/entity")
    public Response entity(@Param("id") final String id, final Request request) {
        if (id == null) {
            return new Response(Response.BAD_REQUEST);
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET: {
                    final ByteBuffer value;
                    try {
                        value = dao.get(key);
                    } catch (NoSuchElementException e) {
                        return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
                    }
                    final ByteBuffer duplicate = value.duplicate();
                    final byte[] body = new byte[duplicate.remaining()];
                    duplicate.get(body);
                    return new Response(Response.OK, body);
                }
                case Request.METHOD_PUT: {
                    dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                    return new Response(Response.CREATED);
                }
                case Request.METHOD_DELETE: {
                    dao.remove(key);
                    return new Response(Response.ACCEPTED);
                }
                default: {
                    return new Response(Response.METHOD_NOT_ALLOWED);
                }
            }
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR);
        }
    }

    public static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65535) {
            throw new IllegalArgumentException("Invalid port");
        }
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final HttpServerConfig serverConfig = new HttpServerConfig();
        serverConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        return serverConfig;
    }
}
