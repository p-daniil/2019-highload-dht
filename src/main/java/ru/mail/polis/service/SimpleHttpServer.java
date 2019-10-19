package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import ru.mail.polis.dao.DAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class SimpleHttpServer extends HttpApiBase {

    /**
     * Blocking API for database.
     *
     * @param port port
     * @param dao  database DAO
     * @throws IOException if I/O errors occurred
     */
    public SimpleHttpServer(final int port, final DAO dao) throws IOException {
        super(port, dao);
    }

    /**
     * Method for handling "/v0/entity" requests.
     *
     * @param id      id of entity, passed in url
     * @param request received request
     * @return response to client
     */
    @Path("/v0/entity")
    public Response entity(@Param("id") final String id, final Request request) {

        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET: {
                    return get(key);
                }
                case Request.METHOD_PUT: {
                    return put(request, key);
                }
                case Request.METHOD_DELETE: {
                    return delete(key);
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

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }
}
