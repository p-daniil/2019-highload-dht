package ru.mail.polis.service;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static java.nio.charset.StandardCharsets.UTF_8;

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
     */
    private void entity(final Request request, final HttpSession session) {
        final String id = request.getParameter("id=");
        if (id == null || id.isEmpty()) {
            sendResponseAsync(new Response(Response.BAD_REQUEST, "No id".getBytes(UTF_8)), session);
            return;
        }

        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(UTF_8));

        final String nodePollingHeader = request.getHeader(PROXY_HEADER);
        if (nodePollingHeader != null) {
            sendResponseAsync(processNodeRequest(request, key), session);
            return;
        }

        final RF rf;
        try {
            rf = getRf(request);
        } catch (IllegalArgumentException e) {
            sendResponseAsync(new Response(Response.BAD_REQUEST, e.getMessage().getBytes(UTF_8)), session);
            return;
        }
        LOG.info("New client request with RF {}", rf);
        sendResponseAsync(processClientRequest(request, key, rf), session);
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private void sendResponseAsync(final CompletableFuture<Response> responseFuture, final HttpSession session) {
        responseFuture.whenCompleteAsync((response, fail) -> {
            if (fail == null) {
                try {
                    session.sendResponse(response);
                } catch (IOException e) {
                    LOG.error("Failed to send response", e);
                }
            } else {
                LOG.error("Failed to create response", fail);
                try {
                    session.sendError(Response.INTERNAL_ERROR, "Failed to create response");
                } catch (IOException ex) {
                    LOG.error("Failed to send error", ex);
                }
            }
        });
    }

    private void sendResponseAsync(final Response response, final HttpSession session) {
        sendResponseAsync(CompletableFuture.completedFuture(response), session);
    }

    /**
     * Method for handling "/v0/entities" requests.
     *
     * @param request received request
     * @param session current http session
     * @throws IOException if I/O errors occurred
     */
    private void entities(final Request request, final HttpSession session) throws IOException {
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
                    ByteBuffer.wrap(start.getBytes(UTF_8)),
                    end == null ? null : ByteBuffer.wrap(end.getBytes(UTF_8)));
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
            case "/v0/entity":
                entity(request, session);
                break;
            case "/v0/entities":
                entities(request, session);
                break;
            default:
                sendResponseAsync(new Response(Response.BAD_REQUEST, "Wrong path".getBytes(UTF_8)), session);
                break;
        }
    }
}
