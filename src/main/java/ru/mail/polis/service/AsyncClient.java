package ru.mail.polis.service;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import static java.nio.charset.StandardCharsets.UTF_8;

class AsyncClient {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncClient.class);
    private static final String TIMESTAMP_HEADER = "Timestamp: ";
    private final HttpClient client;
    private final String nodeAddress;

    AsyncClient(final String nodeAddress, final Executor executor) {
        this.client = HttpClient.newBuilder()
                .executor(executor)
                .connectTimeout(Duration.of(50, ChronoUnit.MILLIS))
                .build();
        this.nodeAddress = nodeAddress;
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    CompletableFuture<Response> proxy(final Request request) {
        final String method = getMethodString(request.getMethod());
        if (method == null) {
            return CompletableFuture.completedFuture(new Response(Response.METHOD_NOT_ALLOWED,
                    "Method not allowed".getBytes(UTF_8)));
        }
        final HttpRequest asyncRequest = createRequest(request, method);
        LOG.info("Created request");
        final CompletableFuture<Response> future = new CompletableFuture<>();
        if (request.getMethod() == Request.METHOD_GET) {
            LOG.info("Execute get request");
            client.sendAsync(asyncRequest, HttpResponse.BodyHandlers.ofByteArray())
                    .whenCompleteAsync(getResponseWithBodyHandler(future));
        } else {
            LOG.info("Execute put or delete request");
            client.sendAsync(asyncRequest, HttpResponse.BodyHandlers.discarding())
                    .whenCompleteAsync(getEmptyBodyResponseHandler(future));
        }
        return future;
    }

    @NotNull
    private BiConsumer<HttpResponse<Void>, Throwable> getEmptyBodyResponseHandler(
            final CompletableFuture<Response> future) {
        return (httpResponse, throwable) -> {
            LOG.info("Received response with empty body");
            if (throwable == null) {
                future.complete(new Response(String.valueOf(httpResponse.statusCode()), Response.EMPTY));
            } else {
                future.completeExceptionally(throwable);
            }
        };
    }

    @NotNull
    private BiConsumer<HttpResponse<byte[]>, Throwable> getResponseWithBodyHandler(
            final CompletableFuture<Response> future) {
        return (httpResponse, throwable) -> {
            LOG.info("Received response on GET");
            if (throwable == null) {
                final Response response = new Response(
                        String.valueOf(httpResponse.statusCode()),
                        httpResponse.body());

                final String timestamp = httpResponse.headers()
                        .firstValue("Timestamp")
                        .orElse("");
                if (!timestamp.isEmpty()) {
                    response.addHeader(TIMESTAMP_HEADER + timestamp);
                }

                future.complete(response);
            } else {
                future.completeExceptionally(throwable);
            }
        };
    }

    private HttpRequest createRequest(final Request request, final String method) {
        return HttpRequest.newBuilder()
                .header("Node-polling", "true")
                .uri(URI.create(nodeAddress + request.getURI()))
                .method(method, "PUT".equals(method)
                        ? HttpRequest.BodyPublishers.ofByteArray(request.getBody()) :
                        HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.of(500, ChronoUnit.MILLIS))
                .build();
    }

    @Nullable
    private static String getMethodString(final int method) {
        switch (method) {
            case Request.METHOD_GET:
                return "GET";
            case Request.METHOD_PUT:
                return "PUT";
            case Request.METHOD_DELETE:
                return "DELETE";
            default:
                return null;
        }
    }
}
