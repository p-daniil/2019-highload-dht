package ru.mail.polis.service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
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
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import static java.nio.charset.StandardCharsets.UTF_8;

class AsyncClient {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncClient.class);

    private static final String TIMESTAMP_HEADER = "Timestamp";
    private static final String PROXY_HEADER = "Node-polling";
    private final HttpClient client;

    AsyncClient() {
        final Executor clientExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("client-%d").build());
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.of(500, ChronoUnit.MILLIS))
                .executor(clientExecutor)
                .build();
    }

    CompletableFuture<Response> proxy(final Request request, final String address) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(request, address);
            case Request.METHOD_PUT:
                return put(request, address);
            case Request.METHOD_DELETE:
                return delete(request, address);
            default:
                return CompletableFuture.completedFuture(new Response(Response.METHOD_NOT_ALLOWED,
                        "Method not allowed".getBytes(UTF_8)));
        }
    }

    private CompletableFuture<Response> get(final Request request, final String address) {
        final HttpRequest asyncRequest = requestBase(request, address)
                .GET()
                .build();
        return sendAsync(asyncRequest, request.getMethod());
    }

    private CompletableFuture<Response> put(final Request request, final String address) {
        final HttpRequest asyncRequest = requestBase(request, address)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(request.getBody()))
                .build();
        return sendAsync(asyncRequest, request.getMethod());
    }

    private CompletableFuture<Response> delete(final Request request, final String address) {
        final HttpRequest asyncRequest = requestBase(request, address)
                .DELETE()
                .build();
        return sendAsync(asyncRequest, request.getMethod());
    }

    private CompletableFuture<Response> sendAsync(final HttpRequest asyncRequest, final int method) {
        final CompletableFuture<Response> future = new CompletableFuture<>();
        switch (method) {
            case Request.METHOD_GET:
                client.sendAsync(asyncRequest, HttpResponse.BodyHandlers.ofByteArray())
                        .whenCompleteAsync(getResponseWithBodyHandler(future))
                        .exceptionally(e -> {
                            LOG.error("Failed to handle result", e);
                            return null;
                        });
                break;
            case Request.METHOD_PUT:
            case Request.METHOD_DELETE:
                client.sendAsync(asyncRequest, HttpResponse.BodyHandlers.discarding())
                        .whenCompleteAsync(getEmptyBodyResponseHandler(future))
                        .exceptionally(e -> {
                            LOG.error("Failed to handle result", e);
                            return null;
                        });
                break;
            default:
                return CompletableFuture.completedFuture(new Response(Response.METHOD_NOT_ALLOWED,
                        "Method not allowed".getBytes(UTF_8)));
        }
        return future;
    }

    private HttpRequest.Builder requestBase(final Request request, final String address) {
        return HttpRequest.newBuilder()
                .header(PROXY_HEADER, "true")
                .uri(URI.create(address + request.getURI()))
                .timeout(Duration.of(500, ChronoUnit.MILLIS));
    }

    @NotNull
    private BiConsumer<HttpResponse<Void>, Throwable> getEmptyBodyResponseHandler(
            final CompletableFuture<Response> future) {
        return (httpResponse, throwable) -> {
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
            if (throwable == null) {
                final Response response = new Response(
                        String.valueOf(httpResponse.statusCode()),
                        httpResponse.body());
                httpResponse.headers()
                        .firstValue(TIMESTAMP_HEADER)
                        .ifPresent(timestamp -> response.addHeader(TIMESTAMP_HEADER + ": " + timestamp));
                future.complete(response);
            } else {
                future.completeExceptionally(throwable);
            }
        };
    }
}
