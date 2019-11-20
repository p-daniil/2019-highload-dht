package ru.mail.polis.service;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

class ExtendedCompletableFuture<T> extends CompletableFuture<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ExtendedCompletableFuture.class);

    static <T> CompletableFuture<List<T>> firstN(final List<CompletableFuture<T>> list, final int n) {
        final int maxFail = list.size() - n;
        if (maxFail < 0) throw new IllegalArgumentException();

        final AtomicInteger fails = new AtomicInteger(0);
        final List<T> resultList = new ArrayList<>(n);
        final ReadWriteLock lock = new ReentrantReadWriteLock();

        final CompletableFuture<List<T>> result = new CompletableFuture<>();

        final BiConsumer<T, Throwable> c = getResultHandler(n, maxFail, fails, resultList, lock, result);
        for (final CompletableFuture<T> f : list) f.whenCompleteAsync(c).exceptionally(e -> {
            LOG.error("Failed to handle result", e);
            return null;
        });
        return result;
    }

    @NotNull
    private static <T> BiConsumer<T, Throwable> getResultHandler(final int n,
                                                                 final int maxFail,
                                                                 final AtomicInteger fails,
                                                                 final List<T> resultList,
                                                                 final ReadWriteLock lock,
                                                                 final CompletableFuture<List<T>> future) {
        return (value, failure) -> {
            if (failure == null) {
                if (!future.isDone()) {
                    lock.readLock().lock();
                    try {
                        resultList.add(value);
                        if (resultList.size() == n) {
                            future.complete(Collections.unmodifiableList(resultList));
                        }
                    } finally {
                        lock.readLock().unlock();
                    }
                }
            } else {
                if (fails.incrementAndGet() > maxFail) future.completeExceptionally(failure);
            }
        };
    }
}
