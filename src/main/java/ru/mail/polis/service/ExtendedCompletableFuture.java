package ru.mail.polis.service;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

class ExtendedCompletableFuture<T> extends CompletableFuture<T> {
    private static final ReadWriteLock lock = new ReentrantReadWriteLock();

    @SuppressWarnings("FutureReturnValueIgnored")
    static <T> CompletableFuture<List<T>> firstN(final List<CompletableFuture<T>> list, final int n) {
        final int maxFail = list.size() - n;
        if (maxFail < 0) throw new IllegalArgumentException();

        final AtomicInteger fails = new AtomicInteger(0);
        final List<T> rList = new ArrayList<>(n);

        final CompletableFuture<List<T>> result = new CompletableFuture<>();

        final BiConsumer<T, Throwable> c = getResultHandler(n, maxFail, fails, rList, result);
        for (final CompletableFuture<T> f : list) f.whenCompleteAsync(c);
        return result;
    }

    @NotNull
    private static <T> BiConsumer<T, Throwable> getResultHandler(final int n,
                                                                 final int maxFail,
                                                                 final AtomicInteger fails,
                                                                 final List<T> rList,
                                                                 final CompletableFuture<List<T>> result) {
        return (value, failure) -> {
                if (failure == null) {
                    if (!result.isDone()) {
                        final boolean commit;
                        lock.readLock().lock();
                        try {
                            commit = rList.size() < n && rList.add(value) && rList.size() == n;
                        } finally {
                            lock.readLock().unlock();
                        }
                        if (commit) {
                            result.complete(Collections.unmodifiableList(rList));
                        }
                    }
                } else {
                    if (fails.incrementAndGet() > maxFail) result.completeExceptionally(failure);
                }
            };
    }
}
