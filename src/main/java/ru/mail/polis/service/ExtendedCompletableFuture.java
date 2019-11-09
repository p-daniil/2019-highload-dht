package ru.mail.polis.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class ExtendedCompletableFuture extends CompletableFuture {

    public static <T> CompletableFuture<List<T>> firstN(
            List<CompletableFuture<T>> list, int n) {

        int maxFail = list.size() - n;
        if (maxFail < 0) throw new IllegalArgumentException();

        AtomicInteger fails = new AtomicInteger(0);
        List<T> rList = new ArrayList<>(n);

        CompletableFuture<List<T>> result = new CompletableFuture<>();

        BiConsumer<T, Throwable> c = (value, failure) -> {
            if (failure != null) {
                if (fails.incrementAndGet() > maxFail) result.completeExceptionally(failure);
            } else {
                if (!result.isDone()) {
                    boolean commit;
                    synchronized (rList) {
                        commit = rList.size() < n && rList.add(value) && rList.size() == n;
                    }
                    if (commit) {
                        result.complete(Collections.unmodifiableList(rList));
                    }
                }
            }
        };
        for (CompletableFuture<T> f : list) f.whenComplete(c);
        return result;
    }


}
