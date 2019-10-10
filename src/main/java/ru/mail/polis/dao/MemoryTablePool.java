package ru.mail.polis.dao;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryTablePool implements Table, Closeable {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile MemTable current;
    private final NavigableMap<Long, Table> pendingFlush;
    private final BlockingQueue<TableToFlush> flushQueue;

    private final long memFlushThreshold;

    private final AtomicBoolean stop = new AtomicBoolean();

    public MemoryTablePool(final long memFlushThreshold, final long version) {
        this.memFlushThreshold = memFlushThreshold;
        this.current = new MemTable(version);
        this.pendingFlush = new TreeMap<>();
        this.flushQueue = new ArrayBlockingQueue<>(2);
    }

    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        lock.readLock().lock();
        final List<Iterator<Cell>> iterators = new ArrayList<>();

        try {
            iterators.add(current.iterator(from));

            for (final Table table : pendingFlush.descendingMap().values()) {
                iterators.add(table.iterator(from));
            }
        } finally {
            lock.readLock().unlock();
        }

        final UnmodifiableIterator<Cell> mergeSortedIter =
                Iterators.mergeSorted(iterators, Comparator.naturalOrder());

        final Iterator<Cell> collapsedIter = Iters.collapseEquals(mergeSortedIter, Cell::getKey);

        return Iterators.filter(collapsedIter, cell -> !cell.getValue().isRemoved());
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped");
        }
        current.upsert(key.duplicate(), value.duplicate());
        enqueueFlush();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped");
        }
        current.remove(key.duplicate());
        enqueueFlush();
    }

    TableToFlush takeToFlush() throws InterruptedException {
        return flushQueue.take();
    }

    void flushed(final TableToFlush table) {
        lock.writeLock().lock();
        try {
            pendingFlush.remove(table.getVersion());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void enqueueFlush() {
        if (current.getSize() > memFlushThreshold) {

            lock.writeLock().lock();
            TableToFlush tableToFlush = null;
            try {
                if (current.getSize() > memFlushThreshold) {
                    tableToFlush = new TableToFlush(current);
                    pendingFlush.put(tableToFlush.getVersion(), tableToFlush.getTable());
                    current = new MemTable(current.getVersion() + 1);
                }
            } finally {
                lock.writeLock().unlock();
            }

            if (tableToFlush != null) {
                try {
                    flushQueue.put(tableToFlush);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        }
    }

    @Override
    public long getSize() {
        lock.readLock().lock();
        try {
            long size = current.getSize();
            for (final Table table : pendingFlush.values()) {
                size += table.getSize();
            }
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long getVersion() {
        return current.getVersion();
    }

    public void setVersion(long version) {
        current.setVersion(version);
    }

    @Override
    public void close() throws IOException {
        if (!stop.compareAndSet(false, true)) {
            return;
        }
        lock.writeLock().lock();
        TableToFlush tableToFlush;
        try {
            tableToFlush = new TableToFlush(current, true);
            pendingFlush.put(tableToFlush.getVersion(), tableToFlush.getTable());
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(tableToFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
