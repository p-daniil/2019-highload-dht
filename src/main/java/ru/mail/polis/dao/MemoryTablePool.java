package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryTablePool implements Table, Closeable {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final NavigableMap<Long, Table> pendingFlush;
    private final BlockingQueue<TableToFlush> flushQueue;
    private final long memFlushThreshold;
    private final AtomicBoolean stop = new AtomicBoolean();

    private volatile MemTable current;

    /**
     * Implementation of memory table pool.
     *
     * @param memFlushThreshold threshold after which occurs flushing in-memory table to disk
     * @param version version of current table
     */
    public MemoryTablePool(final long memFlushThreshold, final long version) {
        this.memFlushThreshold = memFlushThreshold;
        this.current = new MemTable(version);
        this.pendingFlush = new TreeMap<>();
        this.flushQueue = new ArrayBlockingQueue<>(2);
    }

    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        lock.readLock().lock();
        final List<Iterator<Cell>> iterators = new ArrayList<>();

        try {
            iterators.add(current.iterator(from));

            for (final Table table : pendingFlush.values()) {
                iterators.add(table.iterator(from));
            }
        } finally {
            lock.readLock().unlock();
        }
        return Iters.cellIterator(iterators);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped");
        }
        lock.readLock().lock();
        try {
            current.upsert(key.duplicate(), value.duplicate());
        } finally {
            lock.readLock().unlock();
        }
        enqueueFlush();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        if (stop.get()) {
            throw new IllegalStateException("Already stopped");
        }
        lock.readLock().lock();
        try {
            current.remove(key.duplicate());
        } finally {
            lock.readLock().unlock();
        }
        enqueueFlush();
    }

    TableToFlush takeToFlush() throws InterruptedException {
        return flushQueue.take();
    }

    void flushed(final Table table) {
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
                    pendingFlush.put(current.getVersion(), current);
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
