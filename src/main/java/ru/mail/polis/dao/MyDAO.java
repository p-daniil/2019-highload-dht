package ru.mail.polis.dao;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MyDAO implements DAO, InternalDAO, Flushable {
    private static final Logger LOG = LoggerFactory.getLogger(MyDAO.class);

    private static final ByteBuffer MIN_BYTE_BUFFER = ByteBuffer.allocate(0);

    private final Path tablesDir;

    private final DaoOptions options;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final FlusherThread flusher;
    private final MemoryTablePool memTable;
    private volatile Deque<SSTable> ssTableDeque;

    private final ReentrantLock compactionLock = new ReentrantLock();
    private final Condition needCompaction = compactionLock.newCondition();
    private final AtomicBoolean stopCompaction = new AtomicBoolean(false);
    private final CompactionThread compactionThread;

    private class CompactionThread extends Thread {

        CompactionThread() {
            super("Compaction");
        }

        @Override
        public void run() {
            while (!isInterrupted()) {
                compactionLock.lock();
                try {
                    needCompaction.await();
                } catch (InterruptedException e) {
                    LOG.info("Compaction interrupted");
                    Thread.currentThread().interrupt();
                } finally {
                    compactionLock.unlock();
                }
                if (ssTableDeque.size() < options.getCompactionThreshold()) continue;
                if (!stopCompaction.get()) {
                    try {
                        compact();
                    } catch (IOException e) {
                        LOG.error("Error while compacting", e);
                    }
                }
            }
            LOG.info("Compaction thread stopped");
        }
    }

    /**
     * DAO Implementation for LSM Database.
     *
     * @param tablesDir directory to store SSTable files
     * @param maxHeap   max memory, allocated for JVM
     * @throws IOException if unable to read existing SSTable files
     */
    MyDAO(final Path tablesDir, final long maxHeap, final DaoOptions options) throws IOException {
        this.ssTableDeque = SSTable.findVersions(tablesDir, options.getSsTableImpl());
        final SSTable lastAddedSSTable = ssTableDeque.peekFirst();
        long version = lastAddedSSTable == null ? 0 : lastAddedSSTable.getVersion();
        this.memTable = new MemoryTablePool((long) (maxHeap * options.getLoadFactor()), ++version);
        this.options = options;

        this.flusher = new FlusherThread(this, memTable);
        this.flusher.start();

        this.compactionThread = new CompactionThread();
        this.compactionThread.start();

        this.tablesDir = tablesDir;
    }

    @Override
    public Value getValue(final ByteBuffer key) throws IOException {
        final Iterator<Cell> iter = cellIterator(key, true);
        if (!iter.hasNext()) {
            throw new NoSuchElementLiteException("Not found");
        }

        final Cell next = iter.next();
        if (next.getKey().equals(key)) {
            return next.getValue();
        } else {
            throw new NoSuchElementLiteException("Not found");
        }
    }

    @Override
    public void upsertValue(final ByteBuffer key, final ByteBuffer value) {
        upsert(key, value);
    }

    @Override
    public void removeValue(final ByteBuffer key) {
        remove(key);
    }

    @Override
    public Iterator<Record> recordRange(final ByteBuffer from, final ByteBuffer to) throws IOException {
        return range(from, to);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Cell> cellIterator = cellIterator(from, true);
        final UnmodifiableIterator<Cell> filteredIter =
                Iterators.filter(cellIterator, cell -> !cell.getValue().isRemoved());
        return Iterators.transform(
                filteredIter,
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    private Iterator<Cell> cellIterator(
            @NotNull final ByteBuffer from,
            final boolean includeMemTable) throws IOException {

        final List<Iterator<Cell>> ssIterators = new ArrayList<>();

        lock.readLock().lock();
        try {
            for (final Table ssTable : ssTableDeque) {
                ssIterators.add(ssTable.iterator(from));
            }

            if (includeMemTable) {
                ssIterators.add(memTable.iterator(from));
            }
        } finally {
            lock.readLock().unlock();
        }
        return Iters.cellIterator(ssIterators);
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        memTable.upsert(key.duplicate(), value.duplicate());
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        memTable.remove(key.duplicate());
    }

    @Override
    public void flush(final Table table) throws IOException {
        LOG.info("Flushing...\n\tCurrent table size: {} bytes\n\tHeap free: {} bytes",
                table.getSize(),
                Runtime.getRuntime().freeMemory());
        final long startTime = System.currentTimeMillis();

            final SSTable flushedTable = SSTable.flush(
                    tablesDir,
                    table.iterator(MIN_BYTE_BUFFER),
                    table.getVersion(),
                    options.getSsTableImpl());

            LOG.info("Flushed in {} ms", System.currentTimeMillis() - startTime);

            ssTableDeque.addFirst(flushedTable);
            memTable.flushed();

        if (ssTableDeque.size() > options.getCompactionThreshold() && compactionLock.tryLock()) {
            try {
                needCompaction.signal();
            } finally {
                compactionLock.unlock();
            }
        }
    }

    @Override
    public void compact() throws IOException {
        if (compactionLock.tryLock()) {
            try {
                compactImpl();
            } finally {
                compactionLock.unlock();
            }
        }
    }

    private void compactImpl() throws IOException {
        LOG.info("Compaction started...");
        final long startTime = System.currentTimeMillis();

        final List<SSTable> tablesToCompact = new ArrayList<>(ssTableDeque);

        // Write table with unreachable version to avoid file rewriting while flushing
        final Path compactedFile = SSTable.writeTable(
                tablesDir,
                cellIterator(MIN_BYTE_BUFFER, false),
                SSTable.MIN_TABLE_VERSION - 1);

        final Path compactedFileReseted;
        lock.writeLock().lock();
        try {
            // Remove old tables
            for (final SSTable ssTable : tablesToCompact) {
                ssTableDeque.removeLast();
            }
            // Now we can rewrite oldest version of table
            compactedFileReseted = SSTable.resetTableVersion(compactedFile);
            final SSTable compactedSsTable = SSTable.createSSTable(compactedFileReseted, options.getSsTableImpl());
            // And add compacted table
            ssTableDeque.addLast(compactedSsTable);
        } finally {
            lock.writeLock().unlock();
        }

        // Delete unused table files
        for (final SSTable t : tablesToCompact) {
            final Path file = t.getFile();
            // Except file, which we overwrite (now it is compacted table file)
            if (!file.equals(compactedFileReseted)) {
                if (t instanceof Closeable) {
                    ((Closeable) t).close();
                }
                Files.delete(file);
            }
        }
        LOG.info("Compaction finished in {} ms", System.currentTimeMillis() - startTime);
    }

    private void closeSSTables(final Deque<SSTable> ssTableList) throws IOException {
        for (final Table t : ssTableList) {
            if (t instanceof Closeable) {
                ((Closeable) t).close();
            }
        }
    }

    private void closeSSTables() throws IOException {
        closeSSTables(ssTableDeque);
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing DAO...");
        memTable.close();
        try {
            flusher.join();
            LOG.info("Flusher closed");
            LOG.info("Waiting for finish of compaction");
            stopCompaction.set(true);
            compactionLock.lock();
            try {
                LOG.info("Compaction finished, interrupt it");
                compactionThread.interrupt();

            } finally {
                compactionLock.unlock();
            }
            compactionThread.join();
            LOG.info("Compaction closed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            closeSSTables();
        }
        LOG.info("DAO closed");
    }
}
