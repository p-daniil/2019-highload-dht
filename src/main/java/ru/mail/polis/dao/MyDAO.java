package ru.mail.polis.dao;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.mail.polis.dao.SSTable.Impl.FILE_CHANNEL_READ;

public class MyDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(MyDAO.class);

    private static final SSTable.Impl SSTABLE_IMPL = FILE_CHANNEL_READ;
    private static final ByteBuffer MIN_BYTE_BUFFER = ByteBuffer.allocate(0);
    private static final double LOAD_FACTOR = 0.1;
    private static final double COMPACTION_THRESHOLD = 10;

    private final Path tablesDir;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final FlusherThread flusher;
    private final MemoryTablePool memTable;
    private List<Table> ssTableList;

    private class FlusherThread extends Thread {

        public FlusherThread() {
            super("Flusher");
        }

        @Override
        public void run() {
            boolean poisonReceived = false;
            while (!poisonReceived && !isInterrupted()) {
                TableToFlush tableToFlush = null;
                try {
                    tableToFlush = memTable.takeToFlush();
                    poisonReceived = tableToFlush.isPoisonPill();
                    flush(tableToFlush.getTable());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    LOG.error("Error while flushing version: " + tableToFlush.getVersion(), e);
                }
            }
            if (poisonReceived) {
                LOG.info("Poison pill received. Stop flushing.");
            }
        }
    }

    /**
     * DAO Implementation for LSM Database.
     *
     * @param tablesDir directory to store SSTable files
     * @param maxHeap   max memory, allocated for JVM
     * @throws IOException if unable to read existing SSTable files
     */
    public MyDAO(final Path tablesDir, final long maxHeap) throws IOException {

        this.ssTableList = SSTable.findVersions(tablesDir, SSTABLE_IMPL);
        int version = ssTableList.size();
        this.memTable = new MemoryTablePool((long) (maxHeap * LOAD_FACTOR), version++);
        this.flusher = new FlusherThread();
        this.flusher.start();

        this.tablesDir = tablesDir;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {

        return Iterators.transform(
                cellIterator(from, true),
                cell -> Record.of(cell.getKey(), cell.getValue().getData()));
    }

    private Iterator<Cell> cellIterator(
            @NotNull final ByteBuffer from,
            final boolean includeMemTable) throws IOException {

        final List<Iterator<Cell>> ssIterators = new ArrayList<>();

        lock.readLock().lock();
        try {
            for (final Table ssTable : ssTableList) {
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
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value.duplicate());
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key.duplicate());
    }

    private void flush(final Table table) throws IOException {
        LOG.info("Flushing...\n\tCurrent table size: {} bytes\n\tHeap free: {} bytes",
                table.getSize(),
                Runtime.getRuntime().freeMemory());
        final long startTime = System.currentTimeMillis();

        final Table flushedTable = SSTable.flush(
                tablesDir,
                table.iterator(MIN_BYTE_BUFFER),
                table.getVersion(),
                SSTABLE_IMPL);

        LOG.info("Flushed in {} ms", System.currentTimeMillis() - startTime);

        ssTableList.add(flushedTable);
        memTable.flushed(flushedTable);

        if (ssTableList.size() > COMPACTION_THRESHOLD) {
            compact();
        }
    }

    @Override
    public void compact() throws IOException {
        lock.readLock().lock();
        LOG.info("Compaction started...");
        final long startTime = System.currentTimeMillis();
        final Path actualFile;
        try {
            actualFile = SSTable.writeTable(
                    tablesDir,
                    cellIterator(MIN_BYTE_BUFFER, false),
                    memTable.getVersion());
        } finally {
            lock.readLock().unlock();
        }
        lock.writeLock().lock();
        try {
            closeSSTables();
            SSTable.removeOldVersionsAndResetCounter(tablesDir, actualFile);
            ssTableList = SSTable.findVersions(tablesDir, SSTABLE_IMPL);

            assert ssTableList.size() == SSTable.MIN_TABLE_VERSION;
            memTable.setVersion(SSTable.MIN_TABLE_VERSION + 1);
            LOG.info("Compaction finished in {} ms", System.currentTimeMillis() - startTime);
        } finally {
            lock.writeLock().unlock();
        }

    }

    private void closeSSTables() throws IOException {
        for (final Table t : ssTableList) {
            if (t instanceof Closeable) {
                ((Closeable) t).close();
            }
        }
    }

    @Override
    public void close() throws IOException {
        memTable.close();
        try {
            flusher.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            closeSSTables();
        }
    }
}
