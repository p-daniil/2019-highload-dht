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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ru.mail.polis.dao.SSTable.Implementation.FILE_CHANNEL_READ;

public class MyDAO implements DAO {

    private static final Logger LOG = LoggerFactory.getLogger(MyDAO.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private static final SSTable.Implementation SSTABLE_IMPL = FILE_CHANNEL_READ;
    private static final ByteBuffer MIN_BYTE_BUFFER = ByteBuffer.allocate(0);
    private static final double LOAD_FACTOR = 0.016;

    private final Path tablesDir;

    private final MemoryTablePool memTable;
    private List<Table> ssTableList;
    private final FlusherThread flusher;

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
                    lock.writeLock().lock();
                    try {
                        flush(tableToFlush.getTable());
                        memTable.flushed(tableToFlush);
                    } finally {
                        lock.writeLock().unlock();
                    }
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

        final UnmodifiableIterator<Cell> mergeSortedIter =
                Iterators.mergeSorted(ssIterators, Comparator.naturalOrder());

        final Iterator<Cell> collapsedIter = Iters.collapseEquals(mergeSortedIter, Cell::getKey);

        return Iterators.filter(collapsedIter, cell -> !cell.getValue().isRemoved());
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
        final SSTable flushedTable = SSTable.flush(
                tablesDir,
                table.iterator(MIN_BYTE_BUFFER),
                table.getVersion(),
                SSTABLE_IMPL);
        LOG.info("Flushed in {} ms", System.currentTimeMillis() - startTime);
        ssTableList.add(flushedTable);
    }

    @Override
    public void compact() throws IOException {

        final Path actualFile = SSTable.writeTable(
                tablesDir,
                cellIterator(MIN_BYTE_BUFFER, false),
                memTable.getVersion());

        closeSSTables();
        SSTable.removeOldVersionsAndResetCounter(tablesDir, actualFile);
        ssTableList = SSTable.findVersions(tablesDir, SSTABLE_IMPL);

        assert ssTableList.size() == SSTable.MIN_TABLE_VERSION;
        int versionCounter = SSTable.MIN_TABLE_VERSION;

        memTable.setVersion(++versionCounter);
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
