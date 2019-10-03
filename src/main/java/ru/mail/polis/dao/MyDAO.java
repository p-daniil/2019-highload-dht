package ru.mail.polis.dao;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MyDAO implements DAO {

    private static final SSTable.Implementation SSTABLE_IMPL = SSTable.Implementation.FILE_CHANNEL_READ;
    private static final ByteBuffer MIN_BYTE_BUFFER = ByteBuffer.allocate(0);
    private static final double LOAD_FACTOR = 0.016;

    private final long allowableMemTableSize;

    private final Path tablesDir;
    
    private int versionCounter;

    private MemTable memTable;
    private List<SSTable> ssTableList;

    /** 
     * DAO Implementation for LSM Database.
     *
     * @param tablesDir directory to store SSTable files
     * @param maxHeap max memory, allocated for JVM
     * @throws IOException if unable to read existing SSTable files
     */
    public MyDAO(final Path tablesDir, final long maxHeap) throws IOException {
        
        ssTableList = SSTable.findVersions(tablesDir, SSTABLE_IMPL);
        versionCounter = ssTableList.size();
        memTable = new MemTable(++versionCounter);
        
        this.allowableMemTableSize = (long) (maxHeap * LOAD_FACTOR);
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

        for (final SSTable ssTable : ssTableList) {
            ssIterators.add(ssTable.iterator(from));
        }

        if (includeMemTable) {
            ssIterators.add(memTable.iterator(from));
        }

        final UnmodifiableIterator<Cell> mergeSortedIter =
                Iterators.mergeSorted(ssIterators, Comparator.naturalOrder());

        final Iterator<Cell> collapsedIter = Iters.collapseEquals(mergeSortedIter, Cell::getKey);

        return Iterators.filter(collapsedIter, cell -> !cell.getValue().isRemoved());
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value.duplicate());
        if (memTable.getSize() > allowableMemTableSize) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key.duplicate());
        if (memTable.getSize() > allowableMemTableSize) {
            flush();
        }
    }

    private void flush() throws IOException {
        DebugUtils.flushInfo(memTable);

        ssTableList.add(SSTable.flush(
                tablesDir, 
                memTable.iterator(MIN_BYTE_BUFFER),
                memTable.getVersion(),
                SSTABLE_IMPL));

        memTable = new MemTable(++versionCounter);
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
        versionCounter = SSTable.MIN_TABLE_VERSION;

        memTable.setVersion(++versionCounter);
    }

    private void closeSSTables() throws IOException {
        for (final SSTable t : ssTableList) {
            if (t instanceof SSTableFileChannel) {
                ((SSTableFileChannel) t).close();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.getSize() != 0) {
            flush();
        }
        closeSSTables();
    }
}
