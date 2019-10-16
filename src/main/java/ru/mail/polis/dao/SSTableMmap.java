package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SSTableMmap extends SSTable {
    private final int rowCount;
    private final LongBuffer offsetArray;
    private final ByteBuffer dataArray;

    /** MMapped SSTable implementation.
     *
     * @param file directory of SSTable files
     * @throws IOException if unable to read SSTable files
     */
    public SSTableMmap(final Path file) throws IOException {
        super(file);
        
        MappedByteBuffer mappped = null;

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            mappped = (MappedByteBuffer) channel
                    .map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
                    .order(ByteOrder.BIG_ENDIAN);
        }

        final int rowCountOff = mappped.limit() - Integer.BYTES;

        final ByteBuffer rowCountDuplicate = mappped.duplicate();
        rowCountDuplicate.position(rowCountOff);
        rowCount = rowCountDuplicate.getInt();

        final int offsetArrayOff = mappped.limit() - Integer.BYTES - Long.BYTES * rowCount;

        final ByteBuffer offsetDuplicate = mappped.duplicate();
        offsetDuplicate.position(offsetArrayOff);
        offsetDuplicate.limit(rowCountOff);
        offsetArray = offsetDuplicate.slice().asLongBuffer();

        final ByteBuffer dataDuplicate = mappped.duplicate();
        dataDuplicate.limit(offsetArrayOff);
        dataArray = dataDuplicate.slice().asReadOnlyBuffer();
    }

    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {

            private int position = findStartIndex(from, 0, rowCount - 1);

            @Override
            public boolean hasNext() {
                return position < rowCount;
            }

            @Override
            public Cell next() {
                return parseCell(position++);
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException("SSTable is immutable");
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException("SSTable is immutable");
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public long getVersion() {
        return version;
    }

    private long receiveOffset(final int index) {
        return offsetArray.get(index);
    }

    private ByteBuffer parseKey(final long offset) {

        final ByteBuffer duplicate = dataArray.duplicate();
        duplicate.position((int) offset);
        final long keySize = duplicate.getLong();

        duplicate.limit((int) (duplicate.position() + keySize));

        return duplicate.slice();
    }

    @Override
    public ByteBuffer parseKey(final int index) {
        return parseKey(receiveOffset(index));
    }

    private Cell parseCell(final long offset) {
        final ByteBuffer cellDuplicate = dataArray.duplicate();
        cellDuplicate.position((int) offset);
        cellDuplicate.slice();

        final ByteBuffer keyDuplicate = cellDuplicate.duplicate();
        final long keySize = keyDuplicate.getLong();

        keyDuplicate.limit((int) (keyDuplicate.position() + keySize));

        final int timeStampOff = keyDuplicate.limit();

        final ByteBuffer key = keyDuplicate.slice();

        final ByteBuffer valueDuplicate = cellDuplicate.duplicate();
        valueDuplicate.position(timeStampOff);
        final long timeStamp = valueDuplicate.getLong();
        final boolean tombstone = valueDuplicate.get() != 0;

        if (tombstone) {
            return Cell.create(key, Value.tombstone(timeStamp), getVersion());
        } else {
            final long valueSize = valueDuplicate.getLong();
            valueDuplicate.limit((int) (valueDuplicate.position() + valueSize));
            final ByteBuffer value = valueDuplicate.slice();

            return Cell.create(key, Value.of(timeStamp, value), getVersion());
        }
    }

    @Override
    protected Cell parseCell(final int index) {
        return parseCell(receiveOffset(index));
    }
}
