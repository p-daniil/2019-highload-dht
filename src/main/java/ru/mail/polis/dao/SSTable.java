package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class SSTable implements Table {

    protected static final int MIN_TABLE_VERSION = 1;
    protected static final String TABLE_FILE_SUFFIX = ".dat";
    protected static final String TABLE_TMP_FILE_SUFFIX = ".tmp";
    protected static final String TABLE_FILE_PREFIX = "table_";
    protected static final String FILE_NAME_PATTERN = TABLE_FILE_PREFIX + "(\\d+)" + TABLE_FILE_SUFFIX;
    
    protected final Path file;
    protected final long size;
    protected final long version;

    enum Implementation {
        FILE_CHANNEL_READ,
        MMAPPED
    }

    /**
     * Base implementation of SSTable.
     * 
     * @param file SSTable file
     */
    public SSTable(final Path file) throws IOException {
        this.file = file;
        size = Files.size(file);
        version = getVersionFromName(file.getFileName().toString());
    }

    protected int findStartIndex(final ByteBuffer from, final int low, final int high) throws IOException {
        int curLow = low;
        int curHigh = high;

        while (curLow <= curHigh) {
            final int mid = (curLow + curHigh) / 2;

            final ByteBuffer midKey = parseKey(mid);

            final int compare = midKey.compareTo(from);

            if (compare < 0) {
                curLow = mid + 1;
            } else if (compare > 0) {
                curHigh = mid - 1;
            } else {
                return mid;
            }
        }
        return curLow;
    }

    /** 
     * Finds versions of SSTables in given directory.
     *
     * @param tablesDir directory to find SSTable files
     * @param impl type of SSTable implementation
     * @return list of SSTable abstractions
     * @throws IOException if unable to read directory
     */
    protected static List<Table> findVersions(
            final Path tablesDir,
            final Implementation impl) throws IOException {
        
        final List<Table> ssTables = new CopyOnWriteArrayList<>();
        Files.walkFileTree(tablesDir, EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
            
            @Override
            public FileVisitResult visitFile(
                    final Path file, 
                    final BasicFileAttributes attrs) throws IOException {
                
                if (checkFileName(file.getFileName().toString())) {
                    
                    if (impl == Implementation.FILE_CHANNEL_READ) {
                        ssTables.add(new SSTableFileChannel(file));
                    } else {
                        ssTables.add(new SSTableMmap(file));
                    }
                    
                }
                return FileVisitResult.CONTINUE;
            }
            
        });
        return ssTables;
    }

    /**
     * Removes old versions of SSTables and reset version of actual SSTable file to min value.
     *
     * @param tablesDir directory to find SSTable files
     * @param actualFile file with actual version of SSTable
     * @throws IOException if unable to read directory
     */
    protected static void removeOldVersionsAndResetCounter(
            final Path tablesDir,
            final Path actualFile) throws IOException {

        Files.walkFileTree(tablesDir, EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult visitFile(
                    final Path file,
                    final BasicFileAttributes attrs) throws IOException {

                if (!file.equals(actualFile)) {
                    Files.delete(file);
                }

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                resetTableVersion(actualFile);
                return FileVisitResult.CONTINUE;
            }

            private void resetTableVersion(final Path tableFile) throws IOException {
                changeTableVersion(tableFile, MIN_TABLE_VERSION);
            }
        });
    }

    private static void changeTableVersion(final Path tableFile, final long newVersion) throws IOException {
        Files.move(tableFile, tableFile.resolveSibling(createName(newVersion)), StandardCopyOption.ATOMIC_MOVE);
    }

    private static String createName(final long version) {
        return TABLE_FILE_PREFIX + version + TABLE_FILE_SUFFIX;
    }
    
    protected static long getVersionFromName(final String fileName) {
        final Pattern pattern = Pattern.compile(FILE_NAME_PATTERN);
        final Matcher matcher = pattern.matcher(fileName);
        if (matcher.matches()) {
            return Long.valueOf(matcher.group(1));
        } else {
            throw new IllegalArgumentException("File name doesn't match accepted format");
        }
    }

    protected static boolean checkFileName(final String fileName) {
        final Pattern pattern = Pattern.compile(FILE_NAME_PATTERN);
        final Matcher matcher = pattern.matcher(fileName);
        return matcher.matches();
    }

    /** 
     * Writes SSTable in file.
     *
     * <p>Each cell is sequentially written in the following format:
     * - keySize (8 bytes)
     * - key ("keySize" bytes)
     * - timestamp (8 bytes)
     * - tombstone (1 byte)</p>
     *
     * <p>If cell has value:
     * - valueSize (8 bytes)
     * - value ("valueSize" bytes)</p>
     *
     * <p>This is followed by offsets:
     * - offset (cellCount * 8 bytes)</p>
     *
     * <p>At the end of file is cell count:
     * - cellCount (4 bytes)</p>
     *
     * @param tablesDir directory to write table
     * @param cellIterator iterator over cells, that you want to flush
     * @param version version of table
     * @return path to the file in which the cells were written
     * @throws IOException if unable to open file
     */
    static Path writeTable(
            final Path tablesDir,
            final Iterator<Cell> cellIterator,
            final long version) throws IOException {

        final Path tmpFile = tablesDir.resolve(TABLE_FILE_PREFIX + version + TABLE_TMP_FILE_SUFFIX);

        try (FileChannel channel = FileChannel.open(tmpFile,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {

            final List<Integer> offsetList = new ArrayList<>();

            while (cellIterator.hasNext()) {

                offsetList.add((int) channel.position());

                final Cell cell = cellIterator.next();

                final long keySize = cell.getKey().limit();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(keySize).flip());
                channel.write(ByteBuffer.allocate((int) keySize).put(cell.getKey()).flip());

                final long timeStamp = cell.getValue().getTimeStamp();
                channel.write(ByteBuffer.allocate(Long.BYTES).putLong(timeStamp).flip());

                final boolean tombstone = cell.getValue().isRemoved();
                channel.write(ByteBuffer.allocate(Byte.BYTES).put((byte) (tombstone ? 1 : 0)).flip());

                if (!tombstone) {
                    final ByteBuffer value = cell.getValue().getData();
                    final long valueSize = value.limit();
                    channel.write(ByteBuffer.allocate(Long.BYTES).putLong(valueSize).flip());
                    channel.write(ByteBuffer.allocate((int) valueSize).put(value).flip());
                }
            }

            final ByteBuffer offsetByteBuffer = ByteBuffer.allocate(Long.BYTES * offsetList.size());

            for (final int offset : offsetList) {
                offsetByteBuffer.putLong(offset);
            }

            channel.write(offsetByteBuffer.flip());

            channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(offsetList.size()).flip());
        }

        final Path newTableFile = tablesDir
                .resolve(tmpFile.toString().replace(TABLE_TMP_FILE_SUFFIX, TABLE_FILE_SUFFIX));
        Files.move(tmpFile, newTableFile, StandardCopyOption.ATOMIC_MOVE);

        return newTableFile;
    }

    public abstract Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    public abstract void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    public abstract void remove(@NotNull ByteBuffer key);

    public abstract long getSize();

    public abstract long getVersion();

    protected abstract ByteBuffer parseKey(final int index) throws IOException;

    protected abstract Cell parseCell(final int index) throws IOException;

    /**
     * Flushes in-memory table to file.
     *
     * @param tablesDir directory to flush
     * @param cellIterator iterator over cell
     * @param version version of table
     * @param impl type of implementation of SSTable abstraction
     * @return SSTable abstraction
     * @throws IOException if unable to open file
     */
    public static SSTable flush(
            final Path tablesDir, 
            final Iterator<Cell> cellIterator, 
            final long version,
            final Implementation impl) throws IOException {

        if (impl == Implementation.FILE_CHANNEL_READ) {
            return new SSTableFileChannel(writeTable(tablesDir, cellIterator, version));
        } else {
            return new SSTableMmap(writeTable(tablesDir, cellIterator, version));
        }
    }

}
