package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.Record;
import ru.mail.polis.TestBase;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency tests for {@link DAO}.
 */
class ConcurrentTest extends TestBase {

    @Test
    void singleWriter(@TempDir File data) throws IOException, InterruptedException {
        concurrentWrites(1, 1000, 1, data);
    }

    @Test
    void twoWriters(@TempDir File data) throws IOException, InterruptedException {
        concurrentWrites(2, 1000, 1, data);
    }

    @Test
    void twoWritersManyRecords(@TempDir File data) throws IOException, InterruptedException {
        concurrentWrites(2, 1_000_000, 1000, data);
    }

    @Test
    void tenWritersManyRecords(@TempDir File data) throws IOException, InterruptedException {
        concurrentWrites(10, 1_000_000, 1000, data);
    }

    @Test
    void singleReaderWriter(@TempDir File data) throws IOException, InterruptedException {
        concurrentReadWrite(1, 1_000, data);
    }

    @Test
    void twoReaderWriter(@TempDir File data) throws IOException, InterruptedException {
        concurrentReadWrite(2, 1_000, data);
    }

    @Test
    void tenReaderWriterManyRecords(@TempDir File data) throws IOException, InterruptedException {
        concurrentReadWrite(10, 1_000_000, data);
    }

    @Test
    void backgroundCompaction(@TempDir File data) throws IOException, InterruptedException {
        concurrentWritesWithCheckStorage(10, 30, data);
    }

    private void concurrentWrites(int threadsCount,
                                  int recordsCount, int samplePeriod,
                                  @NotNull final File data)
            throws IOException, InterruptedException {
        final RecordsGenerator records = new RecordsGenerator(recordsCount, samplePeriod);
        try (final DAO dao = DAOFactory.create(data)) {
            final ExecutorService executor = new ThreadPoolExecutor(threadsCount, threadsCount,
                    1, TimeUnit.MINUTES,
                    new ArrayBlockingQueue<>(1024),
                    (r, usedExecutor) -> {
                        try {
                            // test thread will be blocked and wait
                            usedExecutor.getQueue().put(r);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
            while (records.hasNext()) {
                final Record record = records.next();
                executor.submit(() -> {
                    try {
                        dao.upsert(record.getKey(), record.getValue());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }

        // Recreate dao and check the contents with sampling (LSM is slow for reads)
        try (final DAO dao = DAOFactory.create(data)) {
            for (final Map.Entry<Integer, Byte> sample : records.getSamples().entrySet()) {
                final ByteBuffer key = ByteBuffer.allocate(Integer.BYTES);
                key.putInt(sample.getKey());
                key.rewind();

                final ByteBuffer value = ByteBuffer.allocate(Byte.BYTES);
                value.put(sample.getValue());
                value.rewind();

                assertEquals(value, dao.get(key));
            }
        }
    }

    private void concurrentReadWrite(int threadsCount,
                                     int recordsCount,
                                     @NotNull final File data)
            throws IOException, InterruptedException {
        final RecordsGenerator records = new RecordsGenerator(recordsCount, 0);
        try (final DAO dao = DAOFactory.create(data)) {
            final ExecutorService executor = new ThreadPoolExecutor(threadsCount, threadsCount,
                    1, TimeUnit.MINUTES,
                    new ArrayBlockingQueue<>(1024),
                    (r, usedExecutor) -> {
                        try {
                            // test thread will be blocked and wait
                            usedExecutor.getQueue().put(r);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
            final AtomicInteger matches = new AtomicInteger();
            while (records.hasNext()) {
                final Record record = records.next();
                executor.submit(() -> {
                    try {
                        dao.upsert(record.getKey(), record.getValue());
                        ByteBuffer value = dao.get(record.getKey());
                        if (value.equals(record.getValue().duplicate().rewind())) {
                            matches.incrementAndGet();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
            assertEquals(recordsCount, matches.get());
        }
    }

    private void concurrentWritesWithCheckStorage(int threadsCount,
                                                  int maxCompactionsExpected,
                                                  @NotNull final File data)
            throws IOException, InterruptedException {
        final long flushThreshold = (long) (DAOFactory.MAX_HEAP * DAOFactory.OPTIONS.getLoadFactor());
        final int compactionThreshold = DAOFactory.OPTIONS.getCompactionThreshold();
        final List<Integer> ssTablesCountSamples = new ArrayList<>();
        final ScheduledExecutorService storageChecker;

        try (final DAO dao = DAOFactory.create(data)) {
            storageChecker = Executors.newScheduledThreadPool(1);
            storageChecker.scheduleAtFixedRate(() -> ssTablesCountSamples.add(data.list().length),
                    0, 10, TimeUnit.MILLISECONDS);

            final ExecutorService executor = new ThreadPoolExecutor(threadsCount, threadsCount,
                    1, TimeUnit.MINUTES,
                    new ArrayBlockingQueue<>(1024),
                    (r, usedExecutor) -> {
                        try {
                            // test thread will be blocked and wait
                            usedExecutor.getQueue().put(r);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });

            final int keyLength = 16;
            final int valueLength = 1024;
            long recordSize = keyLength + valueLength;

            // Estimated records count to achieve max compactions count
            // (make one more flush, to exceed compaction threshold)
            final long recordsCount =
                    (flushThreshold * compactionThreshold * maxCompactionsExpected + flushThreshold) / recordSize;

            for (int i = 0; i < recordsCount; i++) {
                executor.submit(() -> {
                    final ByteBuffer key = randomBuffer(keyLength);
                    final ByteBuffer value = randomBuffer(valueLength);
                    try {
                        dao.upsert(key, value);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        // Shutdown storage checker after closing DAO
        storageChecker.shutdown();
        // Control sample
        ssTablesCountSamples.add(data.list().length);

        int filesCountDecreasedTimes = 0;
        boolean decreasedBefore = false;
        for (int i = 1; i < ssTablesCountSamples.size(); i++) {
            if (ssTablesCountSamples.get(i) < ssTablesCountSamples.get(i - 1)) {
                if (!decreasedBefore) {
                    filesCountDecreasedTimes++;
                    decreasedBefore = true;
                }
            } else {
                decreasedBefore = false;
            }
        }
        assertTrue(filesCountDecreasedTimes > 0);
    }
}
