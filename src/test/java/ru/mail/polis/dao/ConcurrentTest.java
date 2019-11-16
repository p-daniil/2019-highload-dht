package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.TestBase;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentTest.class);

    private void concurrentWritesWithCheck(int threadsCount,
                                           @NotNull final File data,
                                           long flushThreshold,
                                           int compactionThreshold)
            throws IOException, InterruptedException {
        final ReentrantLock lock = new ReentrantLock();
        final Condition checkStorage = lock.newCondition();
        final List<Integer> ssTablesCountMetrics = new ArrayList<>();
        final int expectedCompactionsCount = 10;
        try (final DAO dao = DAOFactory.create(data)) {
            final ExecutorService storageChecker = Executors.newSingleThreadExecutor();
            storageChecker.execute(() -> {
                while (!storageChecker.isTerminated()) {
                    lock.lock();
                    try {
                        checkStorage.await();
                        ssTablesCountMetrics.add(data.list().length);
                        LOG.info("Check SSTables count: {}", ssTablesCountMetrics.get(ssTablesCountMetrics.size() - 1));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        lock.unlock();
                    }
                }
            });
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
            final int keyLength = 1024;
            final int valueLength = 16 * 1024;

            long recordSize = keyLength + valueLength;
            // records count to achieve flushing
            final long recordsCount = flushThreshold * compactionThreshold * expectedCompactionsCount / recordSize + 1;
            final AtomicLong memTableSize = new AtomicLong(0);
            for (int i = 0; i < recordsCount; i++) {
                executor.submit(() -> {
                    final ByteBuffer key = randomBuffer(keyLength);
                    final ByteBuffer value = randomBuffer(valueLength);
                    try {
                        dao.upsert(key, value);
                        if (memTableSize.addAndGet(recordSize) >= flushThreshold) {
                            lock.lock();
                            try {
                                if (memTableSize.get() >= flushThreshold) {
                                    checkStorage.signal();
                                    LOG.info("Flush threshold achieved");
                                    memTableSize.set(0);
                                }
                            } finally {
                                lock.unlock();
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
            executor.shutdown();
            storageChecker.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
            storageChecker.awaitTermination(1, TimeUnit.MINUTES);
        }
        // Last check after closing DAO
        ssTablesCountMetrics.add(data.list().length);
        int compactionsCount = 0;
        for (int i = 1; i < ssTablesCountMetrics.size(); i++) {
            if (ssTablesCountMetrics.get(i) < ssTablesCountMetrics.get(i - 1)) {
                compactionsCount++;
            }
        }
        assertEquals(expectedCompactionsCount, compactionsCount);
    }

//    @Test
    void backgroundCompaction(@TempDir File data) throws IOException, InterruptedException {
        concurrentWritesWithCheck(10, data, (long) (DAOFactory.MAX_HEAP * 0.1), 3);
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
}
