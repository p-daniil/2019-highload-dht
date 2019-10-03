package ru.mail.polis.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public final class DebugUtils {

    private static final Logger log = LoggerFactory.getLogger(DebugUtils.class);
    private static final int MAX_BYTES_TO_SHOW = 30;

    private DebugUtils() {
    }

    /** 
     * Print in console bytes of two ByteBuffers to compare.
     *
     * @param my bytebuffer to check
     * @param reference reference bytebuffer
     * @param tag info about comparison
     */
    public static void compareBytes(final ByteBuffer my, final ByteBuffer reference, final String tag) {
        log.info(String.format("%n~~~ %s%n", tag));

        final int myShowLimit = my.limit() < MAX_BYTES_TO_SHOW ? my.limit() : MAX_BYTES_TO_SHOW;

        for (int i = 0; i < myShowLimit; i++) {
            log.info(String.format("%s%s%n", my.get(i), i == myShowLimit - 1 ? "" : ", "));
        }

        final int refShowLimit = reference.limit() < MAX_BYTES_TO_SHOW ? reference.limit() : MAX_BYTES_TO_SHOW;

        for (int i = 0; i < refShowLimit; i++) {
            log.info(String.format("%s%s%n%n", reference.get(i), i == refShowLimit - 1 ? "" : ", "));
        }
    }

    /** 
     * Show in console info about cell.
     * - is cell removed;
     * - timestamp
     *
     * @param cell cell to get info about
     */
    public static void cellInfo(final Cell cell) {
        log.info(String.format("%nTombstone: %s%nTimestamp: %s%n%n",
                cell.getValue().isRemoved(),
                cell.getValue().getTimeStamp()));
    }

    /** 
     * Show in console info about flushing.
     * - Current table size;
     * - Heap free
     *
     * @param table table to flush
     */
    public static void flushInfo(final Table table) {
        log.info(String.format("%n==Flushing==%nCurrent table size: %d bytes%nHeap free: %d bytes%n%n",
                table.getSize(),
                Runtime.getRuntime().freeMemory()));
    }

}
