package ru.mail.polis.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FlusherThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(FlusherThread.class);

    private final Flushable dao;
    private final MemoryTablePool memTable;

    FlusherThread(final Flushable dao, final MemoryTablePool memTable) {
        super("Flusher");
        this.dao = dao;
        this.memTable = memTable;
    }

    @Override
    public void run() {
        boolean poisonReceived = false;
        while (!poisonReceived && !isInterrupted()) {
            TableToFlush tableToFlush = null;
            try {
                tableToFlush = memTable.takeToFlush();
                poisonReceived = tableToFlush.isPoisonPill();
                dao.flush(tableToFlush.getTable());
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
