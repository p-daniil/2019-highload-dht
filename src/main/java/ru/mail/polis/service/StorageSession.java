package ru.mail.polis.service;

import com.google.common.base.Charsets;
import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class StorageSession extends HttpSession {
    private static final Logger LOG = LoggerFactory.getLogger(StorageSession.class);
    private static final byte[] CRLF = "\r\n".getBytes(Charsets.UTF_8);
    private static final byte LF = '\n';
    private static final byte[] EMPTY_CHUNK = "0\r\n\r\n".getBytes(Charsets.UTF_8);

    private Iterator<Record> records;

    public StorageSession(Socket socket, HttpServer server) {
        super(socket, server);
    }

    void stream(Iterator<Record> records) throws IOException {
        this.records = records;

        final Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);

        next();
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        next();
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private void next() throws IOException {
        while (records.hasNext() && queueHead == null) {
            final Record record = records.next();
            final byte[] key = toByteArray(record.getKey());
            final byte[] value = toByteArray(record.getValue());

            final int payloadLength = key.length + 1 + value.length;
            final String size = Integer.toHexString(payloadLength);

            final int chunkLength = size.length() + 2 + payloadLength + 2;

            final byte[] chunk = new byte[chunkLength];
            final ByteBuffer buffer = ByteBuffer.wrap(chunk);
            buffer.put(size.getBytes());
            buffer.put(CRLF);
            buffer.put(key);
            buffer.put(LF);
            buffer.put(value);
            write(chunk, 0, chunkLength);

            if (!records.hasNext()) {
                write(EMPTY_CHUNK, 0, EMPTY_CHUNK.length);
            }

            server.incRequestsProcessed();

            if ((handling = pipeline.pollFirst()) != null) {
                if (handling == FIN) {
                    scheduleClose();
                } else {
                    try {
                        server.handleRequest(handling, this);
                    } catch (IOException e) {
                        LOG.error("Can't process next request: " + handling, e);
                    }

                }
            }
        }
    }
}
