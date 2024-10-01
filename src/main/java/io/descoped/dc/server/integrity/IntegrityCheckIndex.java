package io.descoped.dc.server.integrity;

import de.huxhorn.sulky.ulid.ULID;
import io.descoped.dc.api.handler.Tuple;
import io.descoped.dc.server.db.DirectByteBufferPool;
import io.descoped.dc.server.db.LmdbEnvironment;
import io.descoped.dc.server.db.SequenceKey;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiConsumer;

public class IntegrityCheckIndex implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IntegrityCheckIndex.class);

    static int BUFFER_POOL_SIZE = 1001;
    static int CONTENT_LENGTH = 0;

    private final LmdbEnvironment lmdbEnvironment;
    private final DirectByteBufferPool keyBufferPool;
    private final DirectByteBufferPool contentBufferPool;
    private final Dbi<ByteBuffer> sequenceDb;
    private final Queue<Tuple<ByteBuffer, ByteBuffer>> bufferQueue;

    public IntegrityCheckIndex(LmdbEnvironment lmdbEnvironment) {
        this.lmdbEnvironment = lmdbEnvironment;
        this.keyBufferPool = new DirectByteBufferPool(BUFFER_POOL_SIZE, lmdbEnvironment.maxKeySize());
        this.contentBufferPool = new DirectByteBufferPool(BUFFER_POOL_SIZE, CONTENT_LENGTH);
        this.bufferQueue = new LinkedBlockingDeque<>(BUFFER_POOL_SIZE - 1);
        this.sequenceDb = lmdbEnvironment.open();
    }

    public Path getDatabaseDir() {
        return lmdbEnvironment.getDatabaseDir();
    }

    public void commitQueue() {
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env().txnWrite()) {
            Tuple<ByteBuffer, ByteBuffer> buffer;
            while ((buffer = bufferQueue.poll()) != null) {
                try {
                    sequenceDb.put(txn, buffer.getKey(), buffer.getValue());
                } finally {
                    keyBufferPool.release(buffer.getKey());
                    contentBufferPool.release(buffer.getValue());
                }
            }
            txn.commit();
        }
    }

    public void writeSequence(ULID.Value ulid, String position) {
        ByteBuffer keyBuffer = keyBufferPool.acquire();
        SequenceKey sequenceKey = new SequenceKey(ulid, position);
        sequenceKey.toByteBuffer(keyBuffer);
        ByteBuffer contentBuffer = contentBufferPool.acquire();
        contentBuffer.flip();
        if (!bufferQueue.offer(new Tuple<>(keyBuffer, contentBuffer))) {
            commitQueue();
            // try to re-offer buffer after queue reach max capacity
            if (!bufferQueue.offer(new Tuple<>(keyBuffer, contentBuffer))) {
                throw new IllegalStateException("Buffers was not added to bufferQueue!");
            }
        }
    }

    public void readSequence(BiConsumer<SequenceKey, Boolean> visit) {
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env().txnRead()) {
            Iterator<CursorIterable.KeyVal<ByteBuffer>> it = sequenceDb.iterate(txn).iterator();
            while (it.hasNext()) {
                CursorIterable.KeyVal<ByteBuffer> next = it.next();
                ByteBuffer keyBuffer = next.key();
                SequenceKey sequenceKey = SequenceKey.fromByteBuffer(keyBuffer);
                visit.accept(sequenceKey, it.hasNext());
            }
        }
    }

    @Override
    public void close() {
        commitQueue();
        keyBufferPool.close();
        contentBufferPool.close();
    }

}
