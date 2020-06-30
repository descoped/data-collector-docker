package no.ssb.dc.server.service;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.dc.api.handler.Tuple;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiConsumer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class IntegrityCheckIndex implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IntegrityCheckIndex.class);

    static int BUFFER_POOL_SIZE = 500;
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
//                LOG.trace("Commit buffer: {}", buffer);
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

    void writeSequence(ULID.Value ulid, String position) {
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

    void readSequence(BiConsumer<SequenceKey, Boolean> visit) {
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

    static class SequenceKey implements Comparable<SequenceKey> {
        final ULID.Value ulid;
        final String position;

        SequenceKey(ULID.Value ulid, String position) {
            this.ulid = ulid;
            this.position = position;
        }

        static SequenceKey fromByteBuffer(ByteBuffer keyBuffer) {
            Objects.requireNonNull(keyBuffer);
            int positionLength = keyBuffer.get();
            byte[] positionBytes = new byte[positionLength];
            keyBuffer.get(positionBytes);
            long ulidMostSignificantBits = keyBuffer.getLong();
            long ulidLeastSignificantBits = keyBuffer.getLong();
            String position = new String(positionBytes, UTF_8);
            ULID.Value ulid = new ULID.Value(ulidMostSignificantBits, ulidLeastSignificantBits);
            return new SequenceKey(ulid, position);
        }

        ByteBuffer toByteBuffer(ByteBuffer allocatedBuffer) {
            Objects.requireNonNull(allocatedBuffer);
            byte[] key = position.getBytes(UTF_8);
            allocatedBuffer.put((byte) key.length);
            allocatedBuffer.put(key);
            allocatedBuffer.putLong(ulid.getMostSignificantBits());
            allocatedBuffer.putLong(ulid.getLeastSignificantBits());
            return allocatedBuffer.flip();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SequenceKey that = (SequenceKey) o;
            return Objects.equals(ulid, that.ulid) &&
                    Objects.equals(position, that.position);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ulid, position);
        }

        @Override
        public int compareTo(SequenceKey that) {
            return this.ulid.compareTo(that.ulid);
        }

        @Override
        public String toString() {
            return "SequenceKey{" +
                    "ulid=" + ulid +
                    ", position='" + position + '\'' +
                    '}';
        }
    }

    static class Writer {

    }

}
