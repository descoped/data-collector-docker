package no.ssb.dc.server.service;

import de.huxhorn.sulky.ulid.ULID;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class IntegrityCheckIndex {

    static short FIXED_POSITION_LENGTH = 32;
    static int SEQUENCE_KEY_LENGTH = FIXED_POSITION_LENGTH + 8 + 8;
    static int CONTENT_LENGTH = 256;

    private final LmdbEnvironment lmdbEnvironment;
    private final DirectByteBufferPool keyBufferPool;
    private final DirectByteBufferPool contentBufferPool;
    private final Dbi<ByteBuffer> sequenceDb;

    public IntegrityCheckIndex(LmdbEnvironment lmdbEnvironment) {
        this.lmdbEnvironment = lmdbEnvironment;
        this.keyBufferPool = new DirectByteBufferPool(25, SEQUENCE_KEY_LENGTH);
        this.contentBufferPool = new DirectByteBufferPool(25, CONTENT_LENGTH);
        this.sequenceDb = lmdbEnvironment.open();
    }

    void writeSequence(ULID.Value ulid, String position) {
        ByteBuffer keyBuffer = keyBufferPool.acquire();
        try {
            SequenceKey sequenceKey = new SequenceKey(ulid, position);
            sequenceKey.toByteBuffer(keyBuffer);
            ByteBuffer contentBuffer = contentBufferPool.acquire();
            try {
                contentBuffer.flip();
                try (Txn<ByteBuffer> txn = lmdbEnvironment.env.txnWrite()) {
                    sequenceDb.put(txn, keyBuffer, contentBuffer);
                    txn.commit();
                }
            } finally {
                contentBufferPool.release(contentBuffer);
            }
        } finally {
            keyBufferPool.release(keyBuffer);
        }
    }

    void readSequence(Consumer<SequenceKey> visit) {
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env.txnRead()) {
            Iterator<CursorIterable.KeyVal<ByteBuffer>> it = sequenceDb.iterate(txn).iterator();
            while (it.hasNext()) {
                CursorIterable.KeyVal<ByteBuffer> next = it.next();
                ByteBuffer keyBuffer = next.key();
                SequenceKey sequenceKey = SequenceKey.fromByteBuffer(keyBuffer);
                visit.accept(sequenceKey);
            }
        }

    }

    static class SequenceKey implements Comparable<SequenceKey>  {
        final ULID.Value ulid;
        final String position;

        SequenceKey(ULID.Value ulid, String position) {
            this.ulid = ulid;
            this.position = position;
        }

        static byte[] trim(byte[] bytes) {
            int i = 0;
            while (i < bytes.length && bytes[i] == 0) {
                i++;
            }
            return Arrays.copyOfRange(bytes, i, bytes.length);
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

        static SequenceKey fromByteBuffer(ByteBuffer keyBuffer) {
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
            byte[] keySrc = position.getBytes(UTF_8);
            allocatedBuffer.put((byte)keySrc.length);
            allocatedBuffer.put(keySrc);
            allocatedBuffer.putLong(ulid.getMostSignificantBits());
            allocatedBuffer.putLong(ulid.getLeastSignificantBits());

            return allocatedBuffer.flip();
        }
    }

}
