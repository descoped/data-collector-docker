package no.ssb.dc.server.service;

import de.huxhorn.sulky.ulid.ULID;

import java.nio.ByteBuffer;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SequenceKey implements Comparable<SequenceKey> {

    private final ULID.Value ulid;
    private final String position;

    public SequenceKey(ULID.Value ulid, String position) {
        this.ulid = ulid;
        this.position = position;
    }

    public ULID.Value ulid() {
        return ulid;
    }

    public String position() {
        return position;
    }

    public static SequenceKey fromByteBuffer(ByteBuffer keyBuffer) {
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

    public ByteBuffer toByteBuffer(ByteBuffer allocatedBuffer) {
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
