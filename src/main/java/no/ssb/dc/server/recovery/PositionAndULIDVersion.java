package no.ssb.dc.server.recovery;

import de.huxhorn.sulky.ulid.ULID;

import java.util.Objects;

/**
 * Maintain the state of the oldest version of an ulid
 */
public class PositionAndULIDVersion {

    private String position;
    private ULID.Value ulid;

    public PositionAndULIDVersion() {
    }

    public String position() {
        return position;
    }

    public ULID.Value ulid() {
        return ulid;
    }

    public boolean isEmpty() {
        return this.ulid == null && this.position == null;
    }

    public boolean isOlder(ULID.Value ulid) {
        if (isEmpty()) {
            return true; // ensure we accept
        }
        return this.ulid.compareTo(ulid) == 1;
    }

    public boolean compareAndSet(ULID.Value ulid, String position) {
        if (isOlder(ulid)) {
            this.ulid = ulid;
            this.position = position;
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PositionAndULIDVersion that = (PositionAndULIDVersion) o;
        return Objects.equals(ulid, that.ulid) &&
                Objects.equals(position, that.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ulid, position);
    }

    @Override
    public String toString() {
        return "ULIDVersion{" +
                "ulid=" + ulid +
                ", position='" + position + '\'' +
                '}';
    }
}
