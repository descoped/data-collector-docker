package no.ssb.dc.server.service;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.dc.api.util.CommonUtils;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public class SequenceDbHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceDbHelper.class);

    private final LmdbEnvironment lmdbEnvironment;
    private final Dbi<ByteBuffer> dbi;

    public SequenceDbHelper(LmdbEnvironment lmdbEnvironment, Dbi<ByteBuffer> dbi) {
        this.lmdbEnvironment = lmdbEnvironment;
        this.dbi = dbi;
    }

    public PositionAndULIDVersion findFirstPosition() {
        final PositionAndULIDVersion positionAndUlidVersion = new PositionAndULIDVersion();
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env().txnRead()) {
            Iterator<CursorIterable.KeyVal<ByteBuffer>> it = dbi.iterate(txn).iterator();
            if (it.hasNext()) {
                CursorIterable.KeyVal<ByteBuffer> next = it.next();
                SequenceKey sequenceKey = SequenceKey.fromByteBuffer(next.key());
                positionAndUlidVersion.compareAndSet(sequenceKey.ulid(), sequenceKey.position());
            }
        }
        return positionAndUlidVersion;
    }

    public PositionAndULIDVersion findLastPosition() {
        final PositionAndULIDVersion positionAndUlidVersion = new PositionAndULIDVersion();
        handlePositionDuplicates(event -> positionAndUlidVersion.compareAndSet(event.ulidSet().iterator().next(), event.sequenceKey().position()));
        return positionAndUlidVersion;
    }

    void handlePositionDuplicates(Predicate<DuplicateEvent> visit) {
        AtomicBoolean canceled = new AtomicBoolean(false);
        AtomicReference<SequenceKey> prevSequenceKey = new AtomicReference<>();
        Map<String, SortedSet<ULID.Value>> duplicatePositionAndUlidSet = new LinkedHashMap<>();
        try (Txn<ByteBuffer> txn = lmdbEnvironment.env().txnRead()) {
            Iterator<CursorIterable.KeyVal<ByteBuffer>> it = dbi.iterate(txn).iterator();
            while (!canceled.get() && it.hasNext()) {
                CursorIterable.KeyVal<ByteBuffer> next = it.next();
                ByteBuffer keyBuffer = next.key();
                SequenceKey sequenceKey = SequenceKey.fromByteBuffer(keyBuffer);

                // mark first position
                if (prevSequenceKey.get() == null) {
                    prevSequenceKey.set(sequenceKey);
                    continue;
                }

                // check if we got a duplicate
                if (prevSequenceKey.get().position().equals(sequenceKey.position())) {
                    // make counters for position and increment duplicateCount
                    duplicatePositionAndUlidSet.computeIfAbsent(prevSequenceKey.get().position(), duplicateUlidSet -> new TreeSet<>()).add(prevSequenceKey.get().ulid());
                    duplicatePositionAndUlidSet.get(sequenceKey.position()).add(sequenceKey.ulid());
                }

                // fire event on duplicates when prev is not equal to curr
                if (duplicatePositionAndUlidSet.size() > 0 && !prevSequenceKey.get().position().equals(sequenceKey.position())) {
                    SortedSet<ULID.Value> ulidSet = duplicatePositionAndUlidSet.get(prevSequenceKey.get().position());
                    try {
                        if (!visit.test(new DuplicateEvent(sequenceKey, ulidSet, it.hasNext()))) {
                            canceled.set(true);
                        }
                    } catch (Exception e) {
                        LOG.error("Duplicate iteration error: {}", CommonUtils.captureStackTrace(e));
                        canceled.set(true);
                    }
                    duplicatePositionAndUlidSet.clear();
                }

                // move marker to next
                prevSequenceKey.set(sequenceKey);
            }
        }
    }

    static class DuplicateEvent {
        private final SequenceKey sequenceKey;
        private final SortedSet<ULID.Value> ulidSet;
        private final boolean hasNext;

        DuplicateEvent(SequenceKey sequenceKey, SortedSet<ULID.Value> ulidSet, boolean hasNext) {
            this.sequenceKey = sequenceKey;
            this.ulidSet = ulidSet;
            this.hasNext = hasNext;
        }

        SequenceKey sequenceKey() {
            return sequenceKey;
        }

        SortedSet<ULID.Value> ulidSet() {
            return ulidSet;
        }

        boolean hasNext() {
            return hasNext;
        }
    }

}
