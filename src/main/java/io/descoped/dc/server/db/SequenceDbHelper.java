package io.descoped.dc.server.db;

import de.huxhorn.sulky.ulid.ULID;
import io.descoped.config.DynamicConfiguration;
import io.descoped.dc.api.util.CommonUtils;
import io.descoped.dc.server.recovery.PositionAndULIDVersion;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class SequenceDbHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceDbHelper.class);

    private final LmdbEnvironment lmdbEnvironment;
    private final Dbi<ByteBuffer> dbi;

    public SequenceDbHelper(LmdbEnvironment lmdbEnvironment, Dbi<ByteBuffer> dbi) {
        this.lmdbEnvironment = lmdbEnvironment;
        this.dbi = dbi;
    }

    public static Path getSequenceDatabaseLocation(DynamicConfiguration configuration) {
        String location = configuration.evaluateToString("data.collector.integrityCheck.database.location");
        return ((Supplier<Path>) () -> {
            if (location == null || location.isEmpty()) {
                // dev directory
                return CommonUtils.currentPath().resolve("lmdb");
            } else if (location.startsWith("./")) {
                // relative path if starts with dot slash
                return CommonUtils.currentPath().resolve(location.substring(2)).resolve("lmdb");
            } else if (location.startsWith(".\\")) {
                // relative path if starts with dot slash
                return CommonUtils.currentPath().resolve(location.substring(2)).resolve("lmdb");
            } else if (location.startsWith(".//")) {
                // relative path if starts with dot slash
                return CommonUtils.currentPath().resolve(location.substring(3)).resolve("lmdb");
            } else if (location.startsWith(".")) {
                // relative path if starts with dot
                return CommonUtils.currentPath().resolve(location.substring(1)).resolve("lmdb");
            } else {
                // absolute path
                return Paths.get(location);
            }
        }).get();
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
                        if (!visit.test(new DuplicateEvent(prevSequenceKey.get(), ulidSet, it.hasNext()))) {
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
