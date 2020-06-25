package no.ssb.dc.server.service;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.dc.api.ulid.ULIDGenerator;
import no.ssb.dc.api.ulid.ULIDStateHolder;
import no.ssb.dc.api.util.CommonUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static no.ssb.dc.server.service.LmdbEnvironment.removePath;

/**
 * Test requires vm arg: --add-opens java.base/java.nio=lmdbjava --add-exports=java.base/sun.nio.ch=lmdbjava
 */
public class LmdbEnvironmentTest {

    private static final Logger LOG = LoggerFactory.getLogger(LmdbEnvironmentTest.class);

    static ULIDStateHolder stateHolder = new ULIDStateHolder();

    @Test
    void testSequence() throws IOException {
        Path dbPath = CommonUtils.currentPath().resolve("target").resolve("lmdb");
        removePath(dbPath);

        try (LmdbEnvironment environment = new LmdbEnvironment(null, dbPath, "test-stream")) {
            try (IntegrityCheckIndex index = new IntegrityCheckIndex(environment, 50)) {

                for (int n = 1; n < 100; n++) {
                    {
                        ULID.Value ulid = ULIDGenerator.nextMonotonicUlid(stateHolder);
                        index.writeSequence(ulid, String.valueOf(n));
                    }

                    if (n == 11 || n == 88) {
                        ULID.Value ulid = ULIDGenerator.nextMonotonicUlid(stateHolder);
                        index.writeSequence(ulid, String.valueOf(n));
                    }

                    if (n == 88) {
                        ULID.Value ulid = ULIDGenerator.nextMonotonicUlid(stateHolder);
                        index.writeSequence(ulid, String.valueOf(n));
                    }
                }
                index.commit();

                AtomicReference<IntegrityCheckIndex.SequenceKey> prevSequenceKey = new AtomicReference<>();
                Map<String, Set<IntegrityCheckIndex.SequenceKey>> duplicateMap = new LinkedHashMap<>();
                index.readSequence(sequenceKey -> {
                    if (prevSequenceKey.get() == null) {
                        prevSequenceKey.set(sequenceKey);
                        return;
                    }
                    if (prevSequenceKey.get().position.equals(sequenceKey.position)) {
                        duplicateMap.computeIfAbsent(prevSequenceKey.get().position, duplicateList -> new TreeSet<>()).add(prevSequenceKey.get());
                        duplicateMap.get(sequenceKey.position).add(sequenceKey);
                    }
                    prevSequenceKey.set(sequenceKey);
                });

                duplicateMap.forEach((sequenceKey, duplicateList) ->
                        LOG.trace("{}/{}", sequenceKey, duplicateList.stream().map(key ->
                                Long.toString(key.ulid.timestamp() + key.ulid.getLeastSignificantBits())).collect(Collectors.joining(",")))
                );

                index.readSequence(sequenceKey -> {
                    LOG.trace("{}/{}", sequenceKey.position, ULIDGenerator.toUUID(sequenceKey.ulid));
                });
            }
        }
    }

}
