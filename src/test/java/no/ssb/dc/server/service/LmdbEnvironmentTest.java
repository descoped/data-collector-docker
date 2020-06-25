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

import static no.ssb.dc.server.service.LmdbEnvironment.removeDb;

/**
 * Test requires vm arg: --add-opens java.base/java.nio=lmdbjava --add-exports=java.base/sun.nio.ch=lmdbjava
 */
public class LmdbEnvironmentTest {

    private static final Logger LOG = LoggerFactory.getLogger(LmdbEnvironmentTest.class);

    static ULIDStateHolder stateHolder = new ULIDStateHolder();

    @Test
    void testSequence() throws IOException {
        Path dbPath = CommonUtils.currentPath().resolve("target").resolve("lmdb");
        removeDb(dbPath);

        try (LmdbEnvironment environment = new LmdbEnvironment(dbPath, "test-stream")) {
            IntegrityCheckIndex index = new IntegrityCheckIndex(environment);

            for (int n = 1; n < 1000; n++) {
                {
                    ULID.Value ulid = ULIDGenerator.nextMonotonicUlid(stateHolder);
                    index.writeSequence(ulid, String.valueOf(n));
                }

                if (n == 11 || n == 88) {
                    ULID.Value ulid = ULIDGenerator.nextMonotonicUlid(stateHolder);
                    index.writeSequence(ulid, String.valueOf(n));
                }
            }

            index.readSequence(sequenceKey -> {
                LOG.trace("{}/{}", sequenceKey.position, ULIDGenerator.toUUID(sequenceKey.ulid));
            });
        }
    }

}
