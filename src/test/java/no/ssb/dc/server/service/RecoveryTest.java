package no.ssb.dc.server.service;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.ulid.ULIDGenerator;
import no.ssb.dc.api.util.CommonUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecoveryTest {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryTest.class);

    @Test
    void readIndex() {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("content.stream.connector", "rawdata")
                .values("rawdata.client.provider", "memory")
                .values("data.collector.integrityCheck.consumer.timeoutInSeconds", "1")
                .values("data.collector.integrityCheck.dbSizeInMb", "100")
                .build();

        Path dbLocation = CommonUtils.currentPath().resolve("certs").resolve("lmdb");
        LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(configuration, dbLocation, "ske-freg");

        long past = System.currentTimeMillis();
        SequenceDbHelper sequenceDbHelper = new SequenceDbHelper(lmdbEnvironment, lmdbEnvironment.open());
        PositionAndULIDVersion firstPosition = sequenceDbHelper.findFirstPosition();
        PositionAndULIDVersion lastPosition = sequenceDbHelper.findLastPosition();
        long now = System.currentTimeMillis() - past;
        LOG.trace("Time take: {}", now);
        LOG.trace("First: {} -> {} -> {}",
                firstPosition.position(),
                ULIDGenerator.toUUID(firstPosition.ulid()).toString(),
                new Date(firstPosition.ulid().timestamp())
        );
        LOG.trace("Last: {} -> {} -> {}",
                lastPosition.position(),
                ULIDGenerator.toUUID(lastPosition.ulid()).toString(),
                new Date(lastPosition.ulid().timestamp())
        );
    }

    @Test
    void thatLeastVersionIsSelected() {
        ULID.Value v1 = ULIDGenerator.generate();
        ULID.Value v2 = ULIDGenerator.generate();
        ULID.Value v3 = ULIDGenerator.generate();

        PositionAndULIDVersion version = new PositionAndULIDVersion();

        version.compareAndSet(v2, "1");
        assertEquals(v2, version.ulid());

        version.compareAndSet(v3, "1");
        assertEquals(v2, version.ulid());

        version.compareAndSet(v1, "1");
        assertEquals(v1, version.ulid());

        version.compareAndSet(v3, "1");
        assertEquals(v1, version.ulid());
    }


}
