package io.descoped.dc.server.db;

import de.huxhorn.sulky.ulid.ULID;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class PrintUlidToDateTimeTest {

    private static final Logger LOG = LoggerFactory.getLogger(PrintUlidToDateTimeTest.class);

    static Date toDateTime(ULID.Value ulid) {
        return new Date(ulid.timestamp());
    }

    @Disabled
    @Test
    void convertTimestamps() {
        String[] ulidStrings = {"01726559-367d-0000-0000-000000000001", "01726566-4dd4-0000-0000-000000000001", "01726574-0a1f-0000-0000-000000000001"};
        for (String ulidString : ulidStrings) {
            UUID uuid = UUID.fromString(ulidString);
            ULID.Value ulid = new ULID.Value(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            LOG.info("{}", toDateTime(ulid));
        }
    }

}
