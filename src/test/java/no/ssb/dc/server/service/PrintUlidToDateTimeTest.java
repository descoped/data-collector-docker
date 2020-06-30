package no.ssb.dc.server.service;

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
        String[] ulidStrings = { "0172f3e1-115a-0000-0000-000000000004", "0173007e-f37c-0000-0000-000000000004" };
        for(String ulidString : ulidStrings) {
            UUID uuid = UUID.fromString(ulidString);
            ULID.Value ulid = new ULID.Value(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            LOG.info("{}", toDateTime(ulid));
        }
    }
}
