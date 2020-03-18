package no.ssb.dc.server;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ExportRawdataStreamTest {

    final static Logger LOG = LoggerFactory.getLogger(ExportRawdataStreamTest.class);

    final static DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .values("rawdata.client.provider", "postgres")
            .values("data.collector.worker.threads", "20")
            .values("postgres.driver.host", "localhost")
            .values("postgres.driver.port", "15432")
            .values("postgres.driver.user", "rdc")
            .values("postgres.driver.password", "rdc")
            .values("postgres.driver.database", "rdc")
            .values("rawdata.postgres.consumer.prefetch-size", "100")
            .values("rawdata.postgres.consumer.prefetch-poll-interval-when-empty", "1000")
            .values("local-temp-folder", "target/_tmp_avro_")
            .values("avro-file.max.seconds", "86400")
            .values("avro-file.max.bytes", "67108864")
            .values("avro-file.sync.interval", "524288")
            .values("gcs.bucket-name", "")
            .values("gcs.listing.min-interval-seconds", "30")
            .values("gcs.service-account.key-file", "")
            .environment("DC_")
            .build();

    @Disabled
    @Test
    void export() {
        RawdataClient rawdataClient = ProviderConfigurator.configure(configuration.asMap(), configuration.evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class);
        assertNotNull(rawdataClient);

        try (RawdataConsumer consumer = rawdataClient.consumer("sirius-person-utkast")) {
            RawdataMessage message;
            while ((message = consumer.receive(1, TimeUnit.SECONDS)) != null) {
                Map<String, byte[]> data = message.data();
                byte[] manifest = data.get("manifest.json");
                byte[] entry = data.get("entry");

                LOG.info("{}: {}", message.position(), new String(entry));

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
