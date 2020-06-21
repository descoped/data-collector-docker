package no.ssb.dc.server;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStream;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.server.component.ContentStoreComponent;
import no.ssb.dc.server.controller.IntegrityCheckJob;
import no.ssb.dc.server.controller.IntegrityCheckJobSummary;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class IntegrityCheckTest {

    private static final Logger LOG = LoggerFactory.getLogger(IntegrityCheckJob.class);

    static void produceMessages(ContentStream contentStream) {
        try (ContentStreamProducer producer = contentStream.producer("test-stream")) {
            for (int n = 0; n < 55; n++) {
                producer.publishBuilders(producer.builder().position(String.valueOf(n)).put("entry", "DATA".getBytes(StandardCharsets.UTF_8)));

                if (n == 10 || n == 25 || n == 50) {
                    producer.publishBuilders(producer.builder().position(String.valueOf(n)).put("entry", "DATA".getBytes(StandardCharsets.UTF_8)));
                }

                if (n == 25 || n == 50) {
                    producer.publishBuilders(producer.builder().position(String.valueOf(n)).put("entry", "DATA".getBytes(StandardCharsets.UTF_8)));
                }

                if (n == 50) {
                    producer.publishBuilders(producer.builder().position(String.valueOf(n)).put("entry", "DATA".getBytes(StandardCharsets.UTF_8)));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * IntegrityCheckJob -> scan rawdata and build IntegrityCheckStatus and add to IntegrityCheckMonitor
     * IntegrityCheckMonitor -> array of IntegrityCheckJobSummary
     * IntegrityCheckController -> render IntegrityCheckMonitor
     * IntegrityCheckController -> run og cancel job on topic (unique)
     */

    @Test
    void testIntegrityChecker() throws Exception {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("content.stream.connector", "rawdata")
                .values("rawdata.client.provider", "memory")
                .build();

        ContentStoreComponent contentStoreComponent = ContentStoreComponent.create(configuration);
        ContentStore contentStore = contentStoreComponent.getDelegate();

        Thread producerThread = new Thread(() -> produceMessages(contentStore.contentStream()));
        producerThread.start();

        IntegrityCheckJobSummary summary = new IntegrityCheckJobSummary();
        IntegrityCheckJob job = new IntegrityCheckJob(configuration, contentStoreComponent, summary);
        job.consume("test-stream");

        producerThread.join();
        contentStoreComponent.close();

        String json = JsonParser.createJsonParser().toPrettyJSON(summary.build());
        LOG.trace("summary: {}", json);
    }

}
