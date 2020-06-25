package no.ssb.dc.server;

import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStream;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.server.component.ContentStoreComponent;
import no.ssb.dc.server.service.IntegrityCheckIndex;
import no.ssb.dc.server.service.IntegrityCheckJob;
import no.ssb.dc.server.service.IntegrityCheckJobSummary;
import no.ssb.dc.server.service.JsonArrayWriter;
import no.ssb.dc.server.service.LmdbEnvironment;
import no.ssb.dc.test.client.ResponseHelper;
import no.ssb.dc.test.client.TestClient;
import no.ssb.dc.test.server.TestServer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class IntegrityCheckTest {

    private static final Logger LOG = LoggerFactory.getLogger(IntegrityCheckJob.class);

    static void produceMessages(ContentStream contentStream) {
        try (ContentStreamProducer producer = contentStream.producer("2020-test-stream")) {
            for (int n = 0; n < 100; n++) {
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

    @Test
    void testIntegrityChecker() throws Exception {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("content.stream.connector", "rawdata")
                .values("rawdata.client.provider", "memory")
                .values("data.collector.consumer.timeoutInSeconds", "1")
                .values("data.collector.integrityCheck.dbSizeInMb", "100")
                .build();

        ContentStoreComponent contentStoreComponent = ContentStoreComponent.create(configuration);
        ContentStore contentStore = contentStoreComponent.getDelegate();

        Thread producerThread = new Thread(() -> produceMessages(contentStore.contentStream()));
        producerThread.start();

        Path dbPath = CommonUtils.currentPath().resolve("target").resolve("lmdb");
        LmdbEnvironment.removePath(dbPath);
        LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(configuration, dbPath, "2020-test-stream");
        IntegrityCheckIndex index = new IntegrityCheckIndex(lmdbEnvironment, 50);
        IntegrityCheckJobSummary summary = new IntegrityCheckJobSummary(index);
        IntegrityCheckJob job = new IntegrityCheckJob(configuration, contentStoreComponent, summary);
        job.consume("2020-test-stream");

        producerThread.join();
        contentStoreComponent.close();

        String json = JsonParser.createJsonParser().toPrettyJSON(summary.build());
        LOG.trace("summary: {}", json);
    }

    @Test
    void testIntegrityCheckerController() throws InterruptedException {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("content.stream.connector", "rawdata")
                .values("rawdata.client.provider", "memory")
                .values("data.collector.consumer.timeoutInSeconds", "1")
                .build();

        TestServer server = TestServer.create(configuration);
        TestClient client = TestClient.create(server);
        server.start();

        ContentStoreComponent contentStoreComponent = server.getApplication().unwrap(ContentStoreComponent.class);
        ContentStore contentStore = contentStoreComponent.getDelegate();

        Thread producerThread = new Thread(() -> produceMessages(contentStore.contentStream()));
        producerThread.start();
        producerThread.join();

        {
            ResponseHelper<String> response = client.put("/check-integrity/2020-test-stream");
            response.expect201Created();
        }

        {
            ResponseHelper<String> response = client.get("/check-integrity");
            LOG.trace("job-list: {}", response.expect200Ok().body());
        }

        {
            ResponseHelper<String> response = client.get("/check-integrity/2020-test-stream");
            LOG.trace("job-summary: {}", response.expect200Ok().body());
        }

        {
            ResponseHelper<String> response = client.delete("/check-integrity/2020-test-stream");
            LOG.trace("cancel-job: {}", response.expectAnyOf(400).body());
        }

        // Add a test that produces enough messages to keep job running, so it can be canceled

        server.stop();
    }

    @Test
    void writeJsonChunk() throws IOException {
        Path jsonExportPath = CommonUtils.currentPath().resolve("target").resolve("lmdb").resolve("export");
        LmdbEnvironment.removePath(jsonExportPath);
        try (JsonArrayWriter writer = new JsonArrayWriter(jsonExportPath, "summary.json", 50)) {
            {
                ObjectNode rootNode = writer.parser().createObjectNode();
                ObjectNode childNode = writer.parser().createObjectNode();
                childNode.put("key", "value");
                rootNode.set("field", childNode);
                LOG.trace("{}", writer.parser().toPrettyJSON(rootNode));
                writer.write(rootNode);
            }
            {
                ObjectNode rootNode = writer.parser().createObjectNode();
                rootNode.put("field2", "value");
                writer.write(rootNode);
            }
        }
    }

}
