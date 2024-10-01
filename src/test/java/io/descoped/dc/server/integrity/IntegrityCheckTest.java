package io.descoped.dc.server.integrity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.descoped.config.DynamicConfiguration;
import io.descoped.config.StoreBasedDynamicConfiguration;
import io.descoped.dc.api.content.ContentStore;
import io.descoped.dc.api.content.ContentStream;
import io.descoped.dc.api.content.ContentStreamProducer;
import io.descoped.dc.api.util.CommonUtils;
import io.descoped.dc.api.util.JsonParser;
import io.descoped.dc.server.content.ContentStoreComponent;
import io.descoped.dc.server.db.LmdbEnvironment;
import io.descoped.dc.test.client.ResponseHelper;
import io.descoped.dc.test.client.TestClient;
import io.descoped.dc.test.server.TestServer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Test requires vm arg: --add-opens java.base/java.nio=lmdbjava --add-exports=java.base/sun.nio.ch=lmdbjava
 */
public class IntegrityCheckTest {

    static final int NUMBER_OF_MESSAGES = 25000;
    private static final Logger LOG = LoggerFactory.getLogger(IntegrityCheckTest.class);

    static void produceMessages(ContentStream contentStream, String prefixTopic) {
        try (ContentStreamProducer producer = contentStream.producer(prefixTopic + "test-stream")) {
            for (int n = 0; n < NUMBER_OF_MESSAGES; n++) {
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

    @Disabled
    @Test
    void testIntegrityChecker() throws Exception {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("content.stream.connector", "rawdata")
                .values("rawdata.client.provider", "memory")
                .values("data.collector.integrityCheck.consumer.timeoutInSeconds", "1")
                .values("data.collector.integrityCheck.dbSizeInMb", "100")
                .build();

        ContentStoreComponent contentStoreComponent = ContentStoreComponent.create(configuration);
        ContentStore contentStore = contentStoreComponent.getDelegate();

        Thread producerThread = new Thread(() -> produceMessages(contentStore.contentStream(), ""));
        producerThread.start();
        producerThread.join();

        Path dbPath = CommonUtils.currentPath().resolve("target").resolve("lmdb");
        LmdbEnvironment.removePath(dbPath);
        Map<String, IntegrityCheckJob> jobs = new LinkedHashMap<>();
        CompletableFuture<IntegrityCheckJob> future = CompletableFuture.supplyAsync(() -> {
            try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(configuration, dbPath, "test-stream")) {
                try (IntegrityCheckIndex index = new IntegrityCheckIndex(lmdbEnvironment)) {
                    IntegrityCheckJob job = new IntegrityCheckJob(configuration, contentStoreComponent, index, new IntegrityCheckJobSummary());
                    jobs.put("test-stream", job);
                    LOG.trace("Running jobs: {}", jobs);
                    job.consume("test-stream");
                    return job;
                }
            }
        }).exceptionally(throwable -> {
            LOG.error("Ended exceptionally with error: {}", CommonUtils.captureStackTrace(throwable));
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else if (throwable instanceof Error) {
                throw (Error) throwable;
            } else {
                throw new RuntimeException(throwable);
            }
        });
        IntegrityCheckJob job = future.join();

        IntegrityCheckJobSummary.Summary summary = job.getSummary();
        LOG.trace("LastPos: {} -- CurrPos: {}", summary.lastPosition, summary.currentPosition);

        contentStoreComponent.close();

        String json = JsonParser.createJsonParser().toPrettyJSON(summary);
        LOG.trace("summary: {}", json);
    }

    @Disabled
    @Test
    void testIntegrityCheckerController() throws InterruptedException {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .values("content.stream.connector", "rawdata")
                .values("rawdata.client.provider", "memory")
                .values("data.collector.integrityCheck.consumer.timeoutInSeconds", "1")
                .values("data.collector.integrityCheck.database.location", "./target")
                .build();

        TestServer server = TestServer.create(configuration);
        TestClient client = TestClient.create(server);
        server.start();

        ContentStoreComponent contentStoreComponent = server.getApplication().unwrap(ContentStoreComponent.class);
        ContentStore contentStore = contentStoreComponent.getDelegate();

        Thread producerThread = new Thread(() -> produceMessages(contentStore.contentStream(), "2020-"));
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
            while (true) {
                ResponseHelper<String> response = client.get("/check-integrity/2020-test-stream");
                if (!response.body().isEmpty()) {
                    JsonNode statusNode = JsonParser.createJsonParser().fromJson(response.body(), JsonNode.class).get("status");
                    if ("CLOSED".equals(statusNode.asText())) {
                        break;
                    }
                }
                Thread.sleep(250);
            }
        }

        LOG.trace("Completed integrity check !!");

        {
            ResponseHelper<String> response = client.get("/check-integrity/2020-test-stream/full");
            LOG.trace("job-full-summary:\n{}", response.expect200Ok().body());
        }

        {
            client.put("/check-integrity/2020-test-stream").expect201Created();
            ResponseHelper<String> response = client.delete("/check-integrity/2020-test-stream");
            LOG.trace("cancel-job: {}", response.expectAnyOf(200).response().statusCode());
        }

        Thread.sleep(250);

        server.stop();
    }

    @Disabled
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
