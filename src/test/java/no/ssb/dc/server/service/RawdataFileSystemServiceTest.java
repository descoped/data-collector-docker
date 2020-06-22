package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.server.component.ContentStoreComponent;
import no.ssb.dc.test.client.TestClient;
import no.ssb.dc.test.server.TestServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class RawdataFileSystemServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(RawdataFileSystemServiceTest.class);

    static int EXPECTED_MESSAGES = 100;

    static DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
            .values("content.stream.connector", "rawdata")
            .values("rawdata.client.provider", "memory")
            .values("data.collector.rawdata.dump.enabled", "true")
            .values("data.collector.rawdata.dump.location", CommonUtils.currentPath().resolve("target").resolve("storage").toFile().getAbsolutePath())
            .values("data.collector.rawdata.dump.topic", "test-stream")
            .values("data.collector.consumer.timeoutInSeconds", "1")
            .build();
    static TestServer server;
    static TestClient client;

    @BeforeAll
    static void beforeAll() {
        server = TestServer.create(configuration);
        client = TestClient.create(server);
        server.start();
    }

    @AfterAll
    static void afterAll() {
        server.stop();
    }

    static void produceMessages(ContentStoreComponent contentStoreComponent) {
        ContentStore contentStore = contentStoreComponent.getDelegate();
        try (ContentStreamProducer producer = contentStore.contentStream().producer(configuration.evaluateToString("data.collector.rawdata.dump.topic"))) {
            for (int n = 0; n < EXPECTED_MESSAGES; n++) {
                producer.publishBuilders(producer.builder().position(String.valueOf(n)).put("entry", "DATA".getBytes(StandardCharsets.UTF_8)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Disabled
    @Test
    void exportData() throws IOException {
        ContentStoreComponent contentStoreComponent = server.getApplication().unwrap(ContentStoreComponent.class);
        CompletableFuture<Void> producerFuture = CompletableFuture.runAsync(() -> produceMessages(contentStoreComponent));

        RawdataFileSystemService service = server.getApplication().unwrap(RawdataFileSystemService.class);

        CompletableFuture<Void> consumerFuture = CompletableFuture.runAsync(() -> {
            while (true) {
                long count = countWorkDirFolders(service.getWorkDir());
                if (count == EXPECTED_MESSAGES) {
                    break;
                }
                nap();
            }
        });

        CompletableFuture.allOf(producerFuture, consumerFuture).join();
    }

    private long countWorkDirFolders(Path workDir) {
        try {
            return Files.list(workDir).filter(p -> p.toFile().isDirectory()).count();
        } catch (NoSuchFileException e) {
            return 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void nap() {
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
