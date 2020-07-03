package no.ssb.dc.server.content;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStream;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamConsumer;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.application.spi.Service;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RawdataFileSystemService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(RawdataFileSystemService.class);

    private final DynamicConfiguration configuration;
    private final ContentStream contentStream;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private CompletableFuture<Void> consumerFuture;
    private Path workDir;

    public RawdataFileSystemService(DynamicConfiguration configuration, ContentStoreComponent contentStoreComponent) {
        this.configuration = configuration;
        ContentStore contentStore = contentStoreComponent.getDelegate();
        this.contentStream = contentStore.contentStream();
    }

    @Override
    public boolean isEnabled() {
        return configuration.evaluateToBoolean("data.collector.rawdata.dump.enabled");
    }

    @Override
    public void start() {
        if (consumerFuture != null) {
            return;
        }

        if (!configuration.evaluateToBoolean("data.collector.rawdata.dump.enabled")) {
            return;
        }

        if (configuration.evaluateToString("data.collector.rawdata.dump.location") == null) {
            LOG.warn("Unable to start file exporter. No location is defined");
            return;
        }

        String location = configuration.evaluateToString("data.collector.rawdata.dump.location");
        Path targetPath;
        if (location.isEmpty()) {
            targetPath = Paths.get(".").toAbsolutePath().resolve("storage").normalize();
        } else {
            targetPath = Paths.get(location).toAbsolutePath().normalize();
        }

        String topic = configuration.evaluateToString("data.collector.rawdata.dump.topic");

        workDir = targetPath.resolve(topic);
        consumerFuture = createFuture(topic, workDir)
                .exceptionally(throwable -> {
                    LOG.error("Ended exceptionally with error: {}", CommonUtils.captureStackTrace(throwable));
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    } else if (throwable instanceof Error) {
                        throw (Error) throwable;
                    } else {
                        throw new RuntimeException(throwable);
                    }
                });
    }

    @Override
    public void stop() {
        if (!closed.compareAndSet(false, true)) {
            int retryCount = 0;
            while (!consumerFuture.isDone()) {
                if (retryCount > 10) {
                    break;
                }
                nap(100);
                retryCount++;
            }
        }
    }

    CompletableFuture<Void> createFuture(String topic, Path targetPath) {
        TikaConfig config = TikaConfig.getDefaultConfig();
        Metadata metadata = new Metadata();
        AutoDetectParser parser = new AutoDetectParser(config);
        Detector detector = parser.getDetector();

        return CompletableFuture.runAsync(() -> {
            LOG.info("Starting rawdata exporter!");
            ContentStreamBuffer buffer;
            try (ContentStreamConsumer consumer = contentStream.consumer(topic)) {
                while (!closed.get() && (buffer = consumer.receive(1, TimeUnit.SECONDS)) != null) {
                    Path filePath = targetPath.resolve(buffer.position()).normalize();
                    if (!Files.exists(filePath)) {
                        Files.createDirectories(filePath);
                    }

                    for (String key : buffer.keys()) {
                        byte[] data = buffer.get(key);
                        String content = new String(data, StandardCharsets.UTF_8);

                        MediaType mediaType = detector.detect(new ByteArrayInputStream(data), metadata);
                        String subtype = mediaType.getSubtype();
                        if ("plain".equals(subtype)) {
                            if (content.startsWith("{") || content.startsWith("[") && !key.endsWith(".json")) {
                                subtype = ".json";
                            } else if (content.startsWith("<")) {
                                subtype = ".xml";
                            } else {
                                subtype = "";
                            }
                        } else {
                            subtype = "." + subtype;
                        }

                        Path contentFilePath = filePath.resolve(filePath.resolve(key) + subtype);
                        Files.write(contentFilePath, data);
                    }
                }

                LOG.info("Stop rawdata exporter!");

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Path getWorkDir() {
        Objects.requireNonNull(workDir);
        return workDir;
    }

    void nap(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

}
