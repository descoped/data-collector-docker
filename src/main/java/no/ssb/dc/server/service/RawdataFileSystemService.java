package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStream;
import no.ssb.dc.application.spi.Service;
import no.ssb.dc.content.RawdataFileSystemWriter;
import no.ssb.dc.content.provider.rawdata.RawdataClientContentStream;
import no.ssb.dc.server.component.ContentStoreComponent;
import no.ssb.rawdata.api.RawdataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public class RawdataFileSystemService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(RawdataFileSystemService.class);

    private final DynamicConfiguration configuration;
    private final RawdataClient rawdataClient;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private RawdataFileSystemWriter writer;

    public RawdataFileSystemService(DynamicConfiguration configuration, ContentStoreComponent contentStoreComponent) {
        this.configuration = configuration;
        ContentStore contentStore = contentStoreComponent.getDelegate();
        ContentStream contentStream = contentStore.contentStream();
        this.rawdataClient = ((RawdataClientContentStream)contentStream).getClient();
    }

    @Override
    public void start() {
        if (!configuration.evaluateToBoolean("data.collector.rawdata.dump.enabled")) {
            return;
        }

        if (configuration.evaluateToString("data.collector.rawdata.dump.location") == null) {
            LOG.warn("Unable to start file exporter. No location is defined");
            return;
        }

        if (!running.get()) {
            running.set(true);
            String location = configuration.evaluateToString("data.collector.rawdata.dump.location");
            Path path;
            if (location.isEmpty()) {
                path = Paths.get(".").toAbsolutePath().resolve("storage").normalize();
            } else {
                path = Paths.get(location).toAbsolutePath().normalize();
            }
            writer = new RawdataFileSystemWriter(
                    rawdataClient,
                    configuration.evaluateToString("data.collector.rawdata.dump.topic"),
                    path
            );
            writer.start();
        }
    }

    @Override
    public void stop() {
        if (running.get()) {
            writer.shutdown();
            running.set(false);
        }
    }
}
