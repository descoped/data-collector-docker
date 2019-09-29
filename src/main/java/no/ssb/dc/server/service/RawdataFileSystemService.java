package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.application.Service;
import no.ssb.dc.content.RawdataFileSystemWriter;
import no.ssb.rawdata.api.RawdataClient;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public class RawdataFileSystemService implements Service {

    private final DynamicConfiguration configuration;
    private final RawdataClient rawdataClient;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private RawdataFileSystemWriter writer;

    public RawdataFileSystemService(DynamicConfiguration configuration, RawdataClient rawdataClient) {
        this.configuration = configuration;
        this.rawdataClient = rawdataClient;
    }

    @Override
    public void start() {
        if (!running.get()) {
            running.set(true);
            String location = configuration.evaluateToString("data.collector.rawdata.dump.location");
            writer = new RawdataFileSystemWriter(
                    rawdataClient,
                    configuration.evaluateToString("namespace.default"),
                    location != null ?
                            (location.startsWith("/") ? Paths.get(location).toAbsolutePath().normalize() : Paths.get(".").toAbsolutePath().resolve(location)).normalize() :
                            Paths.get(".").toAbsolutePath().resolve("target").resolve("rawdata").normalize()
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
