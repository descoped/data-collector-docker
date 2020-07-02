package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.application.spi.Service;
import no.ssb.dc.server.component.ContentStoreComponent;
import no.ssb.dc.server.component.RecoveryContentStoreComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static no.ssb.dc.server.service.SequenceDbHelper.getSequenceDatabaseLocation;

public class RecoveryService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryService.class);

    private final DynamicConfiguration configuration;
    private final ContentStoreComponent contentStoreComponent;
    private final RecoveryContentStoreComponent recoveryContentStoreComponent;
    final Map<String, RecoveryWorker> jobs = new ConcurrentHashMap<>();

    public RecoveryService(DynamicConfiguration configuration, ContentStoreComponent contentStoreComponent, RecoveryContentStoreComponent recoveryContentStoreComponent) {
        this.configuration = configuration;
        this.contentStoreComponent = contentStoreComponent;
        this.recoveryContentStoreComponent = recoveryContentStoreComponent;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        for (Map.Entry<String, RecoveryWorker> entry : jobs.entrySet()) {
            //entry.getValue().terminate();
        }
    }

    List<Path> getSequenceDatabaseList() {
        Path dbLocation = getSequenceDatabaseLocation(configuration);
        try {
            return Files.list(dbLocation).filter(path -> path.toFile().isDirectory()).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
