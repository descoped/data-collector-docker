package io.descoped.dc.server.recovery;

import io.descoped.config.DynamicConfiguration;
import io.descoped.dc.api.util.CommonUtils;
import io.descoped.dc.application.spi.Service;
import io.descoped.dc.server.content.ContentStoreComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.descoped.dc.server.db.SequenceDbHelper.getSequenceDatabaseLocation;

/*
 * Run the integrity checker before running recovery.
 */

public class RecoveryService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryService.class);

    private final DynamicConfiguration configuration;
    private final ContentStoreComponent contentStoreComponent;
    private final RecoveryContentStoreComponent recoveryContentStoreComponent;
    final Map<String, CompletableFuture<RecoveryWorker>> jobFutures = new ConcurrentHashMap<>();
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
            entry.getValue().terminate();
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

    void createRecoveryWorker(String fromTopic, String toTopic) {
        Path dbLocation = getSequenceDatabaseLocation(configuration);
        LOG.trace("Database path: {}", dbLocation);
        if (!dbLocation.toFile().exists()) {
            throw new RuntimeException("Lmdb Location does not exist: " + dbLocation.toString());
        }

        CompletableFuture<RecoveryWorker> workerFuture = CompletableFuture.supplyAsync(() -> {
            RecoveryWorker recoveryWorker = new RecoveryWorker(configuration, contentStoreComponent, recoveryContentStoreComponent);
            jobs.put(fromTopic, recoveryWorker);
            recoveryWorker.recover(fromTopic, toTopic);
            LOG.trace("Completed Recovery!");
            return recoveryWorker;
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
        jobFutures.put(fromTopic, workerFuture);
    }
}
