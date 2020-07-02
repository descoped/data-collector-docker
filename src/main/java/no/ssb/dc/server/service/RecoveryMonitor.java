package no.ssb.dc.server.service;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RecoveryMonitor {

    final AtomicBoolean running = new AtomicBoolean();
    final AtomicLong started = new AtomicLong();
    final AtomicLong ended = new AtomicLong();
    final AtomicReference<String> startPosition = new AtomicReference<>();
    final AtomicReference<String> currentPosition = new AtomicReference<>();
    final AtomicReference<String> lastPosition = new AtomicReference<>();
    final AtomicReference<Path> sourceDatabasePath = new AtomicReference<>();
    final AtomicReference<String> sourceTopic = new AtomicReference<>();
    final AtomicReference<String> targetTopic = new AtomicReference<>();
    final AtomicLong bufferedPositions = new AtomicLong();
    final AtomicLong copiedPositions = new AtomicLong();

    void setStarted() {
        started.set(System.currentTimeMillis());
        running.set(true);
    }

    void setEnded() {
        ended.set(System.currentTimeMillis());
        running.set(false);
    }

    void setStartPosition(String position) {
        startPosition.set(position);
    }

    void setCurrentPosition(String position) {
        currentPosition.set(position);
    }

    void setLastPosition(String position) {
        lastPosition.set(position);
    }

    void setSourceDatabasePath(Path dbLocation) {
        sourceDatabasePath.set(dbLocation);
    }

    void setSourceTopic(String sourceTopic) {
        this.sourceTopic.set(sourceTopic);
    }

    void setTargetTopic(String targetTopic) {
        this.targetTopic.set(targetTopic);
    }

    void incrementBufferedPositions() {
        bufferedPositions.incrementAndGet();
    }

    void resetBufferedPositions() {
        bufferedPositions.set(0);
    }

    void incrementCopiedPositions(int numberOfPositions) {
        copiedPositions.addAndGet(numberOfPositions);
    }

    public Summary build() {
        return new Summary(
                running.get(),
                started.get(),
                ended.get(),
                startPosition.get(),
                currentPosition.get(),
                lastPosition.get(),
                sourceDatabasePath.get(),
                sourceTopic.get(),
                targetTopic.get(),
                bufferedPositions.get(),
                copiedPositions.get()
        );
    }

    public static class Summary {

        @JsonProperty public final boolean running;
        @JsonProperty public final long started;
        @JsonProperty public final long ended;
        @JsonProperty public final String startPosition;
        @JsonProperty public final String currentPosition;
        @JsonProperty public final String lastPosition;
        @JsonProperty public final Path sourceDatabasePath;
        @JsonProperty public final String sourceTopic;
        @JsonProperty public final String targetTopic;
        @JsonProperty public final long bufferedPositions;
        @JsonProperty public final long copiedPositions;

        public Summary(boolean running,
                       long started,
                       long ended,
                       String startPosition,
                       String currentPosition,
                       String lastPosition,
                       Path sourceDatabasePath,
                       String sourceTopic,
                       String targetTopic,
                       long bufferedPositions,
                       long copiedPositions) {
            this.running = running;
            this.started = started;
            this.ended = ended;
            this.startPosition = startPosition;
            this.currentPosition = currentPosition;
            this.lastPosition = lastPosition;
            this.sourceDatabasePath = sourceDatabasePath;
            this.sourceTopic = sourceTopic;
            this.targetTopic = targetTopic;
            this.bufferedPositions = bufferedPositions;
            this.copiedPositions = copiedPositions;
        }
    }
}
