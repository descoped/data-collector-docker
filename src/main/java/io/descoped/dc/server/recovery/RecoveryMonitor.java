package io.descoped.dc.server.recovery;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.descoped.dc.api.health.HealthResourceUtils;

import java.math.RoundingMode;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Instant;
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
    final AtomicLong bufferedPositions = new AtomicLong(0);
    final AtomicLong copiedPositions = new AtomicLong(0);
    final AtomicLong postCheckFromTimestamp = new AtomicLong(0);
    final AtomicReference<String> postCheckStartPosition = new AtomicReference<>();
    final AtomicReference<String> postCheckLastPosition = new AtomicReference<>();
    final AtomicLong postCheckCheckedPositions = new AtomicLong(0);

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

    void setPostCheckFromTimestamp(long timestamp) {
        postCheckFromTimestamp.set(timestamp);
    }

    void setPostCheckStartPosition(String position) {
        postCheckStartPosition.set(position);
    }

    void setPostCheckLastPosition(String position) {
        postCheckLastPosition.set(position);
    }

    void incrementPostCheckCheckedPositions() {
        postCheckCheckedPositions.incrementAndGet();
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
                copiedPositions.get(),
                postCheckFromTimestamp.get(),
                postCheckStartPosition.get(),
                postCheckLastPosition.get(),
                postCheckCheckedPositions.get()
        );
    }

    public static class Summary {

        @JsonProperty
        public final String status;
        @JsonProperty
        public final String started;
        @JsonProperty
        public final String ended;
        @JsonProperty
        public final String since;
        @JsonProperty
        public final String startPosition;
        @JsonProperty
        public final String currentPosition;
        @JsonProperty
        public final String lastPosition;
        @JsonProperty
        public final Path indexDatabasePath;
        @JsonProperty
        public final String sourceTopic;
        @JsonProperty
        public final String targetTopic;
        @JsonProperty
        public final long bufferedPositions;
        @JsonProperty
        public final long copiedPositions;
        @JsonProperty
        public final float averageCopiedPositionsPerSecond;
        @JsonProperty
        public final String postCheckFromTimestamp;
        @JsonProperty
        public final String postCheckStartPosition;
        @JsonProperty
        public final String postCheckLastPosition;
        @JsonProperty
        public final long postCheckCheckedPositions;

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
                       long copiedPositions,
                       long postCheckFromTimestamp,
                       String postCheckStartPosition,
                       String postCheckLastPosition,
                       long postCheckCheckedPositions) {

            this.status = running ? "RUNNING" : "COMPLETED";
            this.started = Instant.ofEpochMilli(started).toString();
            this.ended = Instant.ofEpochMilli(ended).toString();
            this.since = HealthResourceUtils.durationAsString(started);
            this.startPosition = startPosition;
            this.currentPosition = currentPosition;
            this.lastPosition = lastPosition;
            this.indexDatabasePath = sourceDatabasePath;
            this.sourceTopic = sourceTopic;
            this.targetTopic = targetTopic;
            this.bufferedPositions = bufferedPositions;
            this.copiedPositions = copiedPositions;
            this.postCheckFromTimestamp = Instant.ofEpochMilli(postCheckFromTimestamp).toString();
            this.postCheckStartPosition = postCheckStartPosition;
            this.postCheckLastPosition = postCheckLastPosition;
            this.postCheckCheckedPositions = postCheckCheckedPositions;

            long now = System.currentTimeMillis();
            Float averageRequestPerSecond = HealthResourceUtils.divide(copiedPositions, (now - started) / 1000);
            DecimalFormat df = new DecimalFormat("#.##");
            df.setRoundingMode(RoundingMode.UP);
            this.averageCopiedPositionsPerSecond = Float.parseFloat(df.format(averageRequestPerSecond));
        }
    }
}
