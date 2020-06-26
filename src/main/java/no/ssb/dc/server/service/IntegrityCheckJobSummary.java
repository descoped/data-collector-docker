package no.ssb.dc.server.service;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.health.HealthResourceUtils;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class IntegrityCheckJobSummary {

    private final AtomicReference<String> topic = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong started = new AtomicLong();
    private final AtomicLong ended = new AtomicLong();
    private final AtomicReference<String> firstPosition = new AtomicReference<>();
    private final AtomicReference<String> lastPosition = new AtomicReference<>();
    private final AtomicReference<String> currentPosition = new AtomicReference<>();
    private final AtomicLong positionCount = new AtomicLong();
    private final AtomicReference<String> duplicateReportId = new AtomicReference<>();
    private final AtomicLong duplicatePositions = new AtomicLong();
    private final AtomicLong affectedPositions = new AtomicLong();
    private final Map<String, AtomicLong> duplicatePositionCount = new LinkedHashMap<>();

    public IntegrityCheckJobSummary() {
    }

    IntegrityCheckJobSummary setTopic(String topic) {
        this.topic.set(topic);
        return this;
    }

    IntegrityCheckJobSummary setStarted() {
        running.set(true);
        started.set(Instant.now().toEpochMilli());
        return this;
    }

    IntegrityCheckJobSummary setEnded() {
        ended.set(Instant.now().toEpochMilli());
        running.set(false);
        return this;
    }

    IntegrityCheckJobSummary setFirstPosition(String position) {
        firstPosition.set(position);
        return this;
    }

    IntegrityCheckJobSummary setLastPosition(String position) {
        lastPosition.set(position);
        return this;
    }

    IntegrityCheckJobSummary setCurrentPosition(String position) {
        currentPosition.set(position);
        return this;
    }

    IntegrityCheckJobSummary incrementPositionCount() {
        positionCount.incrementAndGet();
        return this;
    }

    public IntegrityCheckJobSummary setDuplicateReportId(String reportId) {
        duplicateReportId.set(reportId);
        return this;
    }

    public IntegrityCheckJobSummary setDuplicatePositionStats(Map<String, AtomicLong> duplicatePositionCounter) {
        AtomicLong duplicatePositionCount = new AtomicLong(0);
        duplicatePositionCounter.forEach((key, value) -> {
            duplicatePositionCount.addAndGet(value.get());
        });
        duplicatePositions.set(duplicatePositionCount.get());
        affectedPositions.set(duplicatePositionCounter.size());
        return this;
    }

    public Summary build() {
        Summary summary = new Summary(
                topic.get(),
                running.get(),
                started.get(),
                ended.get(),
                firstPosition.get(),
                lastPosition.get(),
                currentPosition.get(),
                positionCount.get(),
                duplicatePositions.get(),
                affectedPositions.get(),
                duplicateReportId.get()
        );
        return summary;
    }

    static class PositionInfo {
        final ULID.Value ulid;
        final String position;

        PositionInfo(ContentStreamBuffer buffer) {
            ulid = buffer.ulid();
            position = buffer.position();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PositionInfo that = (PositionInfo) o;
            return Objects.equals(position, that.position);
        }

        @Override
        public int hashCode() {
            return Objects.hash(position);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Summary {
        @JsonProperty public String topic;
        @JsonProperty public String status;
        @JsonProperty public String started;
        @JsonProperty public String ended;
        @JsonProperty public String since;
        @JsonProperty public String firstPosition;
        @JsonProperty public String lastPosition;
        @JsonProperty public String currentPosition;
        @JsonProperty public long checkedPositions;
        @JsonProperty public long duplicatePositions;
        @JsonProperty public long affectedPositions;
        @JsonIgnore final String reportId;

        public Summary(String topic, boolean running, long started, long ended,
                       String firstPosition, String lastPosition, String currentPosition, long checkedPositions,
                       long duplicatePositions, long affectedPositions, String reportId) {
            this.topic = topic;
            this.status = running ? "RUNNING" : "CLOSED";
            this.started = Instant.ofEpochMilli(started).toString();
            this.ended = Instant.ofEpochMilli(ended).toString();
            this.since = HealthResourceUtils.durationAsString(started);
            this.firstPosition = firstPosition;
            this.lastPosition = lastPosition;
            this.currentPosition = currentPosition;
            this.checkedPositions = checkedPositions;
            this.duplicatePositions = duplicatePositions;
            this.affectedPositions = affectedPositions;
            this.reportId = reportId;
        }
    }

    public static class PositionSummary {
        @JsonProperty String position;
        @JsonProperty Integer duplicateCount;
        @JsonProperty List<String> ulid;

        public PositionSummary(String position, Integer duplicateCount, List<String> ulid) {
            this.position = position;
            this.duplicateCount = duplicateCount;
            this.ulid = ulid;
        }
    }

}
