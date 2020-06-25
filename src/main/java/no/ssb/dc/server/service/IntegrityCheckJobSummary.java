package no.ssb.dc.server.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.health.HealthResourceUtils;
import no.ssb.dc.api.ulid.ULIDGenerator;

import java.time.Instant;
import java.util.ArrayList;
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
    private final Map<String, List<PositionInfo>> positionCounter = new LinkedHashMap<>();
    private final IntegrityCheckIndex index;

    public IntegrityCheckJobSummary(IntegrityCheckIndex index) {
        this.index = index;
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

    IntegrityCheckJobSummary updatePositionCounter(ContentStreamBuffer buffer) {
        index.writeSequence(buffer.ulid(), buffer.position());
        synchronized (this) {
            positionCounter.computeIfAbsent(buffer.position(), counter -> new ArrayList<>()).add(new PositionInfo(buffer));
        }
        return this;
    }

    public Summary build() {
        // index.readSequence(); and create json summary file + make upload to client feature
        Map<String, List<PositionInfo>> duplicatePositions = new LinkedHashMap<>(positionCounter.size());
        synchronized (this) {
            for (Map.Entry<String, List<PositionInfo>> entry : positionCounter.entrySet()) {
                if (entry.getValue().size() > 1) {
                    duplicatePositions.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return new Summary(
                topic.get(),
                running.get(),
                started.get(),
                ended.get(),
                firstPosition.get(),
                lastPosition.get(),
                currentPosition.get(),
                positionCount.get(),
                duplicatePositions
        );
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
        @JsonProperty public long positionCount;
        @JsonProperty public List<PositionSummary> duplicates = new ArrayList<>();

        public Summary(String topic, boolean running, long started, long ended,
                       String firstPosition, String lastPosition, String currentPosition, long positionCount,
                       Map<String, List<PositionInfo>> duplicatePositions) {
            this.topic = topic;
            this.status = running ? "RUNNING" : "CLOSED";
            this.started = Instant.ofEpochMilli(started).toString();
            this.ended = Instant.ofEpochMilli(ended).toString();
            this.since = HealthResourceUtils.durationAsString(started);
            this.firstPosition = firstPosition;
            this.lastPosition = lastPosition;
            this.currentPosition = currentPosition;
            this.positionCount = positionCount;
            for(Map.Entry<String, List<PositionInfo>> entry : duplicatePositions.entrySet()) {
                String position = entry.getKey();
                int count = entry.getValue().size();
                List<String> ulidList = new ArrayList<>();
                for (PositionInfo info : entry.getValue()) {
                    String s = ULIDGenerator.toUUID(info.ulid).toString();
                    ulidList.add(s);
                }
                duplicates.add(new PositionSummary(position, count, ulidList));
            }
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
