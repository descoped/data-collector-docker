package io.descoped.dc.server.integrity;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.huxhorn.sulky.ulid.ULID;
import io.descoped.config.DynamicConfiguration;
import io.descoped.dc.api.content.ContentStore;
import io.descoped.dc.api.content.ContentStream;
import io.descoped.dc.api.content.ContentStreamBuffer;
import io.descoped.dc.api.content.ContentStreamConsumer;
import io.descoped.dc.api.ulid.ULIDGenerator;
import io.descoped.dc.api.util.JsonParser;
import io.descoped.dc.server.content.ContentStoreComponent;
import io.descoped.dc.server.db.SequenceKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class IntegrityCheckJob {
    private static final Logger LOG = LoggerFactory.getLogger(IntegrityCheckJob.class);

    private final DynamicConfiguration configuration;
    private final ContentStore contentStore;
    private final IntegrityCheckIndex index;
    private final IntegrityCheckJobSummary summary;
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    public IntegrityCheckJob(DynamicConfiguration configuration, ContentStoreComponent contentStoreComponent, IntegrityCheckIndex index, IntegrityCheckJobSummary summary) {
        this.configuration = configuration;
        this.contentStore = contentStoreComponent.getDelegate();
        this.index = index;
        this.summary = summary;
    }

    public void consume(String topic) {
        summary.setStarted();
        summary.setTopic(topic);
        String lastPosition = contentStore.lastPosition(topic);
        summary.setLastPosition(lastPosition);
        ContentStream contentStream = contentStore.contentStream();
        try {
            ContentStreamConsumer consumer = contentStream.consumer(topic);
            ContentStreamBuffer buffer;
            ContentStreamBuffer peekBuffer = null;
            boolean test = true;
            int timeoutInSeconds = 15;
            if (configuration.evaluateToString("data.collector.integrityCheck.consumer.timeoutInSeconds") != null) {
                timeoutInSeconds = configuration.evaluateToInt("data.collector.integrityCheck.consumer.timeoutInSeconds");
            }
            try {
                LOG.info("Check integrity for topic: {}", topic);
                while (!terminated.get() && (buffer = consumer.receive(timeoutInSeconds, TimeUnit.SECONDS)) != null) {
                    //System.out.printf("consume: %s%n", buffer.position());
                    peekBuffer = buffer;
                    summary.setCurrentPosition(buffer.position());
                    summary.incrementPositionCount();

                    if (test) {
                        summary.setFirstPosition(buffer.position());
                        test = false;
                    }

                    index.writeSequence(buffer.ulid(), buffer.position());

                    if (lastPosition != null && lastPosition.equals(buffer.position())) {
                        LOG.info("Reached en of stream for topic: {}", topic);
                        break;
                    }

                    // release data
                    buffer.data().clear();
                    buffer.manifest().clear();
                }

                LOG.info("Exited stream consumer for topic: {}", topic);
            } finally {
                if (lastPosition == null && peekBuffer != null) {
                    summary.setLastPosition(peekBuffer.position());
                }

                index.commitQueue();

                generateReport();

                summary.setEnded();

                generateSummary(summary.build());

                contentStream.closeAndRemoveConsumer(topic);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void terminate() {
        terminated.set(true);
    }

    public void generateSummary(IntegrityCheckJobSummary.Summary summary) {
        try {
            Path summaryFilenamePath = index.getDatabaseDir().resolve("report").resolve("summary.json");
            Files.createDirectories(summaryFilenamePath.getParent());
            String jsonSummary = JsonParser.createJsonParser().toPrettyJSON(summary);
            Files.write(summaryFilenamePath, jsonSummary.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // generate a duplicate report in json format
    public void generateReport() {
        Path reportPath = index.getDatabaseDir().resolve("report");
        String reportId = ULIDGenerator.toUUID(ULIDGenerator.generate()).toString() + ".json";
        LOG.info("Generating report to: {}/{}", reportPath, reportId);

        AtomicReference<SequenceKey> prevSequenceKey = new AtomicReference<>();
        Map<String, Set<ULID.Value>> duplicatePositionAndUlidSet = new LinkedHashMap<>();
        Map<String, AtomicLong> duplicatePositionCounter = new LinkedHashMap<>();

        try (JsonArrayWriter writer = new JsonArrayWriter(reportPath, reportId, 5000)) {
            index.readSequence((sequenceKey, hasNext) -> {
                // mark first position
                if (prevSequenceKey.get() == null) {
                    prevSequenceKey.set(sequenceKey);
                    return;
                }

                // check if we hit a duplicate on stream
                if (prevSequenceKey.get().position().equals(sequenceKey.position())) {
                    // make counters for position and increment duplicateCount
                    duplicatePositionAndUlidSet.computeIfAbsent(prevSequenceKey.get().position(), duplicateUlidSet -> new TreeSet<>()).add(prevSequenceKey.get().ulid());
                    duplicatePositionAndUlidSet.get(sequenceKey.position()).add(sequenceKey.ulid());
                }

                // if we got duplicates then write data
                if (duplicatePositionAndUlidSet.size() > 0 && (!prevSequenceKey.get().position().equals(sequenceKey.position()) || !hasNext)) {
                    ObjectNode positionNode = writer.parser().createObjectNode();
                    ArrayNode ulidArray = writer.parser().createArrayNode();
                    Set<ULID.Value> ulidSet = duplicatePositionAndUlidSet.get(prevSequenceKey.get().position());
                    ulidSet.forEach(ulid -> {
                        ulidArray.add(ULIDGenerator.toUUID(ulid).toString());
                    });
                    duplicatePositionCounter.computeIfAbsent(prevSequenceKey.get().position(), counter -> new AtomicLong()).set(ulidSet.size());
                    positionNode.set(prevSequenceKey.get().position(), ulidArray);
                    writer.write(positionNode);
                    duplicatePositionAndUlidSet.clear();
                }

                // move marker to next
                prevSequenceKey.set(sequenceKey);
            });
        }

        summary.setReportPath(reportPath);
        summary.setDuplicateReportId(reportId);
        summary.setDuplicatePositionStats(duplicatePositionCounter);
        LOG.info("Done generating report");
    }

    public IntegrityCheckJobSummary.Summary getSummary() {
        IntegrityCheckJobSummary.Summary build = summary.build();
        return build;
    }

}
