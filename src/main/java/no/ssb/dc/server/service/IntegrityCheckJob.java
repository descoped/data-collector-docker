package no.ssb.dc.server.service;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamConsumer;
import no.ssb.dc.api.ulid.ULIDGenerator;
import no.ssb.dc.server.component.ContentStoreComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
        try (ContentStreamConsumer consumer = contentStore.contentStream().consumer(topic)) {
            ContentStreamBuffer buffer;
            ContentStreamBuffer peekBuffer = null;
            boolean test = true;
            int timeoutInSeconds = 15;
            if (configuration.evaluateToString("data.collector.consumer.timeoutInSeconds") != null) {
                timeoutInSeconds = configuration.evaluateToInt("data.collector.consumer.timeoutInSeconds");
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

                index.commit();
                generateReport();

                summary.setEnded();
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void terminate() {
        terminated.set(true);
    }

    // generate a duplicate report in json format
    public void generateReport() {
        Path reportPath = index.getDatabaseDir().resolve("report");
        String reportId = ULIDGenerator.toUUID(ULIDGenerator.generate()).toString() + ".json";

        AtomicReference<IntegrityCheckIndex.SequenceKey> prevSequenceKey = new AtomicReference<>();
        Map<String, Set<ULID.Value>> duplicatePositions = new LinkedHashMap<>();

        try (JsonArrayWriter writer = new JsonArrayWriter(reportPath, reportId, 5000)) {
            index.readSequence((sequenceKey, hasNext) -> {
                LOG.trace("{} -- {}", sequenceKey.position, hasNext);
                // mark first position
                if (prevSequenceKey.get() == null) {
                    prevSequenceKey.set(sequenceKey);
                    return;
                }

                // check if we hit a duplicate on stream
                if (prevSequenceKey.get().position.equals(sequenceKey.position)) {
                    // make counters for position and increment duplicateCount
                    duplicatePositions.computeIfAbsent(prevSequenceKey.get().position, duplicateUlidSet -> new TreeSet<>()).add(prevSequenceKey.get().ulid);
                    duplicatePositions.get(sequenceKey.position).add(sequenceKey.ulid);
                }

                // if we got duplicates then write data
                if (duplicatePositions.size() > 0 && (!prevSequenceKey.get().position.equals(sequenceKey.position) || !hasNext)) {
                    ObjectNode positionNode = writer.parser().createObjectNode();
                    ArrayNode ulidArray = writer.parser().createArrayNode();
                    Set<ULID.Value> ulidSet = duplicatePositions.get(prevSequenceKey.get().position);
                    ulidSet.forEach(ulid -> ulidArray.add(ULIDGenerator.toUUID(ulid).toString()));
                    positionNode.set(prevSequenceKey.get().position, ulidArray);
                    writer.write(positionNode);
                    duplicatePositions.clear();
                }

                // move marker to next
                prevSequenceKey.set(sequenceKey);
            });
        }

        summary.setDuplicateReportId(reportId);
    }

    public IntegrityCheckJobSummary.Summary getSummary() {
        return summary.build();
    }

}
