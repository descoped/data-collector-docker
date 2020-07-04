package no.ssb.dc.server.recovery;

import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStream;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamConsumer;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.api.ulid.ULIDGenerator;
import no.ssb.dc.server.content.ContentStoreComponent;
import no.ssb.dc.server.db.LmdbEnvironment;
import no.ssb.dc.server.db.SequenceDbHelper;
import no.ssb.dc.server.integrity.JsonArrayWriter;
import org.lmdbjava.Dbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static no.ssb.dc.server.db.SequenceDbHelper.getSequenceDatabaseLocation;

public class RecoveryWorker {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryWorker.class);

    private final DynamicConfiguration configuration;
    private final ContentStore contentStore;
    private final ContentStore recoveryContentStore;
    private final RecoveryMonitor monitor = new RecoveryMonitor();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public RecoveryWorker(DynamicConfiguration configuration,
                          ContentStoreComponent contentStoreComponent,
                          RecoveryContentStoreComponent recoveryContentStoreComponent) {
        this.configuration = configuration;
        this.contentStore = contentStoreComponent.getDelegate();
        this.recoveryContentStore = recoveryContentStoreComponent.getDelegate();
    }

    public void recover(String sourceTopic, String targetTopic) {
        LOG.info("Copy from {} to {}", sourceTopic, targetTopic);
        monitor.setStarted();
        Path dbLocation = getSequenceDatabaseLocation(configuration);
        monitor.setSourceDatabasePath(dbLocation.resolve(sourceTopic));
        monitor.setSourceTopic(sourceTopic);
        monitor.setTargetTopic(targetTopic);
        try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(configuration, dbLocation, sourceTopic)) {
            Dbi<ByteBuffer> sequenceDb = lmdbEnvironment.open();
            SequenceDbHelper sequenceDbHelper = new SequenceDbHelper(lmdbEnvironment, sequenceDb);
            PositionAndULIDVersion firstPosition = sequenceDbHelper.findFirstPosition();
            monitor.setStartPosition(firstPosition.position());
            PositionAndULIDVersion lastPosition = sequenceDbHelper.findLastPosition();
            monitor.setLastPosition(lastPosition.position());

            int publishAtCount = 1000;
            AtomicInteger bufferCounter = new AtomicInteger(0);
            List<String> bufferedPositions = new ArrayList<>(publishAtCount);
            ContentStream contentStream = contentStore.contentStream();
            ContentStream recoveryContentStream = recoveryContentStore.contentStream();
            AtomicLong lastTimestamp = new AtomicLong(0);
            try {
                ContentStreamProducer producer = recoveryContentStream.producer(targetTopic);
                ContentStreamConsumer consumer = contentStream.consumer(sourceTopic);
                ContentStreamBuffer buffer;
                ContentStreamBuffer peekBuffer = null;
                while (!closed.get() && (buffer = consumer.receive(15, TimeUnit.SECONDS)) != null) {
                    peekBuffer = buffer;
                    ContentStreamBuffer.Builder producerBuilder = producer.builder();
                    monitor.setCurrentPosition(buffer.position());
                    producerBuilder.ulid(buffer.ulid());
                    producerBuilder.position(buffer.position());
                    for (String key : buffer.keys()) {
                        producerBuilder.put(key, buffer.get(key));
                    }
                    producer.produce(producerBuilder);
                    bufferedPositions.add(buffer.position());
                    monitor.incrementBufferedPositions();

                    if (bufferCounter.incrementAndGet() == publishAtCount) {
                        publishBuffers(bufferCounter, bufferedPositions, producer);
                    }
                    if (lastPosition.ulid().equals(buffer.ulid()) && lastPosition.position().equals(buffer.position())) {
                        if (!bufferedPositions.isEmpty()) {
                            publishBuffers(bufferCounter, bufferedPositions, producer);
                        }
                        break;
                    }
                }
                monitor.setEnded();
                if (peekBuffer != null) {
                    lastTimestamp.set(peekBuffer.ulid().timestamp());
                }
                LOG.info("Successful recovery from {} to {}. Recovered {} positions.", sourceTopic, targetTopic, monitor.copiedPositions.get());
            } catch (Exception e) {
                monitor.setEnded();
                throw new RuntimeException(e);
            } finally {
                recoveryContentStream.closeAndRemoveProducer(sourceTopic);
                contentStream.closeAndRemoveConsumer(targetTopic);
            }

            // test tail from last recovered position
            if (lastTimestamp.get() == 0) {
                return;
            }

            ContentStreamConsumer consumer = contentStream.consumer(targetTopic);
            try {
                LOG.trace("Post check recovered positions from timestamp: {}", Instant.ofEpochMilli(lastTimestamp.get()).toString());
                monitor.setPostCheckFromTimestamp(lastTimestamp.get());
                consumer.seek(lastTimestamp.get());
                ContentStreamBuffer buffer;
                Path reportPath = getSequenceDatabaseLocation(configuration).resolve(targetTopic).resolve("report");
                Files.createDirectories(reportPath);
                try (JsonArrayWriter writer = new JsonArrayWriter(reportPath, "tail-report.json", 5000)) {
                    while (!closed.get() && (buffer = consumer.receive(3, TimeUnit.SECONDS)) != null) {
                        if (monitor.postCheckStartPosition.get() == null) {
                            monitor.setPostCheckStartPosition(buffer.position());
                        }
                        monitor.setPostCheckLastPosition(buffer.position());
                        monitor.incrementPostCheckCheckedPositions();
                        ObjectNode positionNode = writer.parser().createObjectNode();
                        positionNode.put(ULIDGenerator.toUUID(buffer.ulid()).toString(), buffer.position());
                        writer.write(positionNode);
                    }
                }
                LOG.trace("Done post checking tail positions! Tail size is: {}", monitor.postCheckCheckedPositions.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                contentStream.closeAndRemoveConsumer(targetTopic);
            }
        }
    }

    private void publishBuffers(AtomicInteger bufferCounter, List<String> bufferedPositions, ContentStreamProducer producer) {
        String[] publishPositions = bufferedPositions.toArray(new String[0]);
        producer.publish(publishPositions);
        monitor.incrementCopiedPositions(publishPositions.length);
        //LOG.trace("Published: [{}]", String.join(", ", publishPositions));
        bufferCounter.set(0);
        bufferedPositions.clear();
        monitor.resetBufferedPositions();
    }

    public RecoveryMonitor monitor() {
        return monitor;
    }

    public RecoveryMonitor.Summary summary() {
        return monitor.build();
    }

    public void terminate() {
        closed.set(true);
    }
}
