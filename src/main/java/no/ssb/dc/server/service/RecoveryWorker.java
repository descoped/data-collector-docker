package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamConsumer;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.server.component.ContentStoreComponent;
import no.ssb.dc.server.component.RecoveryContentStoreComponent;
import org.lmdbjava.Dbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static no.ssb.dc.server.service.SequenceDbHelper.getSequenceDatabaseLocation;

public class RecoveryWorker {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryWorker.class);

    private final DynamicConfiguration configuration;
    private final ContentStore contentStore;
    private final ContentStore recoveryContentStore;
    private final RecoveryMonitor monitor = new RecoveryMonitor();

    public RecoveryWorker(DynamicConfiguration configuration,
                          ContentStoreComponent contentStoreComponent,
                          RecoveryContentStoreComponent recoveryContentStoreComponent) {
        this.configuration = configuration;
        this.contentStore = contentStoreComponent.getDelegate();
        this.recoveryContentStore = recoveryContentStoreComponent.getDelegate();
    }

    public void recover(String sourceTopic, String targetTopic) {
        monitor.setStarted();
        Path dbLocation = getSequenceDatabaseLocation(configuration);
        monitor.setSourceDatabasePath(dbLocation);
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
            try (ContentStreamProducer producer = recoveryContentStore.contentStream().producer(targetTopic)) {
                try (ContentStreamConsumer consumer = contentStore.contentStream().consumer(sourceTopic)) {
                    ContentStreamBuffer buffer;
                    while ((buffer = consumer.receive(15, TimeUnit.SECONDS)) != null) {
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
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void publishBuffers(AtomicInteger bufferCounter, List<String> bufferedPositions, ContentStreamProducer producer) {
        String[] publishPositions = bufferedPositions.toArray(new String[bufferedPositions.size()]);
        producer.publish(publishPositions);
        monitor.incrementCopiedPositions(publishPositions.length);
        //LOG.trace("Published: [{}]", String.join(", ", publishPositions));
        bufferCounter.set(0);
        bufferedPositions.clear();
        monitor.resetBufferedPositions();
    }

    public RecoveryMonitor.Summary summary() {
        return monitor.build();
    }
}
