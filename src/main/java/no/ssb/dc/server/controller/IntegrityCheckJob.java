package no.ssb.dc.server.controller;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamConsumer;
import no.ssb.dc.server.component.ContentStoreComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class IntegrityCheckJob {
    private static final Logger LOG = LoggerFactory.getLogger(IntegrityCheckJob.class);

    private final DynamicConfiguration configuration;
    private final ContentStore contentStore;
    private final IntegrityCheckJobSummary summary;

    public IntegrityCheckJob(DynamicConfiguration configuration, ContentStoreComponent contentStoreComponent, IntegrityCheckJobSummary summary) {
        this.configuration = configuration;
        this.contentStore = contentStoreComponent.getDelegate();
        this.summary = summary;
    }

    public void consume(String topic) {
        summary.setStarted();
        summary.setTopic(topic);
        String lastPosition = contentStore.lastPosition(topic);
        summary.setLastPosition(lastPosition);
        ContentStreamConsumer consumer = contentStore.contentStream().consumer(topic);
        try {
            ContentStreamBuffer buffer;
            ContentStreamBuffer peekBuffer = null;
            boolean test = true;
            while ((buffer = consumer.receive(1, TimeUnit.SECONDS)) != null) {
                peekBuffer = buffer;
                summary.incrementPositionCount();

                if (test) {
                    summary.setFirstPosition(buffer.position());
                    test = false;
                }

                summary.updatePositionCounter(buffer);

                if (lastPosition != null && lastPosition.equals(buffer.position())) {
                    break;
                }
            }

            if (lastPosition == null && peekBuffer != null) {
                summary.setLastPosition(peekBuffer.position());
            }

            summary.setEnded();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
