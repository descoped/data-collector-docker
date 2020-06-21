package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamConsumer;
import no.ssb.dc.server.component.ContentStoreComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class IntegrityCheckJob {
    private static final Logger LOG = LoggerFactory.getLogger(IntegrityCheckJob.class);

    private final DynamicConfiguration configuration;
    private final ContentStore contentStore;
    private final IntegrityCheckJobSummary summary;
    private final AtomicBoolean terminated = new AtomicBoolean(false);

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
            int timeoutInSeconds = 5;
            if (configuration.evaluateToString("data.collector.consumer.timeoutInSeconds") != null) {
                timeoutInSeconds = configuration.evaluateToInt("data.collector.consumer.timeoutInSeconds");
            }
            while (!terminated.get() && (buffer = consumer.receive(timeoutInSeconds, TimeUnit.SECONDS)) != null) {
                //System.out.printf("consume: %s%n", buffer.position());
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

    public IntegrityCheckJobSummary.Summary getSummary() {
        return summary.build();
    }

    public void terminate() {
        terminated.set(true);
    }

}
