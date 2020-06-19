package no.ssb.dc.server.component;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.application.spi.Component;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class ContentStoreComponent implements Component {

    private static final Logger LOG = LoggerFactory.getLogger(ContentStoreComponent.class);

    private final DynamicConfiguration configuration;
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private ContentStore contentStore;

    public ContentStoreComponent(DynamicConfiguration configuration) {
        this.configuration = configuration;
    }

    public static ContentStoreComponent create(DynamicConfiguration configuration) {
        ContentStoreComponent contentStoreComponent = new ContentStoreComponent(configuration);
        contentStoreComponent.initialize();
        return contentStoreComponent;
    }

    @Override
    public void initialize() {
        if (isOpen()) {
            return;
        }
        contentStore = ProviderConfigurator.configure(configuration.asMap(), configuration.evaluateToString("content.stream.connector"), ContentStoreInitializer.class);
        closed.set(false);
    }

    @Override
    public boolean isOpen() {
        return !closed.get();
    }

    @Override
    public <R> R getDelegate() {
        Objects.requireNonNull(contentStore);
        return (R) contentStore;
    }

    @Override
    public void close() throws Exception {
        if (closed.get()) {
            return;
        }
        contentStore.close();
    }
}
