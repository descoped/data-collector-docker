package no.ssb.dc.server.recovery;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.application.spi.Component;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoveryContentStoreComponent implements Component {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryContentStoreComponent.class);

    private final DynamicConfiguration configuration;
    private final AtomicBoolean closed = new AtomicBoolean(true);
    private ContentStore contentStore;

    public RecoveryContentStoreComponent(DynamicConfiguration configuration) {
        this.configuration = configuration;
    }

    public static RecoveryContentStoreComponent create(DynamicConfiguration configuration) {
        RecoveryContentStoreComponent contentStoreComponent = new RecoveryContentStoreComponent(configuration);
        contentStoreComponent.initialize();
        return contentStoreComponent;
    }

    @Override
    public void initialize() {
        if (isOpen()) {
            return;
        }
        Map<String, String> configMap = new LinkedHashMap<>(this.configuration.asMap());

        // remove data encryption keys in recovery, because encrypted data will be passed from ContentStore-consumer to RecoveryContentStore-producer
        // we don't want double encryption
        configMap.remove("rawdata.encryption.key");
        configMap.remove("rawdata.encryption.salt");
        configMap.put("recovery.rawdata.encryption.credentials.ignore", "true");

        // custom rule to ensure that producer writes that to recovery folder
        String gscRecoveryFolder = configuration.evaluateToString("local-temp-folder");
        if (gscRecoveryFolder != null) {
            configMap.put("local-temp-folder", gscRecoveryFolder + "-recovery");
        }
        contentStore = ProviderConfigurator.configure(configMap, this.configuration.evaluateToString("content.stream.connector"), ContentStoreInitializer.class);
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
