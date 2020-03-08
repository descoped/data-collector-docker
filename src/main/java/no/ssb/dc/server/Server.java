package no.ssb.dc.server;

import ch.qos.logback.classic.ClassicConstants;
import net.bytebuddy.agent.ByteBuddyAgent;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.application.server.UndertowApplication;
import no.ssb.dc.core.http.HttpClientAgent;
import no.ssb.dc.core.util.JavaUtilLoggerBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private static final ConfigurationOverride configurationOverride = ConfigurationOverride.NONE;

    public static void main(String[] args) {
        HttpClientAgent.premain(null, ByteBuddyAgent.install());
        long now = System.currentTimeMillis();

        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
        JavaUtilLoggerBridge.installJavaUtilLoggerBridgeHandler();
        if (logbackConfigurationFile != null) {
            LOG.debug("Using logback configuration: {}" , logbackConfigurationFile);
        }

        StoreBasedDynamicConfiguration.Builder configurationBuilder = new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application-defaults.properties")
                .propertiesResource("/conf/application-defaults.properties")
                .propertiesResource("/conf/application.properties");

        /*
         * pre-req: `docker-compose up` in this repo
         */
        if (configurationOverride == ConfigurationOverride.DEBUG_USING_POSTGRES) {
            configurationBuilder.propertiesResource("application-integration.properties");
        }

        /*
         * pre-req: refer to data-collection-consumer-specifications-repo and configure gcs
         */
        if (configurationOverride == ConfigurationOverride.DEBUG_USING_GCS) {
            configurationBuilder.values(IntegrationTestProfile.loadApplicationPropertiesFromConsumerSpecificationProfile("gcs",
                    "data.collector.certs.directory", "gcs.service-account.key-file", "local-temp-folder"));
        }

        configurationBuilder
                .environment("DC_")
                .systemProperties();

        StoreBasedDynamicConfiguration configuration = configurationBuilder.build();
        UndertowApplication application = UndertowApplication.initializeUndertowApplication(configuration);

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.warn("ShutdownHook triggered..");
                application.stop();
            }));

            application.start();

            long time = System.currentTimeMillis() - now;
            LOG.info("Server started in {}ms..", time);

            // wait for termination signal
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
            }
        } finally {
            application.stop();
        }
    }

    enum ConfigurationOverride {
        NONE,
        DEBUG_USING_POSTGRES,
        DEBUG_USING_GCS;
    }
}
