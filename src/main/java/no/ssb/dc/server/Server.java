package no.ssb.dc.server;

import ch.qos.logback.classic.ClassicConstants;
import net.bytebuddy.agent.ByteBuddyAgent;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dapla.secrets.api.SecretManagerClient;
import no.ssb.dc.api.security.BusinessSSLBundle;
import no.ssb.dc.application.server.UndertowApplication;
import no.ssb.dc.application.ssl.BusinessSSLBundleSupplier;
import no.ssb.dc.core.metrics.MetricsAgent;
import no.ssb.dc.core.util.JavaUtilLoggerBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class Server {

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private static final ConfigurationOverride configurationOverride = ConfigurationOverride.NONE;

    public static void main(String[] args) {
        MetricsAgent.premain(null, ByteBuddyAgent.install());
        long now = System.currentTimeMillis();

        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
        JavaUtilLoggerBridge.installJavaUtilLoggerBridgeHandler();
        if (logbackConfigurationFile != null) {
            LOG.debug("Using logback configuration: {}", logbackConfigurationFile);
        }

        StoreBasedDynamicConfiguration.Builder configurationBuilder = new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application-defaults.properties")
                .propertiesResource("/conf/application-defaults.properties")
                .propertiesResource("/conf/application.properties")
                .propertiesResource("/conf/application-override.properties");
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

        /*
         * Delegated certificate loader from Google Secret Manager is placed here, because we don't won't a
         * dependency in the data collector to any host aware libraries such as the Google Secret Manager.
         * The supplier is wrapped by the BusinessSSLBundleSupplier and passes an implementing instance of
         * BusinessSSLBundle to Worker.useBusinessSSLBundleSupplier() in the Core module.
         *
         * Please note: only Google Secret Manager is supported!
         */

        Supplier<BusinessSSLBundle> sslBundleSupplier = () -> {
            String businessSslBundleProvider = configuration.evaluateToString("data.collector.sslBundle.provider");
            if (!"google-secret-manager".equals(businessSslBundleProvider)) {
                return null;
            }

            Map<String, String> providerConfiguration = new LinkedHashMap<>();
            providerConfiguration.put("secrets.provider", businessSslBundleProvider);
            providerConfiguration.put("secrets.projectId", configuration.evaluateToString("data.collector.sslBundle.gcs.projectId"));
                String gcsServiceAccountKeyPath = configuration.evaluateToString("data.collector.sslBundle.gcs.serviceAccountKeyPath");
            if (gcsServiceAccountKeyPath != null) {
                providerConfiguration.put("secrets.serviceAccountKeyPath", gcsServiceAccountKeyPath);
            }

            try (SecretManagerClient secretManagerClient = SecretManagerClient.create(providerConfiguration)) {
                return new BusinessSSLBundle() {

                    String getType() {
                        return configuration.evaluateToString("data.collector.sslBundle.type");
                    }

                    boolean isPEM() {
                        Objects.requireNonNull(getType());
                        return "pem".equalsIgnoreCase(getType());
                    }

                    @Override
                    public String bundleName() {
                        return secretManagerClient.readString("data.collector.sslBundle.name");
                    }

                    @Override
                    public byte[] publicCertificate() {
                        return isPEM() ? secretManagerClient.readBytes("data.collector.sslBundle.publicCertificate") : new byte[0];
                    }

                    @Override
                    public byte[] privateCertificate() {
                        return isPEM() ? secretManagerClient.readBytes("data.collector.sslBundle.privateCertificate") : new byte[0];
                    }

                    @Override
                    public byte[] archiveCertificate() {
                        return !isPEM() ? secretManagerClient.readBytes("data.collector.sslBundle.archiveCertificate") : new byte[0];
                    }

                    @Override
                    public byte[] passphrase() {
                        return secretManagerClient.readBytes("data.collector.sslBundle.passphrase");
                    }
                };
            }
        };

        UndertowApplication application = UndertowApplication.initializeUndertowApplication(configuration, new BusinessSSLBundleSupplier(sslBundleSupplier));

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
