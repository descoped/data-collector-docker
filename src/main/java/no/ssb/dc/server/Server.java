package no.ssb.dc.server;

import ch.qos.logback.classic.ClassicConstants;
import net.bytebuddy.agent.ByteBuddyAgent;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dapla.secrets.api.SecretManagerClient;
import no.ssb.dc.api.security.ProvidedBusinessSSLResource;
import no.ssb.dc.application.server.UndertowApplication;
import no.ssb.dc.application.ssl.BusinessSSLResourceSupplier;
import no.ssb.dc.core.metrics.MetricsAgent;
import no.ssb.dc.core.util.JavaUtilLoggerBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import static no.ssb.dc.api.security.ProvidedBusinessSSLResource.safeConvertBytesToCharArrayAsUTF8;

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
         * The supplier is wrapped by the BusinessSSLResourceSupplier and passes an implementing instance of
         * BusinessSSLBundle to Worker.useBusinessSSLResourceSupplier() in the Core module.
         *
         * Please note: only Google Secret Manager is supported!
         */

        String businessSslResourceProvider = configuration.evaluateToString("data.collector.sslBundle.provider");
        boolean hasBusinessSslResourceProvider = "google-secret-manager".equals(businessSslResourceProvider);

        Supplier<ProvidedBusinessSSLResource> sslResourceSupplier = () -> {
            if (!hasBusinessSslResourceProvider) {
                return null;
            }
            LOG.info("Create BusinessSSL resource provider: {}", businessSslResourceProvider);
            Map<String, String> providerConfiguration = new LinkedHashMap<>();
            providerConfiguration.put("secrets.provider", businessSslResourceProvider);
            providerConfiguration.put("secrets.projectId", configuration.evaluateToString("data.collector.sslBundle.gcs.projectId"));
                String gcsServiceAccountKeyPath = configuration.evaluateToString("data.collector.sslBundle.gcs.serviceAccountKeyPath");
            if (gcsServiceAccountKeyPath != null) {
                providerConfiguration.put("secrets.serviceAccountKeyPath", gcsServiceAccountKeyPath);
            }

            try (SecretManagerClient secretManagerClient = SecretManagerClient.create(providerConfiguration)) {
                return new ProvidedBusinessSSLResource() {

                    @Override
                    public String getType() {
                        return configuration.evaluateToString("data.collector.sslBundle.type");
                    }

                    @Override
                    public String bundleName() {
                        return secretManagerClient.readString("data.collector.sslBundle.name");
                    }

                    @Override
                    public char[] publicCertificate() {
                        return isPEM() ? safeConvertBytesToCharArrayAsUTF8(secretManagerClient.readBytes("data.collector.sslBundle.publicCertificate")) : new char[0];
                    }

                    @Override
                    public char[] privateCertificate() {
                        return isPEM() ? safeConvertBytesToCharArrayAsUTF8(secretManagerClient.readBytes("data.collector.sslBundle.privateCertificate")) : new char[0];
                    }

                    @Override
                    public byte[] archiveCertificate() {
                        return !isPEM() ? secretManagerClient.readBytes("data.collector.sslBundle.archiveCertificate") : new byte[0];
                    }

                    @Override
                    public char[] passphrase() {
                        return safeConvertBytesToCharArrayAsUTF8(secretManagerClient.readBytes("data.collector.sslBundle.passphrase"));
                    }
                };
            }
        };

        UndertowApplication application = UndertowApplication.initializeUndertowApplication(configuration, hasBusinessSslResourceProvider ? new BusinessSSLResourceSupplier(sslResourceSupplier) : null);

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
