package no.ssb.dc.server;

import ch.qos.logback.classic.ClassicConstants;
import net.bytebuddy.agent.ByteBuddyAgent;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dapla.secrets.api.SecretManagerClient;
import no.ssb.dc.api.config.BusinessSSLBundle;
import no.ssb.dc.api.config.ContentStreamEncryptionCredentials;
import no.ssb.dc.application.server.UndertowApplication;
import no.ssb.dc.core.metrics.MetricsAgent;
import no.ssb.dc.core.util.JavaUtilLoggerBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
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


        Supplier<ContentStreamEncryptionCredentials> contentEncryptionCredentialsSupplier = () -> {
            if (true/*check IF NOT use delegated encryption credentials config*/) {
                return null;
            }
            // TODO config mapping
            Map<String, String> providerConfiguration = Map.of(
                    "secrets.provider", "google-secret-manager",
                    "secrets.projectId", "ssb-team-dapla",
                    "secrets.serviceAccountKeyPath", "FULL_PATH_TO_SERVICE_ACCOUNT.json"
            );
            try (SecretManagerClient secretManagerClient = SecretManagerClient.create(providerConfiguration)) {
                return new ContentStreamEncryptionCredentials() {
                    @Override
                    public char[] key() {
                        final byte[] encryptionKey = secretManagerClient.readBytes("secrets.encryptionKey");
                        try {
                            return ByteBuffer.wrap(encryptionKey).asCharBuffer().array();
                        } finally {
                            Arrays.fill(encryptionKey, (byte) 0);
                        }
                    }

                    @Override
                    public byte[] salt() {
                        final byte[] encryptionSalt = secretManagerClient.readBytes("secrets.encryptionSalt");
                        try {
                            return encryptionSalt;
                        } finally {
                            Arrays.fill(encryptionSalt, (byte) 0);
                        }
                    }
                };
            }
        };

        Supplier<BusinessSSLBundle> sslBundleSupplier = () -> {
            if (true/*check IF NOT use delegated sslBundle config*/) {
                return null;
            }
            // TODO config mapping
            Map<String, String> providerConfiguration = Map.of(
                    "secrets.provider", "google-secret-manager",
                    "secrets.projectId", "ssb-team-dapla",
                    "secrets.serviceAccountKeyPath", "FULL_PATH_TO_SERVICE_ACCOUNT.json"
            );
            try (SecretManagerClient secretManagerClient = SecretManagerClient.create(providerConfiguration)) {
                return new BusinessSSLBundle() {
                    @Override
                    public byte[] publicCertificate() {
                        return new byte[0];
                    }

                    @Override
                    public byte[] privateCertificate() {
                        return new byte[0];
                    }

                    @Override
                    public byte[] passphrase() {
                        return new byte[0];
                    }
                };
            }
        };

        UndertowApplication application = UndertowApplication.initializeUndertowApplication(configuration, contentEncryptionCredentialsSupplier, sslBundleSupplier);

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
