package no.ssb.dc.server;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.application.UndertowApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class Server {

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        long now = System.currentTimeMillis();

        LOG.info("WorkDir: {}", Paths.get("").toAbsolutePath());

        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application-defaults.properties")
                .propertiesResource("/conf/application-defaults.properties")
                .propertiesResource("/conf/application.properties")
                .environment("DC_")
                .systemProperties()
                .build();

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
}
