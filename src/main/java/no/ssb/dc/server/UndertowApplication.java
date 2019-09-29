package no.ssb.dc.server;

import io.undertow.Undertow;
import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.application.Application;
import no.ssb.dc.api.application.Controller;
import no.ssb.dc.api.application.Service;
import no.ssb.dc.api.services.InjectionParameters;
import no.ssb.dc.api.services.ObjectCreator;
import no.ssb.dc.server.controller.NamespaceController;
import no.ssb.dc.server.service.RawdataFileSystemService;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class UndertowApplication implements Application {

    private static final Logger LOG = LoggerFactory.getLogger(UndertowApplication.class);

    public static <T, R> UndertowApplication initializeUndertowApplication(DynamicConfiguration configuration) {
        int port = configuration.evaluateToInt("http.port");
        return initializeUndertowApplication(configuration, port);
    }

    public static <T, R> UndertowApplication initializeUndertowApplication(DynamicConfiguration configuration, Integer port) {
        LOG.info("Initializing Data Collector server ...");

        String host = configuration.evaluateToString("http.host");

        InjectionParameters serviceInjectionParameters = new InjectionParameters();
        serviceInjectionParameters.register(DynamicConfiguration.class, configuration);
        RawdataClient rawdataClient = ProviderConfigurator.configure(configuration.asMap(), configuration.evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class);
        serviceInjectionParameters.register(RawdataClient.class, rawdataClient);

        InjectionParameters controllerInjectionParameters = new InjectionParameters();
        controllerInjectionParameters.putAll(serviceInjectionParameters);

        Map<Class<? extends Service>, Service> services = new LinkedHashMap<>();
        for(Class<Service> serviceClass : ServiceProviderDiscovery.discover(Service.class)) {
            Service service = ObjectCreator.newInstance(serviceClass, serviceInjectionParameters);
            controllerInjectionParameters.register(serviceClass, service);
            services.put(serviceClass, service);
        }

        NavigableMap<String, Controller> controllers = new TreeMap<>();
        for(Class<Controller> controllerClass : ServiceProviderDiscovery.discover(Controller.class)) {
            Controller controller = ObjectCreator.newInstance(controllerClass, controllerInjectionParameters);
            controllers.put(controller.contextPath(), controller);
        }

        NamespaceController namespaceController = new NamespaceController(
                configuration.evaluateToString("namespace.default"),
                configuration.evaluateToString("http.cors.allow.origin"),
                configuration.evaluateToString("http.cors.allow.header"),
                configuration.evaluateToBoolean("http.cors.allow.origin.test"),
                port,
                controllers
        );

        return new UndertowApplication(configuration, host, port, namespaceController, services);
    }

    private final Undertow server;
    private final DynamicConfiguration configuration;
    private final String host;
    private final int port;
    private final NamespaceController namespaceController;
    private final Map<Class<? extends Service>, Service> services;

    <T, R> UndertowApplication(DynamicConfiguration configuration, String host, int port, NamespaceController namespaceController, Map<Class<? extends Service>, Service> services) {
        this.configuration = configuration;
        this.host = host;
        this.port = port;
        this.namespaceController = namespaceController;
        this.services = services;
        NamespaceController handler = namespaceController;
        Undertow server = Undertow.builder()
                .addHttpListener(port, host)
                .setHandler(handler)
                .build();
        this.server = server;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void start() {
        if (configuration.evaluateToBoolean("data.collector.rawdata.dump.enabled")) {
            unwrap(RawdataFileSystemService.class).start();
        }
        server.start();
        LOG.info("Started Data Collector. PID {}", ProcessHandle.current().pid());
        LOG.info("Listening on {}:{}", host, port);
    }

    @Override
    public void stop() {
        server.stop();
        for(Service service : services.values()) {
            service.stop();
        }
        LOG.info("Leaving.. Bye!");
    }

    @Override
    public void enableAllServices() {
        for(Service service : services.values()) {
            service.start();
        }
    }

    @Override
    public void enable(Class<? extends Service> service) {
        services.get(service).start();
    }

    @Override
    public void disable(Class<? extends Service> service) {
        services.get(service).stop();
    }

    @Override
    public <R> R unwrap(Class<R> clazz) {
        if (clazz.isAssignableFrom(server.getClass())) {
            return (R) server;
        } else {
            if (services.containsKey(clazz)) {
                return (R) services.get(clazz);
            }
            throw new UnsupportedOperationException("Unable to unwrap class: " + clazz);
        }
    }

}
