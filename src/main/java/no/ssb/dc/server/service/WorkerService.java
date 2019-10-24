package no.ssb.dc.server.service;

import com.fasterxml.jackson.databind.node.ArrayNode;
import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.application.Service;
import no.ssb.dc.application.health.HealthResourceFactory;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.core.executor.WorkerObservable;
import no.ssb.dc.core.executor.WorkerObserver;
import no.ssb.dc.core.executor.WorkerStatus;
import no.ssb.dc.core.health.HealthWorkerHistoryResource;
import no.ssb.dc.core.health.HealthWorkerMonitor;
import no.ssb.dc.core.health.HealthWorkerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public class WorkerService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerService.class);

    private final DynamicConfiguration configuration;
    private final HealthResourceFactory healthResourceFactory;
    private final WorkManager workManager = new WorkManager();

    public WorkerService(DynamicConfiguration configuration, HealthResourceFactory healthResourceFactory) {
        this.configuration = configuration;
        this.healthResourceFactory = healthResourceFactory;
    }

    private void onWorkerStart(WorkerObservable observable) {
        LOG.info("Start job: {}", observable.workerId());
        HealthWorkerResource healthWorkerResource = healthResourceFactory.addHealthResource(observable.workerId(), HealthWorkerResource.class);
        observable.context().services().register(HealthWorkerMonitor.class, healthWorkerResource.getMonitor());
    }

    private void onWorkerFinish(WorkerObservable observable, WorkerStatus outcome) {
        LOG.info("Completed job: [{}] {}", outcome, observable.workerId());
        workManager.remove(observable.workerId());
        HealthWorkerResource healthWorkerResource = healthResourceFactory.getHealthResource(observable.workerId());
        healthResourceFactory.removeHealthResource(observable.workerId());
        healthResourceFactory.getHealthResource(HealthWorkerHistoryResource.class).add(healthWorkerResource);
    }

    public boolean createOrRejectTask(SpecificationBuilder specificationBuilder) {
        if (workManager.isRunning(specificationBuilder)) {
            LOG.warn("The specification named '{}' is already running!", specificationBuilder.getName());
            return false;
        }

        Worker.WorkerBuilder workerBuilder = Worker.newBuilder()
                .configuration(configuration.asMap())
                .workerObserver(new WorkerObserver(this::onWorkerStart, this::onWorkerFinish))
                .specification(specificationBuilder)
                .printConfiguration()
                .printExecutionPlan();

        String configuredCertBundlesPath = configuration.evaluateToString("data.collector.certs.directory");
        Path certBundlesPath = configuredCertBundlesPath == null ? CommonUtils.currentPath() : Paths.get(configuredCertBundlesPath);
        workerBuilder.buildCertificateFactory(certBundlesPath);

        workManager.run(workerBuilder);

        return true;
    }

    public String list() {
        List<UUID> workers = workManager.list();
        JsonParser jsonParser = JsonParser.createJsonParser();
        ArrayNode rootNode = jsonParser.createArrayNode();
        workers.forEach(id -> rootNode.add(id.toString()));
        return jsonParser.toJSON(rootNode);
    }

    public boolean cancelTask(String workerId) {
        return workManager.cancel(UUID.fromString(workerId));
    }

    @Override
    public void start() {
        // nop
    }

    @Override
    public void stop() {
        workManager.cancel();
    }
}
