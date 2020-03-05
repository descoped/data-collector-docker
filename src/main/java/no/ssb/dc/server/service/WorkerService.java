package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.application.health.HealthResourceFactory;
import no.ssb.dc.application.metrics.MetricsResourceFactory;
import no.ssb.dc.application.spi.Service;
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
    private final MetricsResourceFactory metricsResourceFactory;
    private final HealthResourceFactory healthResourceFactory;
    private final WorkManager workManager = new WorkManager();

    public WorkerService(DynamicConfiguration configuration, MetricsResourceFactory metricsResourceFactory, HealthResourceFactory healthResourceFactory) {
        this.configuration = configuration;
        this.metricsResourceFactory = metricsResourceFactory;
        this.healthResourceFactory = healthResourceFactory;
    }

    private void onWorkerStart(WorkerObservable observable) {
        workManager.lock(observable.specificationId());
        try {
            LOG.info("Start worker: {}", observable.workerId());
            HealthWorkerResource healthWorkerResource = healthResourceFactory.createAndAdddHealthResource(observable.workerId(), HealthWorkerResource.class);
            observable.context().services().register(HealthWorkerMonitor.class, healthWorkerResource.getMonitor());
        } finally {
            workManager.unlock(observable.specificationId());
        }
    }

    private void onWorkerFinish(WorkerObservable observable, WorkerStatus status) {
        workManager.lock(observable.specificationId());
        try {
            if (status == WorkerStatus.COMPLETED) {
                LOG.info("Completed worker: [{}] {}", status, observable.workerId());
            } else {
                LOG.error("Completed worker: [{}] {}", status, observable.workerId());
            }
            workManager.remove(observable.workerId());
            HealthWorkerResource healthWorkerResource = healthResourceFactory.getHealthResource(observable.workerId());
            healthResourceFactory.removeHealthResource(observable.workerId());
            healthResourceFactory.getHealthResource(HealthWorkerHistoryResource.class).add(healthWorkerResource);
        } finally {
            workManager.unlock(observable.specificationId());
        }
    }

    public String createOrRejectTask(SpecificationBuilder specificationBuilder) {
        if ("".equals(specificationBuilder.getId())) {
            LOG.warn("The specification id is empty!");
            return null;
        }

        workManager.lock(specificationBuilder.getId());
        try {
            if (workManager.isRunning(specificationBuilder.getId())) {
                LOG.warn("The specification '{}' is already running!", specificationBuilder.getId());
                return null;
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

            return workManager.run(workerBuilder).workerId.toString();
        } finally {
            workManager.unlock(specificationBuilder.getId());
        }
    }

    public List<WorkManager.Task> list() {
        return workManager.list();
    }

    // TODO lock prevents onWorkerFinish to complete
    public boolean cancelTask(String workerId) {
        WorkManager.JobId jobId = workManager.get(UUID.fromString(workerId));
        if (jobId == null) {
            LOG.warn("Worker '{}' NOT found! Maybe it was already completed.", workerId);
            return false;
        }
        workManager.lock(jobId.specificationId);
        try {
            return workManager.cancel(UUID.fromString(workerId));
        } finally {
            workManager.unlock(jobId.specificationId);
        }
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
