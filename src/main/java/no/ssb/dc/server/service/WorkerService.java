package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
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
import no.ssb.service.provider.api.ProviderConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class WorkerService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerService.class);

    private final DynamicConfiguration configuration;
    private final MetricsResourceFactory metricsResourceFactory;
    private final HealthResourceFactory healthResourceFactory;
    private final WorkManager workManager = new WorkManager();
    private final boolean printExecutionPlan;
    private final WorkerObserver workerObserver;
    private final Consumer<WorkerLifecycleCallback> workerLifecycleCallback;
    private final ContentStore contentStore;

    public WorkerService(DynamicConfiguration configuration, MetricsResourceFactory metricsResourceFactory, HealthResourceFactory healthResourceFactory) {
        this(configuration, metricsResourceFactory, healthResourceFactory, configuration.evaluateToBoolean("data.collector.print-execution-plan"), null);
    }

    public WorkerService(DynamicConfiguration configuration, MetricsResourceFactory metricsResourceFactory, HealthResourceFactory healthResourceFactory,
                         boolean printExecutionPlan, Consumer<WorkerLifecycleCallback> workerLifecycleCallback) {
        this.configuration = configuration;
        this.metricsResourceFactory = metricsResourceFactory;
        this.healthResourceFactory = healthResourceFactory;
        this.printExecutionPlan = printExecutionPlan;
        this.workerLifecycleCallback = workerLifecycleCallback;
        this.workerObserver = new WorkerObserver(this::onWorkerStart, this::onWorkerFinish);
        this.contentStore = ProviderConfigurator.configure(configuration.asMap(), configuration.evaluateToString("content.stream.connector"), ContentStoreInitializer.class);
    }

    void onWorkerStart(WorkerObservable observable) {
        Optional<Consumer<WorkerLifecycleCallback>> workerLifecycleConsumer = Optional.ofNullable(this.workerLifecycleCallback);
        AtomicReference<WorkerStatus> workerStatus = new AtomicReference<>(WorkerStatus.RUNNING);

        workerLifecycleConsumer.ifPresent(callback -> callback.accept(new WorkerLifecycleCallback(WorkerLifecycleCallback.Kind.ON_START_BEFORE_TRY_LOCK, workManager, observable, workerStatus.get())));
        workManager.lock(observable.specificationId());
        workerLifecycleConsumer.ifPresent(callback -> callback.accept(new WorkerLifecycleCallback(WorkerLifecycleCallback.Kind.ON_START_AFTER_TRY_LOCK, workManager, observable, workerStatus.get())));
        try {
            LOG.info("Start worker: {}", observable.workerId());
            HealthWorkerResource healthWorkerResource = healthResourceFactory.createAndAdddHealthResource(observable.workerId(), HealthWorkerResource.class);
            observable.context().services().register(HealthWorkerMonitor.class, healthWorkerResource.getMonitor());
            workerStatus.set(healthWorkerResource.getMonitor().status());
        } finally {
            workerLifecycleConsumer.ifPresent(callback -> callback.accept(new WorkerLifecycleCallback(WorkerLifecycleCallback.Kind.ON_START_BEFORE_UNLOCK, workManager, observable, workerStatus.get())));
            workManager.unlock(observable.specificationId());
            workerLifecycleConsumer.ifPresent(callback -> callback.accept(new WorkerLifecycleCallback(WorkerLifecycleCallback.Kind.ON_START_AFTER_UNLOCK, workManager, observable, workerStatus.get())));
        }
    }

    void onWorkerFinish(WorkerObservable observable, WorkerStatus status) {
        Optional<Consumer<WorkerLifecycleCallback>> workerLifecycleConsumer = Optional.ofNullable(this.workerLifecycleCallback);

        workerLifecycleConsumer.ifPresent(callback -> callback.accept(new WorkerLifecycleCallback(WorkerLifecycleCallback.Kind.ON_FINISH_BEFORE_TRY_LOCK, workManager, observable, status)));
        workManager.lock(observable.specificationId());
        workerLifecycleConsumer.ifPresent(callback -> callback.accept(new WorkerLifecycleCallback(WorkerLifecycleCallback.Kind.ON_FINISH_AFTER_TRY_LOCK, workManager, observable, status)));
        try {
            if (status == WorkerStatus.COMPLETED) {
                LOG.info("Completed worker: [{}] {}", status, observable.workerId());
            } else {
                LOG.error("Completed worker: [{}] {}", status, observable.workerId());
            }
            workerLifecycleConsumer.ifPresent(callback -> callback.accept(new WorkerLifecycleCallback(WorkerLifecycleCallback.Kind.ON_FINISH_BEFORE_REMOVE_WORKER, workManager, observable, status)));
            workManager.remove(observable.workerId());
            workerLifecycleConsumer.ifPresent(callback -> callback.accept(new WorkerLifecycleCallback(WorkerLifecycleCallback.Kind.ON_FINISH_AFTER_REMOVE_WORKER, workManager, observable, status)));

            HealthWorkerResource healthWorkerResource = healthResourceFactory.getHealthResource(observable.workerId());
            healthResourceFactory.removeHealthResource(observable.workerId());
            healthResourceFactory.getHealthResource(HealthWorkerHistoryResource.class).add(healthWorkerResource);
        } finally {
            workerLifecycleConsumer.ifPresent(callback -> callback.accept(new WorkerLifecycleCallback(WorkerLifecycleCallback.Kind.ON_FINISH_BEFORE_UNLOCK, workManager, observable, status)));
            workManager.unlock(observable.specificationId());
            workerLifecycleConsumer.ifPresent(callback -> callback.accept(new WorkerLifecycleCallback(WorkerLifecycleCallback.Kind.ON_FINISH_AFTER_UNLOCK, workManager, observable, status)));
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
                    .workerObserver(workerObserver)
                    .specification(specificationBuilder)
                    .contentStore(contentStore)
                    .keepContentStoreOpenOnWorkerCompletion(true);

            if (printExecutionPlan) {
                workerBuilder
                        .printConfiguration()
                        .printExecutionPlan();
            }

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
        try {
            workManager.cancel();
            LOG.info("Closing ContentStore!");
            contentStore.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
