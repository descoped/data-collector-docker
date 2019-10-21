package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.application.Service;
import no.ssb.dc.application.health.HealthResourceFactory;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.core.executor.WorkerObserver;
import no.ssb.dc.core.executor.WorkerOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerService.class);

    private final DynamicConfiguration configuration;
    private final HealthResourceFactory healthResourceFactory;
    private final Map<UUID, CompletableFuture<ExecutionContext>> jobs = new ConcurrentHashMap<>(); // todo make future housekeeping thread that removes crashed jobs
    private final JobManager jobManager = new JobManager();

    public WorkerService(DynamicConfiguration configuration, HealthResourceFactory healthResourceFactory) {
        this.configuration = configuration;
        this.healthResourceFactory = healthResourceFactory;
    }

    private void onWorkerStart(UUID workerId) {
        LOG.info("Start job: {}", workerId);
    }

    private void onWorkerFinish(UUID workerId, WorkerOutcome outcome) {
        LOG.info("Completed job: [{}] {}", outcome, workerId);
        jobManager.removeJob(workerId);
    }

    public void execute(SpecificationBuilder specificationBuilder) {
        if (jobManager.isActive(specificationBuilder)) {
            LOG.warn("The specification named '{}' is already running!", specificationBuilder.name());
            return;
        }

        Worker.WorkerBuilder workerBuilder = Worker.newBuilder()
                .configuration(configuration.asMap())
                .workerObserver(new WorkerObserver(this::onWorkerStart, this::onWorkerFinish))
                .specification(specificationBuilder)
                .printConfiguration()
                .printExecutionPlan();

        if (configuration.evaluateToString("data.collector.certs.directory") != null) {
            workerBuilder.buildCertificateFactory(Paths.get(configuration.evaluateToString("data.collector.certs.directory")));
        } else {
            workerBuilder.buildCertificateFactory(CommonUtils.currentPath());
        }

        jobManager.runWorker(workerBuilder);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        CompletableFuture.allOf(jobs.values().toArray(new CompletableFuture[0]))
                .cancel(true);
    }
}
