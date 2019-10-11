package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.builder.FlowBuilder;
import no.ssb.dc.api.ulid.ULIDGenerator;
import no.ssb.dc.api.ulid.ULIDStateHolder;
import no.ssb.dc.application.Service;
import no.ssb.dc.core.executor.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerService implements Service {

    private final static Logger LOG = LoggerFactory.getLogger(WorkerService.class);

    private final ULIDStateHolder ulidStateHolder = new ULIDStateHolder();
    private final DynamicConfiguration configuration;
    private final Map<UUID, CompletableFuture<ExecutionContext>> jobs = new ConcurrentHashMap<>(); // todo make future housekeeping thread that removes crashed jobs

    public WorkerService(DynamicConfiguration configuration) {
        this.configuration = configuration;
    }

    public void add(FlowBuilder flowBuilder) {
        Worker.WorkerBuilder workerBuilder = Worker.newBuilder()
                .configuration(configuration.asMap())
                .initialPositionVariable("startPosition")
                .initialPosition("1")
                .specification(flowBuilder)
                .printConfiguration();

        if (configuration.evaluateToString("data.collector.certs.directory") != null) {
            workerBuilder.buildCertificateFactory(Paths.get(configuration.evaluateToString("data.collector.certs.directory")));
        }

        UUID jobId = ULIDGenerator.toUUID(ULIDGenerator.nextMonotonicUlid(ulidStateHolder));
        LOG.info("Start job: {}", jobId);
        CompletableFuture<ExecutionContext> future = workerBuilder.build().runAsync();
        future.thenAccept(context -> {
            LOG.info("Completed job: {}", jobId);
           jobs.remove(jobId);
        });
        jobs.put(jobId, future);
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
