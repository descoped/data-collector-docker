package no.ssb.dc.server.service;

import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.application.health.HealthApplicationMonitor;
import no.ssb.dc.application.health.HealthApplicationResource;
import no.ssb.dc.application.health.HealthConfigResource;
import no.ssb.dc.application.health.HealthResourceFactory;
import no.ssb.dc.application.metrics.MetricsResourceFactory;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.core.executor.WorkerObservable;
import no.ssb.dc.core.executor.WorkerObserver;
import no.ssb.dc.core.executor.WorkerStatus;
import no.ssb.dc.core.health.HealthWorkerHistoryResource;
import no.ssb.dc.core.health.HealthWorkerMonitor;
import no.ssb.dc.core.health.HealthWorkerResource;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerExtension;
import no.ssb.dc.test.server.TestServerFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static no.ssb.dc.api.Builders.addContent;
import static no.ssb.dc.api.Builders.context;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.nextPage;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.parallel;
import static no.ssb.dc.api.Builders.publish;
import static no.ssb.dc.api.Builders.regex;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.status;
import static no.ssb.dc.api.Builders.whenVariableIsNull;
import static no.ssb.dc.api.Builders.xpath;

@ExtendWith(TestServerExtension.class)
class WorkerServiceLifecycleTest {

    static final Logger LOG = LoggerFactory.getLogger(WorkerServiceLifecycleTest.class);

    static final Predicate<Task> isSpecificationRunning = task -> task.workerService.list().stream().anyMatch(job -> task.specificationBuilder.getId().equals(job.specificationId));

    static SpecificationBuilder createSpecificationBuilder(TestServer testServer, String listQueryString, String eventQueryString) {
        return Specification.start("WORKER-TEST", "paginate mock service", "page-loop")
                .configure(context()
                        .topic("topic")
                        .header("accept", "application/xml")
                        .header("origin", "http://localhost")
                        .variable("baseURL", testServer.testURL(""))
                        .variable("nextPosition", "${contentStream.lastOrInitialPosition(1)}")
                )
                .function(paginate("page-loop")
                        .variable("fromPosition", "${nextPosition}")
                        .addPageContent("fromPosition")
                        .iterate(execute("page"))
                        .prefetchThreshold(5)
                        .until(whenVariableIsNull("nextPosition"))
                )
                .function(get("page")
                        .url("${baseURL}/api/events?position=${fromPosition}&pageSize=10" + listQueryString)
                        .validate(status().success(200, 299).fail(300, 599))
                        .pipe(sequence(xpath("/feed/entry"))
                                .expected(xpath("/entry/id"))
                        )
                        .pipe(nextPage()
                                .output("nextPosition", regex(xpath("/feed/link[@rel=\"next\"]/@href"), "(?<=[?&]position=)[^&]*"))
                        )
                        .pipe(parallel(xpath("/feed/entry"))
                                .variable("position", xpath("/entry/id"))
                                .pipe(addContent("${position}", "entry"))
                                .pipe(execute("event-doc")
                                        .inputVariable("eventId", xpath("/entry/event-id"))
                                )
                                .pipe(publish("${position}"))
                        )
                        .returnVariables("nextPosition")
                )
                .function(get("event-doc")
                        .url("${baseURL}/api/events/${eventId}" + eventQueryString)
                        .pipe(addContent("${position}", "event-doc"))
                );
    }

    static Stream<Task> specificationBuilderProvider() {
        TestServer testServer = TestServerFactory.instance().currentServer();

        HealthResourceFactory healthResourceFactory = HealthResourceFactory.create();
        healthResourceFactory.getHealthResource(HealthConfigResource.class).setConfiguration(testServer.getConfiguration().asMap());
        HealthApplicationMonitor applicationMonitor = healthResourceFactory.getHealthResource(HealthApplicationResource.class).getMonitor();
        applicationMonitor.setServerStatus(HealthApplicationMonitor.ServerStatus.RUNNING);

        TestWorkerService workerService = new TestWorkerService(testServer.getConfiguration(), MetricsResourceFactory.create(), healthResourceFactory);

        if (false) {
            return Stream.of(new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true));
        }

        return Stream.of(
                new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), false)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), false)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true) // hangs here
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", "?failWithStatusCode=404&failAt=1105"), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
                , new Task(workerService, createSpecificationBuilder(testServer, "&stopAt=1000", ""), true)
        );
    }

//    @Disabled
    @ParameterizedTest
    @MethodSource("specificationBuilderProvider")
    void testSpecificationWorkerLifecycle(Task task) throws InterruptedException {
        String workerId = task.workerService.createOrRejectTask(task.specificationBuilder);

        if (task.waitForCompletion) {
            while (isSpecificationRunning.test(task)) {
                TimeUnit.MILLISECONDS.sleep(250);
                LOG.trace("{}: Running...", task.index);
            }
            if (task.workerService.list().isEmpty()) {
                System.exit(1);
            }
            LOG.trace("Done");
        }
    }

    static class Task {
        static final AtomicInteger count = new AtomicInteger(0);
        final TestWorkerService workerService;
        final SpecificationBuilder specificationBuilder;
        final boolean waitForCompletion;
        final int index;

        Task(TestWorkerService workerService, SpecificationBuilder specificationBuilder, boolean waitForCompletion) {
            this.workerService = workerService;
            this.specificationBuilder = specificationBuilder;
            this.waitForCompletion = waitForCompletion;
            this.index = count.incrementAndGet();
        }
    }

    static class TestWorkerService {

        private final DynamicConfiguration configuration;
        private final MetricsResourceFactory metricsResourceFactory;
        private final HealthResourceFactory healthResourceFactory;
        private final WorkManager workManager = new WorkManager();

        TestWorkerService(DynamicConfiguration configuration, MetricsResourceFactory metricsResourceFactory, HealthResourceFactory healthResourceFactory) {
            this.configuration = configuration;
            this.metricsResourceFactory = metricsResourceFactory;
            this.healthResourceFactory = healthResourceFactory;
        }

        String createOrRejectTask(SpecificationBuilder specificationBuilder) {
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
                        .specification(specificationBuilder);

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

        void onWorkerStart(WorkerObservable observable) {
            workManager.lock(observable.specificationId());
            try {
                LOG.info("Start worker: {}", observable.workerId());
                HealthWorkerResource healthWorkerResource = healthResourceFactory.createAndAdddHealthResource(observable.workerId(), HealthWorkerResource.class);
                observable.context().services().register(HealthWorkerMonitor.class, healthWorkerResource.getMonitor());
            } finally {
                workManager.unlock(observable.specificationId());
            }

        }

        void onWorkerFinish(WorkerObservable observable, WorkerStatus status) {
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
            } catch (Exception e) {
                LOG.error("------------------------------------------------------------------------------------------------------------------------------ {}", CommonUtils.captureStackTrace(e));
                //System.exit(-1);
                throw e;
            } finally {
                workManager.unlock(observable.specificationId());
            }

        }
    }

}
