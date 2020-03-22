package no.ssb.dc.server.service;

import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.application.health.HealthApplicationMonitor;
import no.ssb.dc.application.health.HealthApplicationResource;
import no.ssb.dc.application.health.HealthConfigResource;
import no.ssb.dc.application.health.HealthResourceFactory;
import no.ssb.dc.application.metrics.MetricsResourceFactory;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerExtension;
import no.ssb.dc.test.server.TestServerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(TestServerExtension.class)
class WorkerServiceLifecycleTest {

    static final Logger LOG = LoggerFactory.getLogger(WorkerServiceLifecycleTest.class);

    static final Predicate<Task> isSpecificationRunning = task -> task.workerService.list().stream().anyMatch(job -> task.specificationBuilder.getId().equals(job.specificationId));

    static final Map<WorkerLifecycleCallback.Kind, AtomicInteger> lifecycleCounter = new ConcurrentHashMap<>();

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

        WorkerService workerService = new WorkerService(testServer.getConfiguration(), MetricsResourceFactory.create(), healthResourceFactory, true, WorkerServiceLifecycleTest::workerLifecycleCallback);

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

    static void workerLifecycleCallback(WorkerLifecycleCallback workerLifecycleCallback) {
        LOG.trace("[{}] - workerId: {} - specId: {} - workerListSize: {} - status: {}",
                workerLifecycleCallback.kind,
                workerLifecycleCallback.workerObservable.workerId(),
                workerLifecycleCallback.workerObservable.specificationId(),
                workerLifecycleCallback.workManager.list().size(),
                workerLifecycleCallback.workerStatus
        );

        lifecycleCounter.get(workerLifecycleCallback.kind).incrementAndGet();
    }

    @BeforeAll
    static void beforeAll() {
        List.of(WorkerLifecycleCallback.Kind.values()).forEach(key -> lifecycleCounter.computeIfAbsent(key, counter -> new AtomicInteger()));
    }

    @AfterAll
    static void afterAll() {
        Map.Entry<WorkerLifecycleCallback.Kind, AtomicInteger> lastCounter = null;
        for (Map.Entry<WorkerLifecycleCallback.Kind, AtomicInteger> counter : lifecycleCounter.entrySet()) {
            if (lastCounter == null) {
                lastCounter = counter;
                continue;
            }
            assertEquals(lastCounter.getValue().get(), counter.getValue().get(), counter.getKey().name());
        }
    }

    @Disabled
    @ParameterizedTest
    @MethodSource("specificationBuilderProvider")
    void testSpecificationWorkerLifecycle(Task task) throws InterruptedException {
        String workerId = task.workerService.createOrRejectTask(task.specificationBuilder);

        int n = 0;
        if (task.waitForCompletion) {
            while (isSpecificationRunning.test(task)) {
                if (n > 25) {
                    throw new RuntimeException("WorkerObserver.finish was not called! The worker has not been completed.");
                }
                TimeUnit.MILLISECONDS.sleep(250);
                LOG.trace("{}: Running... ON_START_BEFORE_TRY_LOCK: {}, ON_FINISH_AFTER_UNLOCK: {}", task.index,
                        lifecycleCounter.get(WorkerLifecycleCallback.Kind.ON_START_BEFORE_TRY_LOCK).get(),
                        lifecycleCounter.get(WorkerLifecycleCallback.Kind.ON_FINISH_AFTER_UNLOCK).get()
                );
                n++;
            }

            LOG.trace("Done");
        }
    }

    static class Task {
        static final AtomicInteger count = new AtomicInteger(0);
        final WorkerService workerService;
        final SpecificationBuilder specificationBuilder;
        final boolean waitForCompletion;
        final int index;

        Task(WorkerService workerService, SpecificationBuilder specificationBuilder, boolean waitForCompletion) {
            this.workerService = workerService;
            this.specificationBuilder = specificationBuilder;
            this.waitForCompletion = waitForCompletion;
            this.index = count.incrementAndGet();
        }
    }


}
