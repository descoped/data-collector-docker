package no.ssb.dc.server.task;

import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.application.health.HealthResourceFactory;
import no.ssb.dc.application.metrics.MetricsResourceFactory;
import no.ssb.dc.server.content.ContentStoreComponent;
import no.ssb.dc.test.client.TestClient;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.function.BiFunction;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(TestServerExtension.class)
public class WorkerServiceTest {

    static final BiFunction<String, String, SpecificationBuilder> specificationBuilderSupplier = (baseURL, failAtQueryString) -> Specification.start("WORKER-TEST", "paginate mock service", "page-loop")
            .configure(context()
                    .topic("topic")
                    .header("accept", "application/xml")
                    .header("origin", "http://localhost")
                    .variable("baseURL", baseURL)
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
                    .url("${baseURL}/api/events?position=${fromPosition}&pageSize=10")
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
                    .url("${baseURL}/api/events/${eventId}?type=event" + failAtQueryString)
                    .pipe(addContent("${position}", "event-doc"))
            );

    @Inject
    TestClient client;

    @Inject
    TestServer testServer;

    @Test
    public void testWorkerService() throws InterruptedException {
        WorkerService workerService = new WorkerService(testServer.getConfiguration(), MetricsResourceFactory.create(), HealthResourceFactory.create(),
                ContentStoreComponent.create(testServer.getConfiguration()));

        SpecificationBuilder specificationBuilder = specificationBuilderSupplier.apply(testServer.testURL(""), "");
        String workerId = workerService.createOrRejectTask(specificationBuilder);
        workerService.createOrRejectTask(specificationBuilder);

        Thread.sleep(500);
        workerService.cancelTask(workerId);

        Thread.sleep(2000);
        assertTrue(workerService.list().isEmpty(), "Task list should be empty!");
    }

    @Test
    public void testWorkerServiceWithFailAt() throws InterruptedException {
        WorkerService workerService = new WorkerService(testServer.getConfiguration(), MetricsResourceFactory.create(), HealthResourceFactory.create(),
                ContentStoreComponent.create(testServer.getConfiguration()));

        SpecificationBuilder specificationBuilder = specificationBuilderSupplier.apply(testServer.testURL(""), "&failWithStatusCode=404&failAt=1005");
        try {
            String workerId = workerService.createOrRejectTask(specificationBuilder);

            Thread.sleep(3000);
        } finally {
            assertTrue(workerService.list().isEmpty(), "Task list should be empty!");
        }
    }

    @Test
    public void testServiceAlive() {
        client.get("/health/alive").expect200Ok();
    }

    @Test
    public void testServiceReady() {
        client.get("/health/ready").expect200Ok();
    }
}
