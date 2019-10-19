package no.ssb.dc.server;

import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.server.service.WorkerService;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerListener;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

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

@Listeners(TestServerListener.class)
public class WorkerServiceTest {

    @Inject
    TestServer testServer;

    @Test
    public void testWorkerService() throws InterruptedException {
        WorkerService workerService = new WorkerService(testServer.getConfiguration());

        SpecificationBuilder specificationBuilder = Specification.start("paginate mock service", "page-loop")
                .configure(context()
                        .topic("topic")
                        .header("accept", "application/xml")
                        .variable("baseURL", testServer.testURL(""))
                        .variable("nextPosition", "${contentStream.lastOrInitialPosition(1)}")
                )
                .function(paginate("page-loop")
                        .variable("fromPosition", "${nextPosition}")
                        .addPageContent()
                        .iterate(execute("page"))
                        .prefetchThreshold(5)
                        .until(whenVariableIsNull("nextPosition"))
                )
                .function(get("page")
                        .url("${baseURL}/mock?seq=${fromPosition}&size=10")
                        .validate(status().success(200, 299).fail(300, 599))
                        .pipe(sequence(xpath("/feed/entry"))
                                .expected(xpath("/entry/id"))
                        )
                        .pipe(nextPage()
                                        .output("nextPosition", regex(xpath("/feed/link[@rel=\"next\"]/@href"), "(?<=[?&]seq=)[^&]*"))
                                //.output("nextPosition", eval(xpath("/feed/entry[last()]/id"), "result", "${cast.toLong(result) + 1}"))
                        )
                        .pipe(parallel(xpath("/feed/entry"))
                                .variable("position", xpath("/entry/id"))
                                .pipe(addContent("${position}", "entry"))
                                .pipe(execute("event-doc")
                                        .inputVariable("eventId", xpath("/entry/event/event-id"))
                                )
                                .pipe(publish("${position}"))
                        )
                        .returnVariables("nextPosition")
                )
                .function(get("event-doc")
                        .url("${baseURL}/mock/${eventId}?type=event")
                        .pipe(addContent("${position}", "event-doc"))
                );
        ;
        workerService.add(specificationBuilder);

        workerService.start();

        Thread.sleep(2000);
    }
}
