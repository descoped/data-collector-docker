package no.ssb.dc.server;

import net.bytebuddy.agent.ByteBuddyAgent;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.http.HttpStatus;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.core.metrics.MetricsAgent;
import no.ssb.dc.test.client.ResponseHelper;
import no.ssb.dc.test.client.TestClient;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;

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
public class DockerServerTest {

    private static final Logger LOG = LoggerFactory.getLogger(DockerServerTest.class);

    @Inject
    TestServer server;

    @Inject
    TestClient client;

    @BeforeAll
    static void beforeAll() {
        MetricsAgent.premain(null, ByteBuddyAgent.install());
    }

    @Test
    public void testPingServer() {
        client.get("/ping").expect200Ok();
    }

    @Test
    public void testMockServer() {
        client.get("/api/events").expect200Ok();
    }

    @Test
    public void testPutTask() throws InterruptedException {
        String spec = CommonUtils.readFileOrClasspathResource("worker.config/page-test.json").replace("PORT", Integer.valueOf(server.getTestServerServicePort()).toString());
        client.put("/tasks", spec).expect201Created();
        LOG.trace("list: {}", client.get("/tasks").expect200Ok().body());
        client.put("/tasks", spec).expectAnyOf(HttpStatus.HTTP_CONFLICT.code());
        Thread.sleep(3000);
    }

    @Test
    public void testHealth() {
        ResponseHelper<String> responseHelper = client.get("/health?config&contexts&threads").expect200Ok();
        System.out.printf("health:%n%s%n", responseHelper.body());
    }

    @Disabled
    @Test
    public void ReadmeExample() {
        SpecificationBuilder feedBuilder = Specification.start("", "", "loop")
                .configure(context()
                        .topic("topic")
                        .variable("nextPosition", "${contentStream.hasLastPosition() ? contentStream.lastPosition() : 1}")
                )
                .function(paginate("loop")
                        .variable("fromPosition", "${nextPosition}")
                        .addPageContent("fromPosition")
                        .iterate(execute("page"))
                        .prefetchThreshold(15)
                        .until(whenVariableIsNull("nextPosition"))
                )
                .function(get("page")
                        .url("http://example.com/feed?pos=${fromPosition}&size=10")
                        .validate(status().success(200, 299))
                        .pipe(sequence(xpath("/feed/entry"))
                                .expected(xpath("/entry/id"))
                        )
                        .pipe(nextPage()
                                .output("nextPosition", regex(xpath("/feed/link[@rel=\"next\"]/@href"), "(?<=[?&]pos=)[^&]*"))
                        )
                        .pipe(parallel(xpath("/feed/entry"))
                                .variable("position", xpath("/entry/id"))
                                .pipe(addContent("${position}", "entry"))
                                .pipe(publish("${position}"))
                        )
                        .returnVariables("nextPosition")
                );

        Worker.newBuilder()
                .configuration(Map.of(
                        "content.stream.connector", "rawdata",
                        "rawdata.client.provider", "memory")
                )
                .specification(feedBuilder)
                .printExecutionPlan()
                .printConfiguration()
                .build()
                .run();
    }
}
