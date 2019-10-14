package no.ssb.dc.server;

import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.test.client.TestClient;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerListener;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

@Listeners(TestServerListener.class)
public class DockerServerTest {

    @Inject
    TestServer server;

    @Inject
    TestClient client;

    @Test
    public void testPingServer() {
        client.get("/ping").expect200Ok();
    }

   @Test
    public void testMockServer() {
        client.get("/mock").expect200Ok();
    }

    @Test
    public void testPutTask() throws InterruptedException {
        String spec = CommonUtils.readFileOrClasspathResource("worker.config/page-test.json").replace("PORT", Integer.valueOf(server.getTestServerServicePort()).toString());
        client.put("/task", spec).expect201Created();
        Thread.sleep(3000);
    }

}
