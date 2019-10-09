package no.ssb.dc.server;

import no.ssb.dc.test.client.TestClient;
import no.ssb.dc.test.server.TestServerListener;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

@Listeners(TestServerListener.class)
public class ServerTest {

    @Inject
    TestClient client;

    @Test
    public void testPingServer() {
        client.get("/ping").expect200Ok();
    }

    @Test
    public void testPutTask() {
        client.put("/task", "hello").expect201Created();
    }
}
