package no.ssb.dc.server;

import no.ssb.dc.api.application.Controller;
import no.ssb.dc.api.application.Service;
import org.testng.annotations.Test;

public class ServiceProviderDiscoveryTest {

    @Test
    public void testController() {
        Iterable<Class<Controller>> discovery = ServiceProviderDiscovery.discover(Controller.class);
        discovery.forEach(a -> System.out.println(a.getName()));
    }

    @Test
    public void testService() {
        Iterable<Class<Service>> discovery = ServiceProviderDiscovery.discover(Service.class);
        discovery.forEach(a -> System.out.println(a.getName()));
    }
}
