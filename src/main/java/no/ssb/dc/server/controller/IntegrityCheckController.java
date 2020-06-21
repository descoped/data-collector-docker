package no.ssb.dc.server.controller;

import io.undertow.server.HttpServerExchange;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.application.spi.Controller;

import java.util.Set;

public class IntegrityCheckController implements Controller {

    public IntegrityCheckController() {
    }

    @Override
    public String contextPath() {
        return "/integrity";
    }

    @Override
    public Set<Request.Method> allowedMethods() {
        return Set.of(Request.Method.GET);
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {

    }
}
