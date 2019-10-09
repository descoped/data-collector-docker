package no.ssb.dc.server.controller;

import io.undertow.server.HttpServerExchange;
import no.ssb.dc.application.Controller;

public class TaskController implements Controller {

    @Override
    public String contextPath() {
        return "/task";
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }
        exchange.setStatusCode(201);
    }
}
