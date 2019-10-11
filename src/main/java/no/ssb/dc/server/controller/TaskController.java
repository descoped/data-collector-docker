package no.ssb.dc.server.controller;

import io.undertow.server.HttpServerExchange;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.FlowBuilder;
import no.ssb.dc.application.Controller;
import no.ssb.dc.server.service.WorkerService;

import java.nio.charset.StandardCharsets;

public class TaskController implements Controller {

    private final WorkerService workerService;

    public TaskController(WorkerService workerService) {
        this.workerService = workerService;
    }

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

        if ("put".equalsIgnoreCase(exchange.getRequestMethod().toString())) {
            exchange.getRequestReceiver().receiveFullBytes((httpServerExchange, bytes) -> {
                String payload = new String(bytes, StandardCharsets.UTF_8);
                FlowBuilder flowBuilder = Specification.deserialize(payload, FlowBuilder.class);
                workerService.add(flowBuilder);

            });
            exchange.setStatusCode(201);
            return;
        }

        exchange.setStatusCode(400);
    }
}
