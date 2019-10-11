package no.ssb.dc.server.controller;

import io.undertow.server.HttpServerExchange;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.node.builder.FlowBuilder;
import no.ssb.dc.application.Controller;
import no.ssb.dc.server.service.WorkerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskController implements Controller {

    private static final Logger LOG = LoggerFactory.getLogger(TaskController.class);

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
            exchange.getRequestReceiver().receiveFullString((httpServerExchange, payload) -> {
                FlowBuilder flowBuilder = Specification.deserialize(payload);
                workerService.add(flowBuilder);

            });
            exchange.setStatusCode(201);
            return;
        }

        exchange.setStatusCode(400);
    }
}
