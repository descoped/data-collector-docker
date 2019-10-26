package no.ssb.dc.server.controller;

import com.fasterxml.jackson.databind.node.ArrayNode;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.application.Controller;
import no.ssb.dc.server.service.WorkerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class TasksController implements Controller {

    private static final Logger LOG = LoggerFactory.getLogger(TasksController.class);

    private final WorkerService workerService;

    public TasksController(WorkerService workerService) {
        this.workerService = workerService;
    }

    @Override
    public String contextPath() {
        return "/tasks";
    }

    @Override
    public Set<Request.Method> allowedMethods() {
        return Set.of(Request.Method.GET);
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        if ("get".equalsIgnoreCase(exchange.getRequestMethod().toString())) {
            if ("/tasks".equals(exchange.getRequestPath())) {
                getTaskList(exchange);
                return;
            }
        }

        exchange.setStatusCode(400);
    }

    private void getTaskList(HttpServerExchange exchange) {
        List<String> tasks = workerService.list();

        JsonParser jsonParser = JsonParser.createJsonParser();
        ArrayNode rootNode = jsonParser.createArrayNode();
        tasks.forEach(rootNode::add);
        String responseBody = jsonParser.toJSON(rootNode);

        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send(responseBody);
    }

}
