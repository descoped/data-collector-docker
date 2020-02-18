package no.ssb.dc.server.controller;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.http.HttpStatusCode;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.application.server.Controller;
import no.ssb.dc.server.service.WorkManager;
import no.ssb.dc.server.service.WorkerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

public class TaskController implements Controller {

    private static final Logger LOG = LoggerFactory.getLogger(TaskController.class);

    private final WorkerService workerService;

    public TaskController(WorkerService workerService) {
        this.workerService = workerService;
    }

    @Override
    public String contextPath() {
        return "/tasks";
    }

    @Override
    public Set<Request.Method> allowedMethods() {
        return Set.of(Request.Method.PUT, Request.Method.GET, Request.Method.DELETE);
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        if ("put".equalsIgnoreCase(exchange.getRequestMethod().toString())) {
            if ("/tasks".equals(exchange.getRequestPath())) {
                createWorkerTask(exchange);
                return;
            }
        }

        if ("get".equalsIgnoreCase(exchange.getRequestMethod().toString())) {
            if ("/tasks".equals(exchange.getRequestPath())) {
                getTaskList(exchange);
                return;
            }
        }

        if ("delete".equalsIgnoreCase(exchange.getRequestMethod().toString())) {
            if (exchange.getRequestPath().startsWith("/tasks")) {
                cancelTask(exchange);
                return;
            }
        }

        exchange.setStatusCode(400);
    }


    private void createWorkerTask(HttpServerExchange exchange) {
        exchange.getRequestReceiver().receiveFullString((httpServerExchange, payload) -> {
            SpecificationBuilder specificationBuilder = Specification.deserialize(payload);
            String workerId = workerService.createOrRejectTask(specificationBuilder);
            int statusCode = workerId != null ? HttpStatusCode.HTTP_CREATED.statusCode() : HttpStatusCode.HTTP_CONFLICT.statusCode();
            exchange.setStatusCode(statusCode);
        });
    }


    private void getTaskList(HttpServerExchange exchange) {
        List<WorkManager.Task> tasks = workerService.list();
        JsonParser jsonParser = JsonParser.createJsonParser();
        String responseBody = jsonParser.toPrettyJSON(tasks);

        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send(responseBody);
    }

    private void cancelTask(HttpServerExchange exchange) {
        String[] path = exchange.getRequestPath().substring(1).split("/");
        if (path.length != 2) {
            exchange.setStatusCode(400);
            return;
        }
        NavigableSet<String> pathElements = new TreeSet<>(List.of(path));
        String resourceName = pathElements.pollLast();
        String workerId = pathElements.pollLast();
        boolean canceled = workerService.cancelTask(workerId);
        if (!canceled) {
            exchange.setStatusCode(400);
            return;
        }
        exchange.setStatusCode(200);
    }
}
