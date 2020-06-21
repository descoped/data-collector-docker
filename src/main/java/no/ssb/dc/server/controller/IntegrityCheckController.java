package no.ssb.dc.server.controller;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.http.HttpStatus;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.application.spi.Controller;
import no.ssb.dc.server.service.IntegrityCheckJobSummary;
import no.ssb.dc.server.service.IntegrityCheckService;

import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

public class IntegrityCheckController implements Controller {

    final DynamicConfiguration configuration;
    final IntegrityCheckService service;

    public IntegrityCheckController(DynamicConfiguration configuration, IntegrityCheckService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @Override
    public String contextPath() {
        return "/check-integrity";
    }

    @Override
    public Set<Request.Method> allowedMethods() {
        return Set.of(Request.Method.GET, Request.Method.PUT, Request.Method.DELETE);
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        if ("put".equalsIgnoreCase(exchange.getRequestMethod().toString())) {
            if (exchange.getRequestPath().startsWith(contextPath())) {
                createJob(exchange);
                return;
            }
        }

        if ("get".equalsIgnoreCase(exchange.getRequestMethod().toString())) {
            if (contextPath().equals(exchange.getRequestPath())) {
                getJobList(exchange);
                return;
            }

            if (exchange.getRequestPath().startsWith(contextPath())) {
                getJobSummary(exchange);
                return;
            }
        }

        if ("delete".equalsIgnoreCase(exchange.getRequestMethod().toString())) {
            if (exchange.getRequestPath().startsWith(contextPath())) {
                cancelJob(exchange);
                return;
            }
        }

        exchange.setStatusCode(400);
    }

    NavigableSet<String> parseRequestPath(HttpServerExchange exchange, int expectedPathElements) {
        String[] path = exchange.getRequestPath().split("/");
        if (path.length != expectedPathElements) {
            exchange.setStatusCode(400);
            return null;
        }
        return new TreeSet<>(List.of(path));
    }

    // PUT /integrity/TOPIC
    void createJob(HttpServerExchange exchange) {
        NavigableSet<String> pathElements = parseRequestPath(exchange, 3);
        if (pathElements == null) return;
        String topic = pathElements.pollLast();
        if (service.isJobRunning(topic)) {
            exchange.setStatusCode(HttpStatus.HTTP_CONFLICT.code());
            return;
        }
        service.removeJobIfClosed(topic);
        service.createJob(topic);
        exchange.setStatusCode(201);
    }

    // GET /integrity
    void getJobList(HttpServerExchange exchange) {
        List<IntegrityCheckService.JobStatus> runningJobs = service.getJobs();

        JsonParser jsonParser = JsonParser.createJsonParser();
        String responseBody = jsonParser.toPrettyJSON(runningJobs);

        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send(responseBody);
    }

    // GET /integrity/TOPIC
    void getJobSummary(HttpServerExchange exchange) {
        NavigableSet<String> pathElements = parseRequestPath(exchange, 3);
        if (pathElements == null) return;
        String topic = pathElements.pollLast();
        if (!service.hasJob(topic)) {
            exchange.setStatusCode(400);
            return;
        }
        IntegrityCheckJobSummary.Summary summary = service.getJobSummary(topic);
        JsonParser jsonParser = JsonParser.createJsonParser();
        String responseBody = jsonParser.toPrettyJSON(summary);

        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send(responseBody);
    }

    // DELETE /integrity/TOPIC
    void cancelJob(HttpServerExchange exchange) {
        NavigableSet<String> pathElements = parseRequestPath(exchange, 2);
        if (pathElements == null) return;
        String topic = pathElements.pollLast();
        if (!service.isJobRunning(topic)) {
            exchange.setStatusCode(400);
            return;
        }
        service.cancelJob(topic);

    }
}
