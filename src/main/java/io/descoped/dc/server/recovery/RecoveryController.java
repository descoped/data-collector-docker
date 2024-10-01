package io.descoped.dc.server.recovery;

import io.descoped.dc.api.http.HttpStatus;
import io.descoped.dc.api.http.Request;
import io.descoped.dc.api.util.CommonUtils;
import io.descoped.dc.api.util.JsonParser;
import io.descoped.dc.application.controller.PathDispatcher;
import io.descoped.dc.application.controller.PathHandler;
import io.descoped.dc.application.spi.Controller;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.descoped.dc.api.http.Request.Method.DELETE;
import static io.descoped.dc.api.http.Request.Method.GET;
import static io.descoped.dc.api.http.Request.Method.PUT;

public class RecoveryController implements Controller {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveryController.class);

    private final PathDispatcher dispatcher;
    private final RecoveryService service;


    public RecoveryController(RecoveryService service) {
        this.service = service;
        dispatcher = PathDispatcher.create();
        dispatcher.bind("/recovery/{topic}", PUT, this::createWorker);
        dispatcher.bind("/recovery", GET, this::getWorkerList);
        dispatcher.bind("/recovery/{topic}", GET, this::getWorkerSummary);
        dispatcher.bind("/recovery/{topic}", DELETE, this::cancelWorker);
    }

    @Override
    public String contextPath() {
        return "/recovery";
    }

    @Override
    public Set<Request.Method> allowedMethods() {
        return Set.of(GET, PUT, DELETE);
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        try {
            PathHandler handler = dispatcher.dispatch(
                    exchange.getRequestPath(),
                    Request.Method.valueOf(exchange.getRequestMethod().toString().toUpperCase()),
                    exchange);

            if (!exchange.isComplete()) {
                exchange.setStatusCode(handler.statusCode().code());
            }

        } catch (Exception e) {
            LOG.error("Request error: {}", CommonUtils.captureStackTrace(e));
            exchange.setStatusCode(400);
        }
    }

    boolean isRunning(String topic) {
        CompletableFuture<RecoveryWorker> future = service.jobFutures.get(topic);
        RecoveryWorker worker = service.jobs.get(topic);
        if (worker == null && future != null && !future.isDone()) {
            try {
                int failCount = 10;
                while (failCount > 0 && (worker = service.jobs.get(topic)) == null) {
                    Thread.sleep(10);
                    failCount--;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (worker == null) {
            return false;
        }
        return worker.monitor().running.get();
    }

    // PUT /recovery/{topic}?toTopic=TARGET_TOPIC
    private HttpStatus createWorker(PathHandler handler) {
        String fromTopic = handler.parameters().get("topic");
        if (isRunning(fromTopic)) {
            return HttpStatus.HTTP_CONFLICT;
        }
        if (!handler.exchange().getQueryParameters().containsKey("toTopic")) {
            return HttpStatus.HTTP_BAD_REQUEST;
        }
        String toTopic = handler.exchange().getQueryParameters().get("toTopic").getFirst();
        service.createRecoveryWorker(fromTopic, toTopic);
        return HttpStatus.HTTP_CREATED;
    }

    // GET /recovery
    private HttpStatus getWorkerList(PathHandler handler) {
        List<String> list = service.getSequenceDatabaseList().stream().map(path -> path.getFileName().toString()).collect(Collectors.toList());
        String indexListJson = JsonParser.createJsonParser().toPrettyJSON(list);
        handler.exchange().getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        handler.exchange().getResponseSender().send(indexListJson);
        return HttpStatus.HTTP_OK;
    }

    // GET /recovery/{topic}
    private HttpStatus getWorkerSummary(PathHandler handler) {
        String fromTopic = handler.parameters().get("topic");
        RecoveryWorker recoveryWorker = service.jobs.get(fromTopic);
        if (recoveryWorker == null) {
            return HttpStatus.HTTP_BAD_REQUEST;
        }
        RecoveryMonitor.Summary summary = recoveryWorker.summary();
        String summaryJson = JsonParser.createJsonParser().toPrettyJSON(summary);
        handler.exchange().getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        handler.exchange().getResponseSender().send(summaryJson);
        return HttpStatus.HTTP_OK;
    }

    // DELETE /recovery/{topic}
    private HttpStatus cancelWorker(PathHandler handler) {
        String fromTopic = handler.parameters().get("topic");
        if (!isRunning(fromTopic)) {
            return HttpStatus.HTTP_NOT_FOUND;
        }
        RecoveryWorker recoveryWorker = service.jobs.get(fromTopic);
        recoveryWorker.terminate();
        return HttpStatus.HTTP_OK;
    }
}
