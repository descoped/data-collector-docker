package no.ssb.dc.server.controller;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.dc.api.application.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class RestController implements Controller {

    private static final Logger LOG = LoggerFactory.getLogger(RestController.class);

    public RestController() {
    }

    @Override
    public String contextPath() {
        return "/dc";
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        Deque<String> path = List.of(exchange.getRequestURI().split("/"))
                .stream().skip(3).collect(Collectors.toCollection(LinkedList::new));

        String action = path.poll();
        LOG.info("Action: {}", action);

        if ("start".equalsIgnoreCase(action)) {
        }

        if ("stop".equalsIgnoreCase(action)) {
        }

        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
    }
}
