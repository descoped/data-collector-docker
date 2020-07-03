package no.ssb.dc.server.recovery;

import io.undertow.server.HttpServerExchange;
import no.ssb.dc.api.http.HttpStatus;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.application.controller.PathDispatcher;
import no.ssb.dc.application.controller.PathHandler;
import no.ssb.dc.application.spi.Controller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static no.ssb.dc.api.http.Request.Method.DELETE;
import static no.ssb.dc.api.http.Request.Method.GET;
import static no.ssb.dc.api.http.Request.Method.PUT;

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

            exchange.setStatusCode(handler.statusCode().code());

        } catch (Exception e) {
            LOG.error("Request error: {}", CommonUtils.captureStackTrace(e));
            exchange.setStatusCode(400);
        }
    }

    private HttpStatus createWorker(PathHandler handler) {

        return HttpStatus.HTTP_NOT_FOUND;
    }

    private HttpStatus getWorkerList(PathHandler handler) {
        return HttpStatus.HTTP_NOT_FOUND;
    }

    private HttpStatus getWorkerSummary(PathHandler handler) {
        return HttpStatus.HTTP_NOT_FOUND;
    }

    private HttpStatus cancelWorker(PathHandler handler) {
        return HttpStatus.HTTP_NOT_FOUND;
    }

}
