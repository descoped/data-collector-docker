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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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

            if (exchange.getRequestPath().startsWith(contextPath()) && exchange.getRequestPath().endsWith("full")) {
                getFullJobSummary(exchange);
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

    Deque<String> parseRequestPath(HttpServerExchange exchange, int expectedPathElements) {
        String[] path = exchange.getRequestPath().split("/");
        if (path.length != expectedPathElements) {
            exchange.setStatusCode(400);
            return null;
        }
        return new LinkedList<>(List.of(path));
    }

    // PUT /check-integrity/TOPIC
    void createJob(HttpServerExchange exchange) {
        Deque<String> pathElements = parseRequestPath(exchange, 3);
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

    // GET /check-integrity
    void getJobList(HttpServerExchange exchange) {
        List<IntegrityCheckService.JobStatus> runningJobs = service.getJobs();

        JsonParser jsonParser = JsonParser.createJsonParser();
        String responseBody = jsonParser.toPrettyJSON(runningJobs);

        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send(responseBody);
    }

    // GET /check-integrity/TOPIC
    void getJobSummary(HttpServerExchange exchange) {
        Deque<String> pathElements = parseRequestPath(exchange, 3);
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

    // GET /check-integrity/TOPIC/full
    void getFullJobSummary(HttpServerExchange exchange) {
        Deque<String> pathElements = parseRequestPath(exchange, 4);
        if (pathElements == null) return;
        pathElements.pollLast();
        String topic = pathElements.pollLast();
        if (!service.hasJob(topic)) {
            exchange.setStatusCode(400);
            return;
        }

        /*
         * The check-integrity job updates the summary metrics and is kept in memory (Service.jobs).
         * The summary object contains a report-path and report-id that points a file containing duplicates (json-array).
         */

        IntegrityCheckJobSummary.Summary summary = service.getJobSummary(topic);
        JsonParser jsonParser = JsonParser.createJsonParser();
        String jsonSummaryResponseBody = jsonParser.toPrettyJSON(summary);

        Path reportFilePath = summary.reportPath.resolve(summary.reportId);
        Path fullSummaryFilePath = summary.reportPath.resolve(topic + ".json");

        boolean firstJsonSummaryLine = false;
        try (BufferedReader jsonSummaryReader = new BufferedReader(new StringReader(jsonSummaryResponseBody))) {
            try (FileWriter jsonSummaryFileWriter = new FileWriter(fullSummaryFilePath.toFile(), true)) {
                try (BufferedWriter jsonSummaryBufferedWriter = new BufferedWriter(jsonSummaryFileWriter)) {
                    jsonSummaryBufferedWriter.write("{");
                    jsonSummaryBufferedWriter.newLine();
                    String jsonSummaryLine = jsonSummaryReader.readLine();
                    while (jsonSummaryLine != null) {
                        if (!firstJsonSummaryLine) {
                            firstJsonSummaryLine = true;
                            jsonSummaryLine = jsonSummaryReader.readLine();
                            continue;
                        }

                        if ("}".equals(jsonSummaryLine)) {
                            break;
                        }

                        jsonSummaryBufferedWriter.write(jsonSummaryLine);
                        jsonSummaryBufferedWriter.newLine();

                        jsonSummaryLine = jsonSummaryReader.readLine();
                    }

                    // write full summary
                    jsonSummaryBufferedWriter.write(" ,\"duplicates\" : ");
                    try (BufferedReader jsonReportReader = new BufferedReader(new FileReader(reportFilePath.toFile()))) {
                        String jsonReportLine = jsonReportReader.readLine();
                        boolean skippedReportLine = false;
                        while (jsonReportLine != null) {
                            if (skippedReportLine) {
                                jsonSummaryBufferedWriter.write("    ");
                            }
                            if (!skippedReportLine) {
                                skippedReportLine = true;
                            }
                            jsonSummaryBufferedWriter.write(jsonReportLine);
                            jsonSummaryBufferedWriter.newLine();
                            jsonReportLine = jsonReportReader.readLine();
                        }
                    }

                    jsonSummaryBufferedWriter.write("}");
                    jsonSummaryBufferedWriter.newLine();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        exchange.setStatusCode(200);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.startBlocking();
        try (OutputStream outputStream = exchange.getOutputStream()) {
            try (InputStream inputStream = new FileInputStream(fullSummaryFilePath.toFile())) {
                byte[] buf = new byte[8192];
                int c;
                while ((c = inputStream.read(buf, 0, buf.length)) > 0) {
                    outputStream.write(buf, 0, c);
                    outputStream.flush();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        exchange.endExchange();
    }

    // DELETE /check-integrity/TOPIC
    void cancelJob(HttpServerExchange exchange) {
        Deque<String> pathElements = parseRequestPath(exchange, 3);
        if (pathElements == null) return;
        String topic = pathElements.pollLast();
        if (!service.isJobRunning(topic)) {
            exchange.setStatusCode(400);
            return;
        }
        service.cancelJob(topic);
        exchange.setStatusCode(200);
    }
}
