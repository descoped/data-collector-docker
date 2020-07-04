package no.ssb.dc.server.integrity;

import com.fasterxml.jackson.annotation.JsonProperty;
import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.application.spi.Service;
import no.ssb.dc.server.content.ContentStoreComponent;
import no.ssb.dc.server.db.LmdbEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static no.ssb.dc.server.db.SequenceDbHelper.getSequenceDatabaseLocation;

public class IntegrityCheckService implements Service {

    private static final Logger LOG = LoggerFactory.getLogger(IntegrityCheckService.class);

    final DynamicConfiguration configuration;
    final ContentStoreComponent contentStoreComponent;
    final Map<String, IntegrityCheckJob> jobs = new ConcurrentHashMap<>();

    public IntegrityCheckService(DynamicConfiguration configuration, ContentStoreComponent contentStoreComponent) {
        this.configuration = configuration;
        this.contentStoreComponent = contentStoreComponent;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        for (Map.Entry<String, IntegrityCheckJob> entry : jobs.entrySet()) {
            entry.getValue().terminate();
        }
    }

    public boolean hasJob(String topic) {
        if (jobs.containsKey(topic)) {
            return true;
        }

        List<JobStatus> previouslyCompletedJobs = getPreviouslyCompletedJobs();
        if (previouslyCompletedJobs.contains(new JobStatus(topic, "CLOSED"))) {
            return true;
        }

        return false;
    }

    public boolean isJobRunning(String topic) {
        return jobs.containsKey(topic) && "RUNNING".equals(jobs.get(topic).getSummary().status);
    }

    public boolean removeJobIfClosed(String topic) {
        if (jobs.containsKey(topic) && !"RUNNING".equals(jobs.get(topic).getSummary().status)) {
            jobs.remove(topic);
            return true;
        }
        return false;
    }

    public void createJob(String topic) {
        if (isJobRunning(topic)) {
            return;
        }

        Path dbLocation = getSequenceDatabaseLocation(configuration);
        LOG.trace("Database path: {}", dbLocation);

        LmdbEnvironment.removePath(dbLocation.resolve(topic));

        CompletableFuture<IntegrityCheckJob> future = CompletableFuture.supplyAsync(() -> {
            try (LmdbEnvironment lmdbEnvironment = new LmdbEnvironment(configuration, dbLocation, topic)) {
                try (IntegrityCheckIndex index = new IntegrityCheckIndex(lmdbEnvironment)) {
                    IntegrityCheckJobSummary summary = new IntegrityCheckJobSummary();
                    IntegrityCheckJob job = new IntegrityCheckJob(configuration, contentStoreComponent, index, summary);
                    jobs.put(topic, job);
                    job.consume(topic);
                    LOG.info("Check integrity of topic {} completed successfully at position {}!", topic, summary.getLastPosition());
                    return job;
                }
            }
        }).exceptionally(throwable -> {
            LOG.error("Ended exceptionally with error: {}", CommonUtils.captureStackTrace(throwable));
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else if (throwable instanceof Error) {
                throw (Error) throwable;
            } else {
                throw new RuntimeException(throwable);
            }
        });

        int failCount = 10;
        while (jobs.get(topic) == null) {
            try {
                if (failCount == 0) break;
                TimeUnit.MILLISECONDS.sleep(5);
                failCount--;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public List<JobStatus> getJobs() {
        List<JobStatus> jobStatusList = jobs.values().stream()
                .map(job -> {
                    IntegrityCheckJobSummary.Summary summary = job.getSummary();
                    return new JobStatus(summary.topic, summary.status);
                }).collect(Collectors.toList());

        List<JobStatus> previouslyCompletedJobs = getPreviouslyCompletedJobs();

        // merge previous completed jobs - restore from file system
        for (JobStatus previousCompletedJob : previouslyCompletedJobs) {
            if (!jobStatusList.contains(previousCompletedJob)) {
                jobStatusList.addAll(previouslyCompletedJobs);
            }
        }
        return jobStatusList;
    }

    List<JobStatus> getPreviouslyCompletedJobs() {
        try {
            Path dbLocation = getSequenceDatabaseLocation(configuration);
            return Files.list(dbLocation).filter(path -> path.toFile().isDirectory())
                    .map(path -> new JobStatus(path.getFileName().toString(), "CLOSED")).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public IntegrityCheckJobSummary.Summary getJobSummary(String topic) {
        if (jobs.containsKey(topic)) {
            return jobs.get(topic).getSummary();
        }

        // try load summary from filesystem if exists
        List<JobStatus> jobStatusList = getJobs();
        if (!jobStatusList.contains(new JobStatus(topic, "CLOSED"))) {
            throw new IllegalStateException("Job not found for topic: " + topic);
        }

        Path summaryPath = getSequenceDatabaseLocation(configuration).resolve(topic).resolve("report").resolve("summary.json");
        if (!summaryPath.toFile().exists()) {
            return null;
        }
        try {
            String summaryJson = Files.readString(summaryPath);
            return JsonParser.createJsonParser().fromJson(summaryJson, IntegrityCheckJobSummary.Summary.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    Path writeJobSummaryReport(String topic, Path summaryReportFilePath, IntegrityCheckJobSummary.Summary summary) {
        /*
         * The check-integrity job updates the summary metrics and is kept in memory (Service.jobs).
         * The summary object contains a report-path and report-id that points a file containing duplicates (json-array).
         */

        Path reportFilePath = summary.reportPath.resolve(summary.reportId);
        JsonParser jsonParser = JsonParser.createJsonParser();
        String jsonSummaryResponseBody = jsonParser.toPrettyJSON(summary);

        boolean firstJsonSummaryLine = false;
        try (BufferedReader jsonSummaryReader = new BufferedReader(new StringReader(jsonSummaryResponseBody))) {
            try (FileWriter jsonSummaryFileWriter = new FileWriter(summaryReportFilePath.toFile(), true)) {
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

        return reportFilePath;
    }

    public void cancelJob(String topic) {
        if (!isJobRunning(topic)) {
            return;
        }
        IntegrityCheckJob job = jobs.get(topic);
        job.terminate();
    }

    public static class JobStatus {
        @JsonProperty String topic;
        @JsonProperty String status;

        public JobStatus(String topic, String status) {
            this.topic = topic;
            this.status = status;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JobStatus jobStatus = (JobStatus) o;
            return Objects.equals(topic, jobStatus.topic) &&
                    Objects.equals(status, jobStatus.status);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, status);
        }
    }

}
