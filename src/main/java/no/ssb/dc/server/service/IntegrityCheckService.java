package no.ssb.dc.server.service;

import com.fasterxml.jackson.annotation.JsonProperty;
import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.application.spi.Service;
import no.ssb.dc.server.component.ContentStoreComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static no.ssb.dc.server.service.SequenceDbHelper.getSequenceDatabaseLocation;

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
        return jobs.containsKey(topic);
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
        return jobs.values().stream()
                .map(job -> {
                    IntegrityCheckJobSummary.Summary summary = job.getSummary();
                    return new JobStatus(summary.topic, summary.status);
                }).collect(Collectors.toList());
    }

    public IntegrityCheckJobSummary.Summary getJobSummary(String topic) {
        if (!jobs.containsKey(topic)) {
            throw new IllegalStateException("Job not found for topic: " + topic);
        }
        return jobs.get(topic).getSummary();
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
    }

}
