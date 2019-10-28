package no.ssb.dc.server.service;

import com.fasterxml.jackson.annotation.JsonProperty;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.core.executor.WorkerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class WorkManager {

    private static final Logger LOG = LoggerFactory.getLogger(WorkManager.class);
    private final Map<JobId, CompletableFuture<ExecutionContext>> workerFutures = new ConcurrentHashMap<>();
    private final Map<String, Lock> lockBySpecificationId = new ConcurrentHashMap<>();

    WorkManager() {
    }

    void lock(String specificationId) {
        Lock lock = lockBySpecificationId.computeIfAbsent(specificationId, l -> new ReentrantLock());
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new WorkerException(e);
        }
    }

    void unlock(String specificationId) {
        Lock lock = lockBySpecificationId.get(specificationId);
        lock.unlock();
    }

    boolean isRunning(String specificationId) {
        return workerFutures.keySet().stream().anyMatch(jobId -> jobId.specificationId.equals(specificationId));
    }

    JobId run(Worker.WorkerBuilder workerBuilder) {
        String specificationId = workerBuilder.getSpecificationBuilder().getId();
        Worker worker = workerBuilder.build();
        JobId jobId = new JobId(worker.getWorkerId(), specificationId, worker.getSpecificationName(), worker);

        CompletableFuture<ExecutionContext> future = worker
                .runAsync()
                .handle((output, throwable) -> {
                    LOG.error("Worker failed: {}", CommonUtils.captureStackTrace(throwable));
                    return output;
                });

        workerFutures.put(jobId, future);

        return jobId;
    }

    List<Task> list() {
        return workerFutures.keySet().stream()
                .map(jobId -> new Task(jobId.workerId.toString(), jobId.specificationId, jobId.specificationName))
                .collect(Collectors.toList());
    }

    boolean cancel(UUID workerId) {
        if (list().stream().noneMatch(task -> task.taskId.equals(workerId.toString()))) {
            LOG.warn("Cannot cancel workerId: {}. Not found!", workerId);
            return false;
        }

        JobId jobId = workerFutures.keySet().stream().filter(key -> key.workerId.equals(workerId)).findFirst().orElseThrow();
        LOG.warn("Cancel worker: {}", jobId.workerId);
        jobId.worker.terminate();
        return true;
    }

    JobId get(UUID workerId) {
        return workerFutures.keySet().stream().filter(key -> key.workerId.equals(workerId)).findFirst().orElse(null);
    }

    void remove(UUID workerId) {
        JobId jobId = workerFutures.keySet().stream().filter(key -> key.workerId.equals(workerId)).findFirst().orElseThrow();
        workerFutures.remove(jobId);
    }

    void cancel() {
        CompletableFuture.allOf(workerFutures.values().toArray(new CompletableFuture[0]))
                .completeOnTimeout(null, 0, TimeUnit.MILLISECONDS);
    }

    static class JobId {
        final UUID workerId;
        final String specificationId;
        final String specificationName;
        final Worker worker;

        JobId(UUID workerId, String specificationId, String specificationName, Worker worker) {
            this.workerId = workerId;
            this.specificationId = specificationId;
            this.specificationName = specificationName;
            this.worker = worker;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JobId jobId = (JobId) o;
            return workerId.equals(jobId.workerId) &&
                    specificationId.equals(jobId.specificationId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workerId, specificationId);
        }

        @Override
        public String toString() {
            return "JobId{" +
                    "workerId=" + workerId +
                    ", specificationId='" + specificationId + '\'' +
                    ", specificationName='" + specificationName + '\'' +
                    '}';
        }
    }

    public static class Task {
        @JsonProperty("task-id") public final String taskId;
        @JsonProperty("specification-id") public final String specificationId;
        @JsonProperty("description") public final String description;

        Task(String taskId, String specificationId, String description) {
            this.taskId = taskId;
            this.specificationId = specificationId;
            this.description = description;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Task task = (Task) o;
            return taskId.equals(task.taskId) &&
                    specificationId.equals(task.specificationId) &&
                    Objects.equals(description, task.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, specificationId, description);
        }

        @Override
        public String toString() {
            return "Task{" +
                    "taskd='" + taskId + '\'' +
                    ", specificationId='" + specificationId + '\'' +
                    ", description='" + description + '\'' +
                    '}';
        }
    }

}
