package no.ssb.dc.server.service;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

class WorkManager {

    private static final Logger LOG = LoggerFactory.getLogger(WorkManager.class);
    private final Map<JobId, CompletableFuture<ExecutionContext>> workerFutures = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    WorkManager() {
    }

    boolean isRunning(SpecificationBuilder specificationBuilder) {
        try {
            if (lock.tryLock(1, TimeUnit.SECONDS)) {
                try {
                    return workerFutures.keySet().stream().anyMatch(jobId -> jobId.specificationBuilder.equals(specificationBuilder));
                } finally {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        return false;
    }

    void run(Worker.WorkerBuilder workerBuilder) {
        try {
            if (lock.tryLock(1, TimeUnit.SECONDS)) {
                try {
                    SpecificationBuilder specificationBuilder = workerBuilder.getSpecificationBuilder();
                    Worker worker = workerBuilder.build();
                    JobId jobId = new JobId(worker.getWorkerId(), specificationBuilder);

                    CompletableFuture<ExecutionContext> future = worker
                            .runAsync()
                            .handle((output, throwable) -> {
                                LOG.error("Worker failed: {}", CommonUtils.captureStackTrace(throwable));
                                return output;
                            });

                    workerFutures.put(jobId, future);

                } finally {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    void remove(UUID workerId) {
        try {
            if (lock.tryLock(1, TimeUnit.SECONDS)) {
                try {
                    JobId jobId = workerFutures.keySet().stream().filter(key -> key.workerId.equals(workerId)).findFirst().orElseThrow();
                    workerFutures.remove(jobId);
                } finally {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    void cancel() {
        CompletableFuture.allOf(workerFutures.values().toArray(new CompletableFuture[0]))
                .cancel(true);
    }

    static class JobId {
        final UUID workerId;
        final SpecificationBuilder specificationBuilder;

        JobId(UUID workerId, SpecificationBuilder specificationBuilder) {
            this.workerId = workerId;
            this.specificationBuilder = specificationBuilder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JobId jobId = (JobId) o;
            return Objects.equals(workerId, jobId.workerId) &&
                    Objects.equals(specificationBuilder, jobId.specificationBuilder);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workerId, specificationBuilder);
        }

        @Override
        public String toString() {
            return "Job{" +
                    "workerId=" + workerId +
                    ", specificationBuilder=" + specificationBuilder +
                    '}';
        }
    }

}
