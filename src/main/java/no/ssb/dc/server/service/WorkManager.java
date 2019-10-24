package no.ssb.dc.server.service;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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
                    JobId jobId = new JobId(worker.getWorkerId(), specificationBuilder, worker);

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

    List<UUID> list() {
        return workerFutures.keySet().stream().map(jobId -> jobId.workerId).collect(Collectors.toList());
    }

    boolean cancel(UUID workerId) {
        if (!list().contains(workerId)) {
            LOG.warn("Cannot cancel workerId: {}. Not found!", workerId);
            return false;
        }

        JobId jobId = workerFutures.keySet().stream().filter(key -> key.workerId.equals(workerId)).findFirst().orElseThrow();
        LOG.warn("Cancel worker: {}", jobId.workerId);
        jobId.worker.terminate();
//        CompletableFuture<ExecutionContext> future = workerFutures.get(jobId);
//        boolean canceled = future.cancel(true);
//        future.completeExceptionally(new EndOfStreamException());
//        return canceled;
        return true;
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
        final Worker worker;

        JobId(UUID workerId, SpecificationBuilder specificationBuilder, Worker worker) {
            this.workerId = workerId;
            this.specificationBuilder = specificationBuilder;
            this.worker = worker;
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
