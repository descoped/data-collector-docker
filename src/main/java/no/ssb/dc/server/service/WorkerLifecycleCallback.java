package no.ssb.dc.server.service;

import no.ssb.dc.core.executor.WorkerObservable;
import no.ssb.dc.core.executor.WorkerStatus;

/**
 * This class is used for debugging and profiling purposes only.
 * Should be removed when FAILED tasks that are NOT removed from WorkManager is fixed
 */
public class WorkerLifecycleCallback {

    enum Kind {
        ON_START_BEFORE_TRY_LOCK,
        ON_START_AFTER_TRY_LOCK,
        ON_START_BEFORE_UNLOCK,
        ON_START_AFTER_UNLOCK,
        ON_FINISH_BEFORE_TRY_LOCK,
        ON_FINISH_AFTER_TRY_LOCK,
        ON_FINISH_BEFORE_REMOVE_WORKER,
        ON_FINISH_AFTER_REMOVE_WORKER,
        ON_FINISH_BEFORE_UNLOCK,
        ON_FINISH_AFTER_UNLOCK;
    }

    final Kind kind;
    final WorkManager workManager;
    final WorkerObservable workerObservable;
    final WorkerStatus workerStatus;

    public WorkerLifecycleCallback(Kind kind, WorkManager workManager, WorkerObservable workerObservable, WorkerStatus workerStatus) {
        this.kind = kind;
        this.workManager = workManager;
        this.workerObservable = workerObservable;
        this.workerStatus = workerStatus;
    }
}
