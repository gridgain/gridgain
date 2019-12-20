/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.localtask;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageTree;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.LocalContinuousTask;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.internal.util.IgniteUtils.awaitForWorkersStop;

/**
 * Processor that is responsible for long-running, continuous tasks that are executed on local node
 * and should be continued even after node restart.
 */
public class LocalContinuousTasksProcessor extends GridProcessorAdapter implements MetastorageLifecycleListener {
    /** Prefix for metastorage keys for continuous tasks. */
    private static final String STORE_LOCAL_CONTINUOUS_TASK_PREFIX = "local-continuous-task-";

    /** Metastorage. */
    private volatile ReadWriteMetastorage metastorage;

    /** Metastorage synchronization mutex. */
    private final Object metaStorageMux = new Object();

    /** Set of workers that executing local continuous tasks. */
    private final Set<GridWorker> asyncLocContinuousTaskWorkers = new GridConcurrentHashSet<>();

    /** Count of workers that executing local continuous tasks. */
    private final AtomicInteger asyncLocContinuousTasksWorkersCntr = new AtomicInteger(0);

    /** Continuous tasks map. */
    private final ConcurrentHashMap<String, LocalContinuousTask> locContinuousTasks = new ConcurrentHashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public LocalContinuousTasksProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Starts the asynchronous operation of pending tasks execution. Is called on start.
     */
    private void asyncLocalContinuousTasksExecution() {
        assert locContinuousTasks != null;

        for (LocalContinuousTask task : locContinuousTasks.values())
            asyncLocalContinuousTaskExecute(task, true);
    }

    /**
     * Creates a worker to execute single local continuous task.
     * @param task Task.
     * @param dropTaskIfFailed Whether to delete task from metastorage, if it has failed.
     */
    private void asyncLocalContinuousTaskExecute(LocalContinuousTask task, boolean dropTaskIfFailed) {
        String workerName = "async-local-continuous-task-executor-" + asyncLocContinuousTasksWorkersCntr.getAndIncrement();

        GridWorker worker = new GridWorker(ctx.igniteInstanceName(), workerName, log) {
            @Override protected void body() {
                try {
                    log.info("Executing local continuous task: " + task.shortName());

                    task.execute(ctx);

                    log.info("Execution of local continuous task completed: " + task.shortName());

                    removeLocalContinuousTask(task);
                }
                catch (Throwable e) {
                    log.error("Could not execute local continuous task: " + task.shortName(), e);

                    if (dropTaskIfFailed)
                        removeLocalContinuousTask(task);
                }
                finally {
                    asyncLocContinuousTaskWorkers.remove(this);
                }
            }
        };

        asyncLocContinuousTaskWorkers.add(worker);

        Thread asyncTask = new IgniteThread(worker);

        asyncTask.start();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        asyncLocalContinuousTasksExecution();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        // Waiting for workers, but not cancelling them, trying to complete running tasks.
        awaitForWorkersStop(asyncLocContinuousTaskWorkers, false, log);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    /**
     * @param msg Message.
     */
    public void onStateChangeFinish(ChangeGlobalStateFinishMessage msg) {
        if (!msg.clusterActive())
            awaitForWorkersStop(asyncLocContinuousTaskWorkers, true, log);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) {
        synchronized (metaStorageMux) {
            if (locContinuousTasks.isEmpty()) {
                try {
                    metastorage.iterate(
                        STORE_LOCAL_CONTINUOUS_TASK_PREFIX,
                        (key, val) -> locContinuousTasks.put(key, (LocalContinuousTask)val),
                        true
                    );
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to iterate continuous tasks storage.", e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) {
        synchronized (metaStorageMux) {
            try {
                for (Map.Entry<String, LocalContinuousTask> entry : locContinuousTasks.entrySet()) {
                    if (metastorage.readRaw(entry.getKey()) == null)
                        metastorage.write(entry.getKey(), entry.getValue());
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to read key from continuous tasks storage.", e);
            }
        }

        this.metastorage = metastorage;
    }

    /**
     * Builds a metastorage key for continuous task object.
     *
     * @param obj Object.
     * @return Metastorage key.
     */
    private String localContinuousTaskMetastorageKey(LocalContinuousTask obj) {
        String k = STORE_LOCAL_CONTINUOUS_TASK_PREFIX + obj.shortName();

        if (k.length() > MetastorageTree.MAX_KEY_LEN) {
            int hashLenLimit = 5;

            String hash = String.valueOf(k.hashCode());

            k = k.substring(0, MetastorageTree.MAX_KEY_LEN - hashLenLimit) +
                (hash.length() > hashLenLimit ? hash.substring(0, hashLenLimit) : hash);
        }

        return k;
    }

    /**
     * Adds local continuous task object.
     *
     * @param obj Object.
     */
    private void addLocalContinuousTask(LocalContinuousTask obj) {
        String objName = localContinuousTaskMetastorageKey(obj);

        synchronized (metaStorageMux) {
            locContinuousTasks.put(objName, obj);

            if (metastorage != null) {
                ctx.cache().context().database().checkpointReadLock();

                try {
                    metastorage.write(objName, obj);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
                finally {
                    ctx.cache().context().database().checkpointReadUnlock();
                }
            }
        }
    }

    /**
     * Removes local continuous task object.
     *
     * @param obj Object.
     */
    private void removeLocalContinuousTask(LocalContinuousTask obj) {
        String objName = localContinuousTaskMetastorageKey(obj);

        synchronized (metaStorageMux) {
            locContinuousTasks.remove(objName);

            if (metastorage != null) {
                ctx.cache().context().database().checkpointReadLock();

                try {
                    metastorage.remove(objName);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
                finally {
                    ctx.cache().context().database().checkpointReadUnlock();
                }
            }
        }
    }

    /**
     * Starts local continuous task. If task is applied to persistent cache, saves it to metastorage.
     *
     * @param task Continuous task.
     * @param ccfg Cache configuration.
     */
    public void startLocalContinuousTask(LocalContinuousTask task, CacheConfiguration ccfg) {
        if (CU.isPersistentCache(ccfg, ctx.config().getDataStorageConfiguration()))
            addLocalContinuousTask(task);

        asyncLocalContinuousTaskExecute(task, false);
    }
}
