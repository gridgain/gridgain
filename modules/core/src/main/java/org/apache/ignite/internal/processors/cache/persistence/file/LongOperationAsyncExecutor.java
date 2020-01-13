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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.thread.IgniteThread;

/**
 * Synchronization wrapper for long operations that should be executed asynchronously
 * and operations that can not be executed in parallel with long operation. Uses {@link ReadWriteLock}
 * to provide such synchronization scenario.
 */
public class LongOperationAsyncExecutor {
    /** */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /** */
    private final String igniteInstanceName;

    /** */
    private final IgniteLogger log;

    /** */
    private Set<GridWorker> workers = new GridConcurrentHashSet<>();

    /** */
    private static final AtomicLong workerCounter = new AtomicLong(0);

    /** */
    public LongOperationAsyncExecutor(String igniteInstanceName, IgniteLogger log) {
        this.igniteInstanceName = igniteInstanceName;

        this.log = log;
    }

    /**
     * Executes long operation in dedicated thread. Uses write lock as such operations can't run
     * simultaneously.
     *
     * @param runnable long operation
     */
    public void async(Runnable runnable) {
        String workerName = "async-file-store-cleanup-task-" + workerCounter.getAndIncrement();

        GridWorker worker = new GridWorker(igniteInstanceName, workerName, log) {
            @Override protected void body() {
                readWriteLock.writeLock().lock();

                try {
                    runnable.run();
                }
                finally {
                    readWriteLock.writeLock().unlock();

                    workers.remove(this);
                }
            }
        };

        workers.add(worker);

        Thread asyncTask = new IgniteThread(worker);

        asyncTask.start();
    }

    /**
     * Executes closure that can't run in parallel with long operation that is executed by
     * {@link LongOperationAsyncExecutor#async}. Uses read lock as such closures can run in parallel with
     * each other.
     *
     * @param closure closure.
     * @param <T> return type.
     * @return value that is returned by {@code closure}.
     */
    public <T> T afterAsyncCompletion(IgniteOutClosure<T> closure) {
        readWriteLock.readLock().lock();
        try {
            return closure.apply();
        }
        finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * Cancels async tasks.
     */
    public void awaitAsyncTaskCompletion(boolean cancel) {
        U.awaitForWorkersStop(workers, cancel, log);
    }
}
