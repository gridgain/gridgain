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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.ReentrantReadWriteLockWithTracking;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS;

/**
 * Wrapper of the classic read write lock with checkpoint features.
 */
public class CheckpointReadWriteLock {
    /** Assertion enabled. */
    private static final boolean ASSERTION_ENABLED = GridCacheDatabaseSharedManager.class.desiredAssertionStatus();

    private static final long CP_ALERT_THRESHOLD_MS = IgniteSystemProperties.getLong("CP_ALERT_THRESHOLD_MS", 10_000);
    private static final int CP_THROTTLING = IgniteSystemProperties.getInteger("CP_THROTTLING", 10);
    private static final boolean CP_TRACE = IgniteSystemProperties.getBoolean("CP_TRACE", false);

    /** Checkpoint lock hold count. */
    public static final ThreadLocal<Integer> CHECKPOINT_LOCK_HOLD_COUNT = ThreadLocal.withInitial(() -> 0);

    public static final ThreadLocal<Long> CHECKPOINT_LOCK_HOLD_START_TS = ThreadLocal.withInitial(() -> 0L);
    public static final ThreadLocal<Integer> LOG_THROTTLING = ThreadLocal.withInitial(() -> 0);

    private static final ConcurrentHashMap<String, Integer> READERS = new ConcurrentHashMap<>();
    private static final AtomicBoolean writeRequested = new AtomicBoolean(false);

    /**
     * Any thread with a such prefix is managed by the checkpoint. So some conditions can rely on it(ex. we don't need a
     * checkpoint lock there because checkpoint is already held the write lock).
     */
    static final String CHECKPOINT_RUNNER_THREAD_PREFIX = "checkpoint-runner";

    /** Checkpoint lock. */
    private final ReentrantReadWriteLockWithTracking checkpointLock;

    private final IgniteLogger log;

    /**
     * @param logger Logger.
     */
    CheckpointReadWriteLock(Function<Class<?>, IgniteLogger> logger) {
        log = logger.apply(getClass());

        if (getBoolean(IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS))
            checkpointLock = new ReentrantReadWriteLockWithTracking(logger.apply(getClass()), 5_000);
        else
            checkpointLock = new ReentrantReadWriteLockWithTracking();
    }

    private void traceRL(String category) {
        int hc = getReadHoldCount();

        READERS.put(Thread.currentThread().getName(), hc);

        if (hc == 1) {
            CHECKPOINT_LOCK_HOLD_START_TS.set(System.nanoTime());
        }

        if (writeRequested.get()) {
            if (CP_THROTTLING > 0) {
                int throttling = LOG_THROTTLING.get();
                if (throttling > CP_THROTTLING) {
                    log("On RL " + category + " :: hc=" + hc);
                    LOG_THROTTLING.set(0);

                } else {
                    LOG_THROTTLING.set(throttling + 1);
                }
            } else {
                log("On RL " + category + " :: hc=" + hc);
            }
        }
    }

    /**
     * Gets the checkpoint read lock. While this lock is held, checkpoint thread will not acquireSnapshotWorker memory
     * state.
     *
     * @throws IgniteException If failed.
     */
    public void readLock() {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return;

        checkpointLock.readLock().lock();
        if (CP_TRACE) traceRL("readLock()");

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);
    }

    /**
     * Gets the checkpoint read lock. While this lock is held, checkpoint thread will not acquireSnapshotWorker memory
     * state.
     *
     * @throws IgniteException If failed.
     */
    public boolean tryReadLock(long timeout, TimeUnit unit) throws InterruptedException {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return true;

        boolean res = checkpointLock.readLock().tryLock(timeout, unit);
        if (res && CP_TRACE) traceRL("tryReadLock(t, u)");

        if (ASSERTION_ENABLED && res)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);

        return res;
    }

    /**
     * Try to get a checkpoint read lock.
     *
     * @return {@code True} if the checkpoint read lock is acquired.
     */
    public boolean tryReadLock() {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return true;

        boolean res = checkpointLock.readLock().tryLock();
        if (res && CP_TRACE) traceRL("tryReadLock()");

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);

        return res;
    }

    /**
     * This method works only if the assertion is enabled or it always returns true otherwise.
     *
     * @return {@code true} if checkpoint lock is held by current thread.
     */
    public boolean checkpointLockIsHeldByThread() {
        return !ASSERTION_ENABLED ||
            checkpointLock.isWriteLockedByCurrentThread() ||
            CHECKPOINT_LOCK_HOLD_COUNT.get() > 0 ||
            Thread.currentThread().getName().startsWith(CHECKPOINT_RUNNER_THREAD_PREFIX);
    }

    /**
     * Releases the checkpoint read lock.
     */
    public void readUnlock() {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return;

        checkpointLock.readLock().unlock();

        if (CP_TRACE) {
            int hc = checkpointLock.getReadHoldCount();
            if (hc == 0) {
                READERS.remove(Thread.currentThread().getName());
            } else {
                READERS.put(Thread.currentThread().getName(), hc);
            }

            long start = CHECKPOINT_LOCK_HOLD_START_TS.get();
            long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            if (durationMillis > CP_ALERT_THRESHOLD_MS) {
                log("CP was captured for too long " + durationMillis + "ms  ");
            }

            if (writeRequested.get()) {
                log("On RL release :: " + READERS);
            }
        }

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() - 1);
    }

    /**
     * Take the checkpoint write lock.
     */
    public void writeLock() {
        log("On WL get :: " + READERS);
        writeRequested.set(true);

        checkpointLock.writeLock().lock();
        log("On WL acquired :: " + READERS);

        if (!Thread.currentThread().getName().startsWith("db-checkpoint")) {
            log("Trying to capture CP WL");
        }
    }

    /**
     * Release the checkpoint write lock
     */
    public void writeUnlock() {
        checkpointLock.writeLock().unlock();
        writeRequested.set(false);
        log("CP WL Released");
    }

    /**
     * @return {@code true} if current thread hold the write lock.
     */
    public boolean isWriteLockHeldByCurrentThread() {
        return checkpointLock.writeLock().isHeldByCurrentThread();
    }

    /**
     *
     */
    public int getReadHoldCount() {
        return checkpointLock.getReadHoldCount();
    }

    private void log(String message) {
        if (log.isDebugEnabled()) log.debug(message);
    }
}
