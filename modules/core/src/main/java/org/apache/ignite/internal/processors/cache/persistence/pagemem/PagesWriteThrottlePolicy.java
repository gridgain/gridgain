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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteSystemProperties;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_THROTTLE_LOG_THRESHOLD;

/**
 * Throttling policy, encapsulates logic of delaying write operations.
 * <p>
 * There are two resources that get (or might get) consumed when writing:
 * <ul>
 *     <li>
 *         <b>Checkpoint Buffer</b> where a page is placed when, being under checkpoint, it gets written
 *     </li>
 *     <li>
 *         <b>Clean pages</b> which get dirtied when writes occur
 *     </li>
 * </ul>
 * Both resources are limited in size. Both are freed when checkpoint finishes. This means that, if writers
 * write too fast, they can consume any of these two resources before we have a change to finish a checkpoint.
 * If this happens, the cluster fails or stalls.
 * <p>
 * Write throttling solves this problem by slowing down the writers to a rate at which they do not exhaust
 * any of the two resources.
 * <p>
 * An alternative to just slowing down is to wait in a loop till the resource we're after gets freed, and
 * only then allow the write to happen. The problem with this approach is that we cannot wait in a loop/sleep
 * under a write lock, so the logic would be a lot more complicated. Maybe in the future we'll follow this path,
 * but for now, a simpler approach of just throttling is used (see below).
 * <p>
 * If we just slow writers down by throttling their writes, AND we have enough Checkpoint Buffer and pages in
 * segments to take some load bursts, we are fine. Under such assumptions, it does not matter whether we throttle
 * a writer thread before acquiring write lock or after it gets released; in the current implementation, this
 * happens after write lock gets released (because it was considered simpler to implement).
 * <p>
 * The actual throttling happens when a page gets marked dirty by calling {@link #onMarkDirty(boolean)}.
 * <p>
 * There are two additional methods for interfacing with other parts of the system:
 * <ul>
 *     <li>{@link #tryWakeupThrottledThreads()} which wakes up the threads currently being throttled; in the current
 *     implementation, it is called  when Checkpoint Buffer utilization falls below 1/2.</li>
 *     <li>{@link #shouldThrottle()} which is called by a checkpointer to see whether the Checkpoint Buffer is
 *     in the danger zone and, if yes, it starts to prioretize writing pages from Checkpoint Buffer over
 *     pages from the normal checkpoint sequence.</li>
 * </ul>
 */
public interface PagesWriteThrottlePolicy {
    /** Min park time which triggers logging. */
    long LOGGING_THRESHOLD = TimeUnit.SECONDS.toNanos(
        IgniteSystemProperties.getInteger(IGNITE_THROTTLE_LOG_THRESHOLD, 10));

    /** Checkpoint buffer fullfill upper bound. */
    float CP_BUF_FILL_THRESHOLD = 2f / 3;

    /**
     * Callback to apply throttling delay.
     * @param isPageInCheckpoint flag indicating if current page is in scope of current checkpoint.
     */
    void onMarkDirty(boolean isPageInCheckpoint);

    /**
     * Callback to try wakeup throttled threads. Invoked when the Checkpoint Buffer use drops below a certain
     * threshold.
     */
    default void tryWakeupThrottledThreads() {
        // No-op.
    }

    /**
     * Callback to notify throttling policy checkpoint was started.
     */
    void onBeginCheckpoint();

    /**
     * Callback to notify throttling policy checkpoint was finished.
     */
    void onFinishCheckpoint();

    /**
     * Whether Checkpoint Buffer is currently in the danger zone.
     *
     * @return {@code true} if throttling to protect Checkpoint Buffer should be enabled, and {@code false} otherwise.
     */
    default boolean shouldThrottle() {
        return false;
    }
}
