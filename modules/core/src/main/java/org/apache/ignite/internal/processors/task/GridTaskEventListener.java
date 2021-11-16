/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.task;

import java.util.EventListener;
import java.util.UUID;
import org.apache.ignite.internal.GridJobSiblingImpl;
import org.jetbrains.annotations.Nullable;

/**
 * Listener for task events.
 */
interface GridTaskEventListener extends EventListener {
    /**
     * @param worker Started grid task worker.
     */
    void onTaskStarted(GridTaskWorker<?, ?> worker);

    /**
     * Callback on splitting the task into jobs.
     *
     * @param worker Grid task worker.
     */
    void onJobsMapped(GridTaskWorker<?, ?> worker);

    /**
     * @param worker Grid task worker.
     * @param sib Job sibling.
     */
    void onJobSend(GridTaskWorker<?, ?> worker, GridJobSiblingImpl sib);

    /**
     * @param worker Grid task worker.
     * @param sib Job sibling.
     * @param nodeId Failover node ID.
     */
    void onJobFailover(GridTaskWorker<?, ?> worker, GridJobSiblingImpl sib, UUID nodeId);

    /**
     * @param worker Grid task worker.
     * @param sib Job sibling.
     */
    void onJobFinished(GridTaskWorker<?, ?> worker, GridJobSiblingImpl sib);

    /**
     * Callback on finish of task execution.
     *
     * @param worker Task worker for finished grid task.
     * @param err Reason for the failure of the task, {@code null} if the task completed successfully.
     */
    void onTaskFinished(GridTaskWorker<?, ?> worker, @Nullable Throwable err);
}
