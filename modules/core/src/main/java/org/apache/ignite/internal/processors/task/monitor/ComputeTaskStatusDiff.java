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

package org.apache.ignite.internal.processors.task.monitor;

/**
 * Changes to task status, similar to {@link ComputeTaskStatusSnapshot}, but only stores changes
 * and {@link ComputeTaskStatusSnapshot#sessionId()} as identification.
 */
public interface ComputeTaskStatusDiff extends ComputeTaskStatusSnapshot {
    /**
     * New task started, all methods from {@link ComputeTaskStatusSnapshot} can be used.
     *
     * @return {@code true} if the task is new and has started execution.
     */
    boolean newTask();

    /**
     * @return {@code true} if {@link #jobNodes} has changed.
     */
    boolean hasJobNodesChanged();

    /**
     * @return {@code true} if {@link #attributes} has changed.
     */
    boolean hasAttributesChanged();

    /**
     * {@link ComputeTaskStatusSnapshot#status} and {@link ComputeTaskStatusSnapshot#endTime} have changed, if it is
     * equal to {@link ComputeTaskStatusEnum#FAILED}, then you can get {@link ComputeTaskStatusSnapshot#failReason}.
     *
     * @return {@code true} if the task has completed its execution.
     */
    boolean taskFinished();
}
