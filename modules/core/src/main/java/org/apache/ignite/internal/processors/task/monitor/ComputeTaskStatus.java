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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.GridTaskSessionImpl;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.FAILED;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.FINISHED;
import static org.apache.ignite.internal.processors.task.monitor.ComputeTaskStatusEnum.RUNNING;

/**
 * Task status container.
 *
 * @see ComputeTaskStatusSnapshot
 * @see ComputeTaskStatusDiff
 */
public class ComputeTaskStatus implements ComputeTaskStatusDiff {
    /** Session ID of the task being executed. */
    private final IgniteUuid sessionId;

    /** Status of the task. */
    @Nullable private final ComputeTaskStatusEnum status;

    /** Task name of the task this session belongs to. */
    @Nullable private final String taskName;

    /** ID of the node on which task execution originated. */
    @Nullable private final UUID originatingNodeId;

    /** Start of computation time for the task. */
    private final long startTime;

    /** End of computation time for the task. */
    private final long endTime;

    /** Nodes IDs on which the task jobs will execute. */
    private final List<UUID> jobNodes;

    /** All session attributes. */
    private final Map<?, ?> attributes;

    /** Reason for the failure of the task. */
    @Nullable private final Throwable failReason;

    /** Flag of a new task. */
    private final boolean newTask;

    /** {@link #jobNodes} field change flag. */
    private final boolean jobNodesChanged;

    /** {@link #attributes} field change flag. */
    private final boolean attributesChanged;

    /** Task completion flag. */
    private final boolean taskFinished;

    /**
     * Constructor for a new task.
     *
     * @param sessionId Session ID of the task being executed.
     * @param status Status of the task.
     * @param taskName Task name of the task this session belongs to.
     * @param originatingNodeId ID of the node on which task execution originated.
     * @param startTime Start of computation time for the task.
     * @param endTime End of computation time for the task.
     * @param jobNodes Nodes IDs on which the task jobs will execute.
     * @param attributes All session attributes.
     * @param failReason Reason for the failure of the task.
     * @param newTask Flag of a new task.
     * @param jobNodesChanged {@code jobNodes} field change flag.
     * @param attributesChanged {@code attributes} field change flag.
     * @param taskFinished Task completion flag.
     */
    private ComputeTaskStatus(
        IgniteUuid sessionId,
        @Nullable ComputeTaskStatusEnum status,
        @Nullable String taskName,
        @Nullable UUID originatingNodeId,
        long startTime,
        long endTime,
        List<UUID> jobNodes,
        Map<?, ?> attributes,
        @Nullable Throwable failReason,
        boolean newTask,
        boolean jobNodesChanged,
        boolean attributesChanged,
        boolean taskFinished
    ) {
        this.sessionId = sessionId;
        this.status = status;
        this.taskName = taskName;
        this.originatingNodeId = originatingNodeId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.jobNodes = jobNodes;
        this.attributes = attributes;
        this.failReason = failReason;
        this.newTask = newTask;
        this.jobNodesChanged = jobNodesChanged;
        this.attributesChanged = attributesChanged;
        this.taskFinished = taskFinished;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid sessionId() {
        return sessionId;
    }

    /** {@inheritDoc} */
    @Override public String taskName() {
        return taskName;
    }

    /** {@inheritDoc} */
    @Override public UUID originatingNodeId() {
        return originatingNodeId;
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public List<UUID> jobNodes() {
        return jobNodes;
    }

    /** {@inheritDoc} */
    @Override public Map<?, ?> attributes() {
        return attributes;
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskStatusEnum status() {
        return status;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Throwable failReason() {
        return failReason;
    }

    /** {@inheritDoc} */
    @Override public boolean newTask() {
        return newTask;
    }

    /** {@inheritDoc} */
    @Override public boolean hasJobNodesChanged() {
        return jobNodesChanged;
    }

    /** {@inheritDoc} */
    @Override public boolean hasAttributesChanged() {
        return attributesChanged;
    }

    /** {@inheritDoc} */
    @Override public boolean taskFinished() {
        return taskFinished;
    }

    /**
     * Creates the status (snapshot) of a task that is in progress.
     *
     * @param sessionImp Task session.
     * @return New instance.
     */
    public static ComputeTaskStatus snapshot(GridTaskSessionImpl sessionImp) {
        return new ComputeTaskStatus(
            sessionImp.getId(),
            RUNNING,
            sessionImp.getTaskName(),
            sessionImp.getTaskNodeId(),
            sessionImp.getStartTime(),
            sessionImp.getEndTime(),
            sessionImp.jobNodes(),
            sessionImp.getAttributes(),
            null,
            false,
            false,
            false,
            false
        );
    }

    /**
     * Creates a task status (diff) on starting new task.
     *
     * @param sessionImp Task session.
     * @return New instance.
     */
    public static ComputeTaskStatus onStartNewTask(GridTaskSessionImpl sessionImp) {
        return new ComputeTaskStatus(
            sessionImp.getId(),
            RUNNING,
            sessionImp.getTaskName(),
            sessionImp.getTaskNodeId(),
            sessionImp.getStartTime(),
            sessionImp.getEndTime(),
            sessionImp.jobNodes(),
            sessionImp.getAttributes(),
            null,
            true,
            false,
            false,
            false
        );
    }

    /**
     * Creates a task status (diff) on changing nodes on which jobs will be executed.
     *
     * @param sessionImp Task session.
     * @return New instance.
     */
    public static ComputeTaskStatus onChangeJobNodes(GridTaskSessionImpl sessionImp) {
        return new ComputeTaskStatus(
            sessionImp.getId(),
            null,
            null,
            null,
            0L,
            0L,
            sessionImp.jobNodes(),
            null,
            null,
            false,
            true,
            false,
            false
        );
    }

    /**
     * Creates a task status (diff) on changing session attributes.
     *
     * @param sessionImp Task session.
     * @return New instance.
     */
    public static ComputeTaskStatus onChangeAttributes(GridTaskSessionImpl sessionImp) {
        return new ComputeTaskStatus(
            sessionImp.getId(),
            null,
            null,
            null,
            0L,
            0L,
            null,
            sessionImp.getAttributes(),
            null,
            false,
            false,
            false,
            true
        );
    }

    /**
     * Creates a task status (diff) on finishing task.
     *
     * @param sessionImp Task session.
     * @param err – Reason for the failure of the task, null if the task completed successfully.
     * @return New instance.
     */
    public static ComputeTaskStatus onFinishTask(GridTaskSessionImpl sessionImp, @Nullable Throwable err) {
        return new ComputeTaskStatus(
            sessionImp.getId(),
            err == null ? FINISHED : FAILED,
            null,
            null,
            0L,
            0L,
            null,
            null,
            err,
            false,
            false,
            false,
            true
        );
    }
}
