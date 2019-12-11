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

package org.apache.ignite.agent.dto.action;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for task response.
 */
public class TaskResponse {
    /** Task response id. */
    private UUID id;

    /** Job count. */
    private int jobCnt;

    /** Node consistent id. */
    private String nodeConsistentId;

    /** Status. */
    private Status status;

    /**
     * @return Task response id.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id Id.
     * @return @{code This} for chaining method calls.
     */
    public TaskResponse setId(UUID id) {
        this.id = id;

        return this;
    }

    /**
     * @return Job count.
     */
    public int getJobCount() {
        return jobCnt;
    }

    /**
     * @param jobCnt Job count.
     * @return @{code This} for chaining method calls.
     */
    public TaskResponse setJobCount(int jobCnt) {
        this.jobCnt = jobCnt;

        return this;
    }

    /**
     * @return Node consistent id.
     */
    public String getNodeConsistentId() {
        return nodeConsistentId;
    }

    /**
     * @param nodeConsistentId Node consistent id.
     * @return @{code This} for chaining method calls.
     */
    public TaskResponse setNodeConsistentId(String nodeConsistentId) {
        this.nodeConsistentId = nodeConsistentId;

        return this;
    }

    /**
     * @return Status.
     */
    public Status getStatus() {
        return status;
    }

    /**
     * @param status Status.
     * @return This for chaining method calls.
     */
    public TaskResponse setStatus(Status status) {
        this.status = status;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TaskResponse.class, this);
    }
}
