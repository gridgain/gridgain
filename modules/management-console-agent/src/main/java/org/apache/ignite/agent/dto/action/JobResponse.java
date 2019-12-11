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
 * DTO for job response.
 */
public class JobResponse {
    /** Request id. */
    private UUID reqId;

    /** Result. */
    private Object res;

    /** Error. */
    private ResponseError error;

    /** Status. */
    private Status status;

    /** Timestamp. */
    private long ts = System.currentTimeMillis();
    
    /** Node consistent id. */
    private String nodeConsistentId;

    /**
     * @return Request id.
     */
    public UUID getRequestId() {
        return reqId;
    }

    /**
     * @param reqId Request id.
     * @return @{code This} for chaining method calls.
     */
    public JobResponse setRequestId(UUID reqId) {
        this.reqId = reqId;

        return this;
    }

    /**
     * @return Action result.
     */
    public Object getResult() {
        return res;
    }

    /**
     * @param res Response.
     * @return @{code This} for chaining method calls.
     */
    public JobResponse setResult(Object res) {
        this.res = res;

        return this;
    }

    /**
     * @return Response error.
     */
    public ResponseError getError() {
        return error;
    }

    /**
     * @param error Response error.
     * @return @{code This} for chaining method calls.
     */
    public JobResponse setError(ResponseError error) {
        this.error = error;

        return this;
    }

    /**
     * @return Status.
     */
    public Status getStatus() {
        return status;
    }

    /**
     * @param status  Status.
     * @return This for chaining method calls.
     */
    public JobResponse setStatus(Status status) {
        this.status = status;

        return this;
    }

    /**
     * @return Timestamp.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @param ts Timestamp.
     */
    public JobResponse setTimestamp(long ts) {
        this.ts = ts;

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
    public JobResponse setNodeConsistentId(String nodeConsistentId) {
        this.nodeConsistentId = nodeConsistentId;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JobResponse.class, this);
    }
}
