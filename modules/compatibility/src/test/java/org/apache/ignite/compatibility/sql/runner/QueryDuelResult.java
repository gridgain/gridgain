/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.compatibility.sql.runner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * The result of query duel.
 */
public class QueryDuelResult {
    /**
     * Status of the particular result. Needed to distinguish fails because
     * of exception from fails because of increased latency.
     */
    public enum Status {
        /** */
        SUCCESS,

        /** */
        VERIFICATION_FAILED,

        /** */
        ERROR
    }

    /** */
    private final Status status;

    /** */
    private final String qry;

    /** */
    private final List<Long> baseExecTimeNanos;

    /** */
    private final List<Long> targetExecTimeNanos;

    /** */
    private final Exception baseErr;

    /** */
    private final Exception targetErr;

    /**
     * @param status Status.
     * @param qry Query.
     * @param baseExecTimeNanos Base execute time nanos.
     * @param targetExecTimeNanos Target execute time nanos.
     * @param baseErr Base err.
     * @param targetErr Target err.
     */
    private QueryDuelResult(Status status, String qry, List<Long> baseExecTimeNanos, List<Long> targetExecTimeNanos,
        Exception baseErr, Exception targetErr) {
        this.status = status;
        this.qry = qry;
        this.baseExecTimeNanos = new ArrayList<>(baseExecTimeNanos);
        this.targetExecTimeNanos = new ArrayList<>(targetExecTimeNanos);

        this.baseErr = baseErr;
        this.targetErr = targetErr;
    }

    /**
     * Creates successful result for given query and execution times.
     *
     * @param qry Query.
     * @param baseExecTimes Base execute times.
     * @param targetExecTimes Target execute times.
     */
    static QueryDuelResult successfulResult(String qry, List<Long> baseExecTimes, List<Long> targetExecTimes) {
        assert !F.isEmpty(qry);
        assert !F.isEmpty(baseExecTimes);
        assert !F.isEmpty(targetExecTimes);

        return new QueryDuelResult(Status.SUCCESS, qry, baseExecTimes, targetExecTimes, null, null);
    }

    /**
     * Creates result with status {@link Status#VERIFICATION_FAILED} for given query and execution times.
     *
     * @param qry Query.
     * @param baseExecTimes Base execute times.
     * @param targetExecTimes Target execute times.
     */
    static QueryDuelResult verificationFailedResult(String qry, List<Long> baseExecTimes, List<Long> targetExecTimes) {
        assert !F.isEmpty(qry);
        assert !F.isEmpty(baseExecTimes);
        assert !F.isEmpty(targetExecTimes);

        return new QueryDuelResult(Status.VERIFICATION_FAILED, qry, baseExecTimes, targetExecTimes, null, null);
    }

    /**
     * Creates result with status {@link Status#ERROR} for given query and exceptions.
     *
     * @param qry Query.
     * @param baseErr Base err.
     * @param targetErr Target err.
     */
    static QueryDuelResult failedResult(String qry, Exception baseErr, Exception targetErr) {
        assert !F.isEmpty(qry) : qry;
        assert baseErr != null;
        assert targetErr != null;

        return new QueryDuelResult(Status.ERROR, qry, Collections.emptyList(), Collections.emptyList(),
            baseErr, targetErr);
    }

    /**
     * Returns {@code true} if result could be considered as successful.
     *
     * @return {@code true} if result could be considered as successful,
     * {@code false} otherwise.
     */
    public boolean successful() {
        return status == Status.SUCCESS;
    }

    /**
     * Returns a query whose execution represents this result.
     *
     * @return Query string.
     */
    public String query() {
        return qry;
    }

    /**
     * Returns base exection times.
     *
     * @return base execution times.
     */
    public List<Long> baseExecutionTimeNanos() {
        return baseExecTimeNanos;
    }

    /**
     * Returns target exection times.
     *
     * @return target execution times.
     */
    public List<Long> targetExecutionTimeNanos() {
        return targetExecTimeNanos;
    }

    /**
     * Returns base exection error.
     *
     * @return base execution error.
     */
    public @Nullable Exception baseError() {
        return baseErr;
    }

    /**
     * Returns target exection error.
     *
     * @return Target execution error.
     */
    public @Nullable Exception targetError() {
        return targetErr;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryDuelResult res = (QueryDuelResult)o;

        return Objects.equals(qry, res.qry);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(qry);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "QueryDuelResult[" +
            "qry='" + qry + '\'' +
            "status='" + status + '\'' +
            ", baseExecTimeNanos=" + baseExecTimeNanos +
            ", targetExecTimeNanos=" + targetExecTimeNanos +
            ", baseErr=" + baseErr +
            ", targetErr=" + targetErr +
            ']';
    }
}
