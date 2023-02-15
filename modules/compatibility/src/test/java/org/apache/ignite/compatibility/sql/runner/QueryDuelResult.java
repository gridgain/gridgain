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

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * The result of query duel.
 */
public class QueryDuelResult {
    /** */
    private final QueryWithParams qry;

    /** */
    private final long baseExecTimeNanos;

    /** */
    private final long targetExecTimeNanos;

    /** */
    private final Exception baseErr;

    /** */
    private final Exception targetErr;

    /**
     * @param qry Query.
     * @param baseExecTimeNanos Base execute time nanos.
     * @param targetExecTimeNanos Target execute time nanos.
     * @param baseErr Base err.
     * @param targetErr Target err.
     */
    private QueryDuelResult(QueryWithParams qry, long baseExecTimeNanos, long targetExecTimeNanos,
        Exception baseErr, Exception targetErr) {
        this.qry = qry;
        this.baseExecTimeNanos = baseExecTimeNanos;
        this.targetExecTimeNanos = targetExecTimeNanos;

        this.baseErr = baseErr;
        this.targetErr = targetErr;
    }

    /**
     * Returns a query whose execution represents this result.
     *
     * @return Query string.
     */
    public QueryWithParams query() {
        return qry;
    }

    /**
     * Returns base exection times.
     *
     * @return base execution times.
     */
    public long baseExecutionTimeNanos() {
        return baseExecTimeNanos;
    }

    /**
     * Returns target exection times.
     *
     * @return target execution times.
     */
    public long targetExecutionTimeNanos() {
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
            ", baseExecTime=" + nanosToMillis(baseExecTimeNanos) + "ms" +
            ", targetExecTime=" + nanosToMillis(targetExecTimeNanos) + "ms" +
            ", baseErr=" + baseErr +
            ", targetErr=" + targetErr +
            ']';
    }

    /**
     * @param nanos Nanoseconds.
     * @return Millis with precision up to 3 decimals after point.
     */
    @SuppressWarnings("IntegerDivisionInFloatingPointContext")
    private double nanosToMillis(long nanos) {
        return 1.0 * (nanos / 1000) / 1000;
    }

    /**
     * @param qry Query.
     */
    public static Builder builder(QueryWithParams qry) {
        return new Builder(qry);
    }

    /** */
    static class Builder {
        /** */
        private final QueryWithParams qry;

        /** */
        private long baseExecTime;

        /** */
        private long targetExecTime;

        /** */
        private Exception baseErr;

        /** */
        private Exception targetErr;

        /**
         * @param qry Query.
         */
        private Builder(QueryWithParams qry) {
            assert qry != null;

            this.qry = qry;
        }

        /**
         * @param time Execution time on base version, in millis.
         *
         * @return {@code this} for chaining.
         */
        Builder baseExecTime(long time) {
            baseExecTime = time;

            return this;
        }

        /**
         * @param time Execution time on target version, in millis.
         *
         * @return {@code this} for chaining.
         */
        Builder targetExecTime(long time) {
            targetExecTime = time;

            return this;
        }

        /**
         * @param err Error of execution on base version.
         *
         * @return {@code this} for chaining.
         */
        Builder baseErr(@Nullable Exception err) {
            baseErr = err;

            return this;
        }

        /**
         * @param err Error of execution on target version.
         *
         * @return {@code this} for chaining.
         */
        Builder targetErr(@Nullable Exception err) {
            targetErr = err;

            return this;
        }

        /**
         * @return Initialised instance of the result.
         */
        QueryDuelResult build() {
            return new QueryDuelResult(qry, baseExecTime, targetExecTime, baseErr, targetErr);
        }
    }
}
