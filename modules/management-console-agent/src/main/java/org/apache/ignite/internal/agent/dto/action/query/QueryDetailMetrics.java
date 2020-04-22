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

package org.apache.ignite.internal.agent.dto.action.query;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Query detail metrics.
 */
public class QueryDetailMetrics {
    /** Textual query representation. */
    private String qry;

    /** Query type. */
    private String qryType;

    /** Cache name. */
    private String cache;

    /** Number of executions. */
    private long execs;

    /** Number of completions executions. */
    private long completions;

    /** Number of failures. */
    private long failures;

    /** Minimum time of execution. */
    private long minTime = -1;

    /** Maximum time of execution. */
    private long maxTime;

    /** Sum of execution time of completions time. */
    private long totalTime;

    /** Sum of execution time of completions time. */
    private long lastStartTime;

    /**
     * @return Query.
     */
    public String getQuery() {
        return qry;
    }

    /**
     * @param qry Query.
     * @return {@code This} for chaining method calls.
     */
    public QueryDetailMetrics setQuery(String qry) {
        this.qry = qry;

        return this;
    }

    /**
     * @return Query type.
     */
    public String getQueryType() {
        return qryType;
    }

    /**
     * @param qryType Query type.
     * @return {@code This} for chaining method calls.
     */
    public QueryDetailMetrics setQueryType(String qryType) {
        this.qryType = qryType;

        return this;
    }

    /**
     * @return Minimal execution time.
     */
    public long getMinTime() {
        return minTime;
    }

    /**
     * @param minTime Minimal time.
     * @return {@code This} for chaining method calls.
     */
    public QueryDetailMetrics setMinTime(long minTime) {
        this.minTime = minTime;

        return this;
    }

    /**
     * @return Maximum time.
     */
    public long getMaxTime() {
        return maxTime;
    }

    /**
     * @param maxTime Max time.
     * @return {@code This} for chaining method calls.
     */
    public QueryDetailMetrics setMaxTime(long maxTime) {
        this.maxTime = maxTime;

        return this;
    }

    /**
     * @return Last start time
     */
    public long getLastStartTime() {
        return lastStartTime;
    }

    /**
     * @param lastStartTime Last start time.
     * @return {@code This} for chaining method calls.
     */
    public QueryDetailMetrics setLastStartTime(long lastStartTime) {
        this.lastStartTime = lastStartTime;

        return this;
    }

    /**
     * @return Cache name.
     */
    public String getCache() {
        return cache;
    }

    /**
     * @param cache Cache name.
     * @return {@code This} for chaining method calls.
     */
    public QueryDetailMetrics setCache(String cache) {
        this.cache = cache;

        return this;
    }

    /**
     * @return Number of executions.
     */
    public long getExecutions() {
        return execs;
    }

    /**
     * @param execs Number of executions.
     * @return {@code This} for chaining method calls.
     */
    public QueryDetailMetrics setExecutions(long execs) {
        this.execs = execs;

        return this;
    }

    /**
     * @return Number of completions.
     */
    public long getCompletions() {
        return completions;
    }

    /**
     * @param completions Number of completions.
     * @return {@code This} for chaining method calls.
     */
    public QueryDetailMetrics setCompletions(long completions) {
        this.completions = completions;

        return this;
    }

    /**
     * @return Number of failures.
     */
    public long getFailures() {
        return failures;
    }

    /**
     * @param failures Number of failures.
     * @return {@code This} for chaining method calls.
     */
    public QueryDetailMetrics setFailures(long failures) {
        this.failures = failures;

        return this;
    }

    /**
     * @return Total time.
     */
    public long getTotalTime() {
        return totalTime;
    }

    /**
     * @param totalTime Total time.
     * @return {@code This} for chaining method calls.
     */
    public QueryDetailMetrics setTotalTime(long totalTime) {
        this.totalTime = totalTime;

        return this;
    }

    /**
     * Aggregate metrics.
     *
     * @param m Other metrics to take into account.
     * @return Aggregated metrics.
     */
    public QueryDetailMetrics merge(QueryDetailMetrics m) {
        execs += m.execs;
        completions += m.completions;
        failures += m.failures;
        minTime = minTime < 0 || minTime > m.minTime ? m.minTime : minTime;
        maxTime = maxTime < m.maxTime ? m.maxTime : maxTime;
        totalTime += m.totalTime;
        lastStartTime = lastStartTime < m.lastStartTime ? m.lastStartTime : lastStartTime;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryDetailMetrics.class, this);
    }
}
