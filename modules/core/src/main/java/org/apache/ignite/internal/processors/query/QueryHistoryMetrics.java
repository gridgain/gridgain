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

package org.apache.ignite.internal.processors.query;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 * Query history metrics.
 */
public class QueryHistoryMetrics {
    /** Link to internal node in eviction deque. */
    @GridToStringExclude
    private final AtomicReference<ConcurrentLinkedDeque8.Node<QueryHistoryMetrics>> linkRef;

    /** Query history metrics immutable wrapper. */
    private volatile QueryHistoryMetricsValue val;

    /** Query history metrics group key. */
    private final QueryHistoryMetricsKey key;

    /**
     * Constructor with metrics.
     *
     * @param failed {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    public QueryHistoryMetrics(GridRunningQueryInfo info, boolean failed) {
        key = new QueryHistoryMetricsKey(info.query(), info.schemaName(), info.local());

        long failures = failed ? 1 : 0;
        long duration = System.currentTimeMillis() - info.startTime();
        long reserved = info.memoryMetricProvider().maxReserved();
        long allocatedOnDisk = info.memoryMetricProvider().maxWrittenOnDisk();
        long totalWrittenOnDisk = info.memoryMetricProvider().totalWrittenOnDisk();

        val = new QueryHistoryMetricsValue(1, failures, duration, duration, info.startTime(),
            reserved, reserved, allocatedOnDisk, allocatedOnDisk, totalWrittenOnDisk, totalWrittenOnDisk);

        linkRef = new AtomicReference<>();
    }

    /**
     * @return Metrics group key.
     */
    public QueryHistoryMetricsKey key() {
        return key;
    }

    /**
     * Aggregate new metrics with already existen.
     *
     * @param m Other metrics to take into account.
     * @return Aggregated metrics.
     */
    public QueryHistoryMetrics aggregateWithNew(QueryHistoryMetrics m) {
        val = new QueryHistoryMetricsValue(
            val.execs() + m.executions(),
            val.failures() + m.failures(),
            Math.min(val.minTime(), m.minimumTime()),
            Math.max(val.maxTime(), m.maximumTime()),
            Math.max(val.lastStartTime(), m.lastStartTime()),
            Math.min(val.minMemory(), m.minMemory()),
            Math.max(val.maxMemory(), m.maxMemory()),
            Math.min(val.minBytesAllocatedOnDisk(), m.minBytesAllocatedOnDisk()),
            Math.max(val.maxBytesAllocatedOnDisk(), m.maxBytesAllocatedOnDisk()),
            Math.min(val.minTotalBytesWrittenOnDisk(), m.minTotalBytesWrittenOnDisk()),
            Math.max(val.maxTotalBytesWrittenOnDisk(), m.maxTotalBytesWrittenOnDisk())
        );

        return this;
    }

    /**
     * @return Textual representation of query.
     */
    public String query() {
        return key.query();
    }

    /**
     * @return Schema.
     */
    public String schema() {
        return key.schema();
    }

    /**
     * @return {@code true} For query with enabled local flag.
     */
    public boolean local() {
        return key.local();
    }

    /**
     * Gets total number execution of query.
     *
     * @return Number of executions.
     */
    public long executions() {
        return val.execs();
    }

    /**
     * Gets number of times a query execution failed.
     *
     * @return Number of times a query execution failed.
     */
    public long failures() {
        return val.failures();
    }

    /**
     * Gets minimum execution time of query.
     *
     * @return Minimum execution time of query.
     */
    public long minimumTime() {
        return val.minTime();
    }

    /**
     * Gets maximum execution time of query.
     *
     * @return Maximum execution time of query.
     */
    public long maximumTime() {
        return val.maxTime();
    }

    /**
     * Gets minimum memory required by query.
     *
     * @return Minimum memory required by query.
     */
    public long minMemory() {
        return val.minMemory();
    }

    /**
     * Gets maximum memory required by query.
     *
     * @return Maximum memory required by query.
     */
    public long maxMemory() {
        return val.maxMemory();
    }

    /**
     * Gets minimum bytes on disk required by query.
     *
     * @return Minimum bytes on disk required by query.
     */
    public long minBytesAllocatedOnDisk() {
        return val.minBytesAllocatedOnDisk();
    }

    /**
     * Gets maximum bytes on disk required by query.
     *
     * @return Maximum bytes on disk required by query.
     */
    public long maxBytesAllocatedOnDisk() {
        return val.maxBytesAllocatedOnDisk();
    }

    /**
     * Gets minimum bytes written on disk in total by query.
     *
     * @return Minimum bytes written on disk in total by query.
     */
    public long minTotalBytesWrittenOnDisk() {
        return val.minTotalBytesWrittenOnDisk();
    }

    /**
     * Gets maximum bytes written on disk in total by query.
     *
     * @return Maximum bytes written on disk in total by query.
     */
    public long maxTotalBytesWrittenOnDisk() {
        return val.maxTotalBytesWrittenOnDisk();
    }

    /**
     * Gets latest query start time.
     *
     * @return Latest time query was stared.
     */
    public long lastStartTime() {
        return val.lastStartTime();
    }

    /**
     * @return Link to internal node in eviction deque.
     */
    @Nullable public ConcurrentLinkedDeque8.Node<QueryHistoryMetrics> link() {
        return linkRef.get();
    }

    /**
     * Atomically replace link to new.
     *
     * @param expLink Link which should be replaced.
     * @param updatedLink New link which should be set.
     * @return {@code true} If link has been updated.
     */
    public boolean replaceLink(ConcurrentLinkedDeque8.Node<QueryHistoryMetrics> expLink,
        ConcurrentLinkedDeque8.Node<QueryHistoryMetrics> updatedLink) {
        return linkRef.compareAndSet(expLink, updatedLink);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryHistoryMetrics.class, this);
    }
}
