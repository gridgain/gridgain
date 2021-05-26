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

package org.apache.ignite.internal.metric;

import java.util.function.LongSupplier;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;

/**
 * Holds metrics of heap memory usage by sql queries. One instance per node.
 *
 * @see QueryMemoryManager
 */
public class SqlMemoryStatisticsHolder {
    /** Name of MetricRegistry that contains for sql purposes. */
    public static final String SQL_QUOTAS_REG_NAME = "sql.memory.quotas";

    /** Measures number of sql memory allocations on this node. */
    private final LongAdderMetric quotaRequestedCnt;

    /** Measures the number of bytes written to disk during offloading. */
    private final LongAdderMetric offloadingWritten;

    /** Measures the number of bytes read from disk during offloading. */
    private final LongAdderMetric offloadingRead;

    /** Measures the number of queries were offloaded. */
    private final LongAdderMetric offloadedQueriesNum;

    /**
     * Creates this metrics holder.
     *
     * @param memMgr Memory manager which tracks sql memory.
     * @param metricMgr registers and exports outside this class metrics.
     */
    public SqlMemoryStatisticsHolder(QueryMemoryManager memMgr, GridMetricManager metricMgr) {
        MetricRegistry quotasMetrics = metricMgr.registry(SQL_QUOTAS_REG_NAME);
        quotaRequestedCnt = quotasMetrics.longAdderMetric("requests",
            "How many times memory quota have been requested on this node by all the queries in total.");

        offloadingWritten = quotasMetrics.longAdderMetric("OffloadingWritten",
            "Metrics that indicates the number of bytes written to the disk during SQL query offloading.");
        offloadingRead = quotasMetrics.longAdderMetric("OffloadingRead",
            "Metrics that indicates the number of bytes read from the disk during SQL query offloading.");
        offloadedQueriesNum = quotasMetrics.longAdderMetric("OffloadedQueriesNumber",
            "Metrics that indicates the number of queries were offloaded to disk locally.");

        quotasMetrics.register("maxMem",
            new LongSupplier() {
                @Override public long getAsLong() {
                    return memMgr.memoryLimit();
                }
            },
            "How much memory in bytes it is possible to reserve by all the queries in total on this node. " +
                "Negative value if sql memory quotas are disabled. " +
                "Individual queries have additional per query quotas."
        );

        quotasMetrics.register("freeMem",
            new LongSupplier() {
                @Override public long getAsLong() {
                    return memMgr.freeMemory();
                }
            },
            "How much memory in bytes currently left available for the queries on this node. " +
                "Negative value if sql memory quotas are disabled."
        );
    }

    /**
     * Updates statistics when memory is reserved for any query. Thread safe.
     */
    public void trackReserve() {
        quotaRequestedCnt.increment();
    }

    /**
     * Updates statistics for bytes written to the disk during offloading.
     * @param written Bytes written.
     */
    public void trackOffloadingWritten(long written) {
        offloadingWritten.add(written);
    }

    /**
     * Updates statistics for bytes read from the disk during offloading.
     * @param read Bytes read.
     */
    public void trackOffloadingRead(long read) {
        offloadingRead.add(read);
    }

    /**
     * Increments the number of offloaded queries
     */
    public void trackQueryOffloaded() {
        offloadedQueriesNum.increment();
    }
}
