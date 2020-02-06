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

    /** Measures the number kilobytes written to disk during offloading. */
    private final LongAdderMetric offloadingWrittenKb;

    /** Measures the number bytes read from disk during offloading. */
    private final LongAdderMetric offloadingReadKb;

    /** Measures the number of files created during offloading. */
    private final LongAdderMetric filesNumber;

    /**
     * Creates this mertrics holder.
     *
     * @param memMgr Memory manager which tracks sql memory.
     * @param metricMgr registers and exports outside this class metrics.
     */
    public SqlMemoryStatisticsHolder(QueryMemoryManager memMgr, GridMetricManager metricMgr) {
        MetricRegistry quotasMetrics = metricMgr.registry(SQL_QUOTAS_REG_NAME);
        
        quotaRequestedCnt = quotasMetrics.longAdderMetric("requests",
            "How many times memory quota have been requested on this node by all the queries in total. " +
                "Always 0 if sql memory quotas are disabled.");

        offloadingWrittenKb = quotasMetrics.longAdderMetric("SqlOffloadingWrittenKb", "TODO"); //TODO
        offloadingReadKb = quotasMetrics.longAdderMetric("SqlOffloadingReadKb", "TODO"); //TODO
        filesNumber = quotasMetrics.longAdderMetric("SqlOffloadingFilesNumber", "TODO"); //TODO



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
                    return memMgr.memoryLimit() - memMgr.memoryReserved();
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

    public void trackOffloadingWrittenKb(long writtenKb) {
        offloadingWrittenKb.add(writtenKb);
    }

    public void trackOffloadingReadKb(long readKb) { // TODO bytes?
        offloadingReadKb.add(readKb);
    }

    public void trackFileCreated() {
        filesNumber.increment();
    }
}
