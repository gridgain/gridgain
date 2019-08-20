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

package org.apache.ignite.internal.processors.metric.export;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.logger.NullLogger;
import org.junit.Test;

//TODO: remove class
public class MetricExporterMessageSizeTest {

    public static final int CACHE_CNT = 10;
    public static final UUID CLUSTER_ID = UUID.randomUUID();

    @Test
    public void testMessageSize() throws Exception {
        Map<String, MetricRegistry> metrics = new TreeMap<>();


        for (int i = 0; i < CACHE_CNT; i++) {
            String regName = "cache" + i;

            metrics.put(regName, createMetricRegistry(regName));
        }

        MetricExporter exp = new MetricExporter(new StandaloneGridKernalContext(new NullLogger(), null, null));

        for (int i = 0; i < 500; i++) {
            long start = System.currentTimeMillis();

            doIteration(exp, metrics);

            System.out.println(">>> Iteration " + (i + 1) + ": " + (System.currentTimeMillis() - start));
        }
    }

    private void doIteration(MetricExporter exp, Map<String, MetricRegistry> metrics) throws IOException {
        MetricResponse msg = exp.metricMessage(CLUSTER_ID, "someUserTag", "someConsistentId", metrics);

/*
        int iterCnt = 100;

        long total = 0;

        for (int i = 0; i < iterCnt; i++) {
            long cur = System.currentTimeMillis();

            MetricResponse msg = exp.metricMessage(UUID.randomUUID(), "someUserTag" + i, "someConsistentId", metrics);


            System.out.println(zip(msg.body).length);

            total += System.currentTimeMillis() - cur;
        }

        System.out.println("Time: " + total / iterCnt);
*/

        System.out.println("Message size: " + msg.size());

        System.out.println("MetricRegistrySchema size: " + msg.schemaSize());

        System.out.println("Data size: " + msg.dataSize());

        //System.out.println("ZIP size: " + zip(msg.body).length);
    }

    byte[] zip(byte[] arr) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ZipOutputStream zos = new ZipOutputStream(baos);

        ZipEntry e = new ZipEntry("msg");

        e.setSize(arr.length);

        zos.putNextEntry(e);

        zos.write(arr);

        zos.closeEntry();

        zos.close();

        return baos.toByteArray();
    }

    MetricRegistry createMetricRegistry(String regName) {
        Random rnd = new Random();

        MetricRegistry mreg = new MetricRegistry(regName, regName, new NullLogger());

        mreg.longMetric("CacheGets",
                "The total number of gets to the cache.").value((int)rnd.nextLong());

        mreg.longMetric("EntryProcessorPuts",
                "The total number of cache invocations, caused update.").value((int)rnd.nextLong());

        mreg.longMetric("EntryProcessorRemovals",
                "The total number of cache invocations, caused removals.").value((int)rnd.nextLong());

        mreg.longMetric("EntryProcessorReadOnlyInvocations",
                "The total number of cache invocations, caused no updates.").value((int)rnd.nextLong());

        mreg.longMetric("EntryProcessorInvokeTimeNanos",
                "The total time of cache invocations, in nanoseconds.").value((int)rnd.nextLong());

        mreg.longMetric("EntryProcessorMinInvocationTime",
                "So far, the minimum time to execute cache invokes.").value((int)rnd.nextLong());

        mreg.longMetric("EntryProcessorMaxInvocationTime",
                "So far, the maximum time to execute cache invokes.").value((int)rnd.nextLong());

        mreg.longMetric("EntryProcessorHits",
                "The total number of invocations on keys, which exist in cache.").value((int)rnd.nextLong());

        mreg.longMetric("EntryProcessorMisses",
                "The total number of invocations on keys, which don't exist in cache.").value((int)rnd.nextLong());

        mreg.longMetric("CachePuts",
                "The total number of puts to the cache.").value((int)rnd.nextLong());

        mreg.longMetric("CacheHits",
                "The number of get requests that were satisfied by the cache.").value((int)rnd.nextLong());

        mreg.longMetric("CacheMisses",
                "A miss is a get request that is not satisfied.").value((int)rnd.nextLong());

        mreg.longMetric("CacheTxCommits",
                "Total number of transaction commits.").value((int)rnd.nextLong());

        mreg.longMetric("CacheTxRollbacks",
                "Total number of transaction rollbacks.").value((int)rnd.nextLong());

        mreg.longMetric("CacheEvictions",
                "The total number of evictions from the cache.").value((int)rnd.nextLong());

        mreg.longMetric("CacheRemovals", "The total number of removals from the cache.").value((int)rnd.nextLong());

        mreg.longMetric("PutTime",
                "The total time of cache puts, in nanoseconds.").value((int)rnd.nextLong());

        mreg.longMetric("GetTime",
                "The total time of cache gets, in nanoseconds.").value((int)rnd.nextLong());

        mreg.longMetric("RemovalTime",
                "The total time of cache removal, in nanoseconds.").value((int)rnd.nextLong());

        mreg.longMetric("CommitTime",
                "The total time of commit, in nanoseconds.").value((int)rnd.nextLong());

        mreg.longMetric("RollbackTime",
                "The total time of rollback, in nanoseconds.").value((int)rnd.nextLong());

        mreg.longMetric("OffHeapGets",
                "The total number of get requests to the off-heap memory.").value((int)rnd.nextLong());

        mreg.longMetric("OffHeapPuts",
                "The total number of put requests to the off-heap memory.").value((int)rnd.nextLong());

        mreg.longMetric("OffHeapRemovals",
                "The total number of removals from the off-heap memory.").value((int)rnd.nextLong());

        mreg.longMetric("OffHeapEvictions",
                "The total number of evictions from the off-heap memory.").value((int)rnd.nextLong());

        mreg.longMetric("OffHeapHits",
                "The number of get requests that were satisfied by the off-heap memory.").value((int)rnd.nextLong());

        mreg.longMetric("OffHeapMisses",
                "A miss is a get request that is not satisfied by off-heap memory.").value((int)rnd.nextLong());

        mreg.longMetric("RebalancedKeys",
                "Number of already rebalanced keys.").value((int)rnd.nextLong());

        mreg.longMetric("TotalRebalancedBytes",
                "Number of already rebalanced bytes.").value((int)rnd.nextLong());

        mreg.longMetric("RebalanceStartTime",
                "Rebalance start time").value((int)rnd.nextLong());

        mreg.longMetric("EstimatedRebalancingKeys",
                "Number estimated to rebalance keys.").value((int)rnd.nextLong());

/*
        mreg.hitRateMetric("RebalancingKeysRate",
                "Estimated rebalancing speed in keys",
                REBALANCE_RATE_INTERVAL,
                20);
*/

/*
        mreg.hitRateMetric("RebalancingBytesRate",
                "Estimated rebalancing speed in bytes",
                REBALANCE_RATE_INTERVAL,
                20);
*/

        mreg.longMetric("RebalanceClearingPartitionsLeft",
                "Number of partitions need to be cleared before actual rebalance start.").value((int)rnd.nextLong());

        return mreg;
    }
}
