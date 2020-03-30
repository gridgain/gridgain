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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cluster.ClusterNode;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Total statistics of rebalance for cache group.
 */
public class CacheGroupTotalRebalanceStatistics {
    /** Start time of rebalance in milliseconds. */
    private final AtomicLong start = new AtomicLong();

    /** End time of rebalance in milliseconds. */
    private final AtomicLong end = new AtomicLong();

    /** Rebalance statistics for suppliers. */
    private final Map<ClusterNode, CacheGroupTotalSupplierRebalanceStatistics> supStat = new ConcurrentHashMap<>();

    /**
     * Default constructor.
     */
    public CacheGroupTotalRebalanceStatistics() {
    }

    /**
     * Copying constructor.
     *
     * @param other Other instance.
     */
    public CacheGroupTotalRebalanceStatistics(CacheGroupTotalRebalanceStatistics other) {
        start.set(other.start.get());
        end.set(other.end.get());
        supStat.putAll(other.supStat);
    }

    /**
     * Updating statistics for supplier.
     *
     * @param grpStat Cache group rebalance statistics.
     */
    public void update(CacheGroupRebalanceStatistics grpStat) {
        start.getAndUpdate(s -> s == 0 ? grpStat.start() : min(grpStat.start(), s));
        end.getAndUpdate(e -> max(grpStat.end(), e));

        Map<ClusterNode, CacheGroupSupplierRebalanceStatistics> grpSupStats = grpStat.supplierStatistics();
        for (Entry<ClusterNode, CacheGroupSupplierRebalanceStatistics> grpSupStatEntry : grpSupStats.entrySet()) {
            CacheGroupTotalSupplierRebalanceStatistics totalSupStat = supStat.computeIfAbsent(
                grpSupStatEntry.getKey(),
                n -> new CacheGroupTotalSupplierRebalanceStatistics()
            );

            CacheGroupSupplierRebalanceStatistics grpSupStat = grpSupStatEntry.getValue();

            long fp = 0, hp = 0;
            for (Entry<Integer, Boolean> part : grpSupStat.partitions().entrySet()) {
                if (part.getValue())
                    fp++;
                else
                    hp++;
            }

            totalSupStat.update(
                grpSupStat.start(),
                grpSupStat.end(),
                fp,
                hp,
                grpSupStat.fullEntries(),
                grpSupStat.histEntries(),
                grpSupStat.fullBytes(),
                grpSupStat.histBytes()
            );
        }
    }

    /**
     * Return start time of rebalance in milliseconds.
     *
     * @return Start time of rebalance in milliseconds.
     */
    public long start() {
        return start.get();
    }

    /**
     * Return end time of rebalance in milliseconds.
     *
     * @return End time of rebalance in milliseconds.
     */
    public long end() {
        return end.get();
    }

    /**
     * Return rebalance statistics for suppliers.
     *
     * @return Rebalance statistics for suppliers.
     */
    public Map<ClusterNode, CacheGroupTotalSupplierRebalanceStatistics> supplierStatistics() {
        return supStat;
    }

    /**
     * Reset statistics.
     */
    public void reset() {
        start.set(0);
        end.set(0);

        supStat.clear();
    }
}
