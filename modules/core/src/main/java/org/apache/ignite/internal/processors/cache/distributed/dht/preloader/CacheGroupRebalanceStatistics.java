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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Rebalance statistics for cache group.
 */
public class CacheGroupRebalanceStatistics {
    /** Start time of rebalance in milliseconds. */
    private volatile long start;

    /** End time of rebalance in milliseconds. */
    private volatile long end;

    /** Rebalance attempt. */
    private volatile int attempt;

    /** Rebalance statistics for suppliers. */
    private final Map<ClusterNode, CacheGroupSupplierRebalanceStatistics> supplierStat = new ConcurrentHashMap<>();

    /**
     * Default constructor.
     */
    public CacheGroupRebalanceStatistics() {
    }

    /**
     * Constructor.
     *
     * @param attempt Rebalance attempt, must be greater than {@code 0}.
     */
    public CacheGroupRebalanceStatistics(int attempt) {
        assert attempt > 0;

        this.attempt = attempt;
    }

    /**
     * Copying constructor.
     *
     * @param other Other instance.
     */
    public CacheGroupRebalanceStatistics(CacheGroupRebalanceStatistics other) {
        attempt = other.attempt;
        start = other.start;
        end = other.end;
        supplierStat.putAll(other.supplierStat);
    }

    /**
     * Set start time of rebalance for supplier in milliseconds.
     *
     * @param supplierNode Supplier node.
     * @param start        Start time of rebalance in milliseconds.
     */
    public void start(ClusterNode supplierNode, long start) {
        supplierRebalanceStatistics(supplierNode).start(start);
    }

    /**
     * Set end time of rebalance for supplier in milliseconds.
     *
     * @param supplierNode Supplier node.
     * @param end          End time of rebalance in milliseconds.
     */
    public void end(ClusterNode supplierNode, long end) {
        supplierRebalanceStatistics(supplierNode).end(end);
    }

    /**
     * Set end time of rebalance for supplier in milliseconds.
     *
     * @param supplierNodeId Supplier node id.
     * @param end            End time of rebalance in milliseconds.
     */
    public void end(UUID supplierNodeId, long end) {
        for (Map.Entry<ClusterNode, CacheGroupSupplierRebalanceStatistics> supStatEntry : supplierStat.entrySet()) {
            if (supStatEntry.getKey().id().equals(supplierNodeId)) {
                supStatEntry.getValue().end(end);
                return;
            }
        }
    }

    /**
     * Updating statistics for supplier.
     *
     * @param supplierNode Supplier node.
     * @param p            Partition id.
     * @param hist         Historical or full rebalance.
     * @param e            Count of entries.
     * @param b            Count of bytes.
     */
    public void update(ClusterNode supplierNode, int p, boolean hist, long e, long b) {
        supplierRebalanceStatistics(supplierNode).update(hist, p, e, b);
    }

    /**
     * Set start time of rebalance in milliseconds.
     *
     * @param start Start time of rebalance in milliseconds.
     */
    public void start(long start) {
        this.start = start;
        end = start;
    }

    /**
     * Return start time of rebalance in milliseconds.
     *
     * @return Start time of rebalance in milliseconds.
     */
    public long start() {
        return start;
    }

    /**
     * Set end time of rebalance in milliseconds.
     *
     * @param end End time of rebalance in milliseconds.
     */
    public void end(long end) {
        this.end = end;
    }

    /**
     * Return end time of rebalance in milliseconds.
     *
     * @return End time of rebalance in milliseconds.
     */
    public long end() {
        return end;
    }

    /**
     * Return rebalance statistics for suppliers.
     *
     * @return Rebalance statistics for suppliers.
     */
    public Map<ClusterNode, CacheGroupSupplierRebalanceStatistics> supplierStatistics() {
        return supplierStat;
    }

    /**
     * Return rebalance attempt.
     *
     * @return Rebalance attempt.
     */
    public int attempt() {
        return attempt;
    }

    /**
     * Reset statistics.
     */
    public void reset() {
        start = 0L;
        end = 0L;

        supplierStat.clear();
    }

    /**
     * Reset attempt.
     */
    public void resetAttempt() {
        attempt = 0;
    }

    /**
     * Return rebalance statistics for supplier node.
     *
     * @param supplierNode Supplier node.
     * @return Rebalance statistics for supplier node.
     */
    private CacheGroupSupplierRebalanceStatistics supplierRebalanceStatistics(ClusterNode supplierNode) {
        return supplierStat.computeIfAbsent(supplierNode, n -> new CacheGroupSupplierRebalanceStatistics());
    }
}
