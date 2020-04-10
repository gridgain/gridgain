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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Statistics for rebalance cache group by supplier.
 */
public class CacheGroupSupplierRebalanceStatistics {
    /** Start time of rebalance in milliseconds. */
    private volatile long start;

    /** End time of rebalance in milliseconds. */
    private volatile long end;

    /**
     * Rebalanced partitions.
     * Key - partition id, value - fully({@code true}) or
     * historically({@code false}) rebalanced.
     */
    private final Map<Integer, Boolean> parts = new ConcurrentHashMap<>();

    /** Counter of entries received by full rebalance. */
    private final LongAdder fullEntries = new LongAdder();

    /** Counter of entries received by historical rebalance. */
    private final LongAdder histEntries = new LongAdder();

    /** Counter of bytes received by full rebalance. */
    private final LongAdder fullBytes = new LongAdder();

    /** Counter of bytes received by historical rebalance. */
    private final LongAdder histBytes = new LongAdder();

    /**
     * Updating statistics.
     *
     * @param p            Partition id.
     * @param hist         Historical or full rebalance.
     * @param e            Count of entries.
     * @param b            Count of bytes.
     */
    public void update(boolean hist, int p, long e, long b) {
        parts.put(p, !hist);

        (hist ? histEntries : fullEntries).add(e);
        (hist ? histBytes : fullBytes).add(b);

        end = U.currentTimeMillis();
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
     * Returns rebalanced partitions.
     * Key - partition id, value - fully({@code true}) or
     * historically({@code false}) rebalanced.
     *
     * @return Rebalanced partitions.
     */
    public Map<Integer, Boolean> partitions() {
        return parts;
    }

    /**
     * Return count of entries received by full rebalance.
     *
     * @return Count of entries received by full rebalance.
     */
    public long fullEntries() {
        return fullEntries.sum();
    }

    /**
     * Return count of entries received by historical rebalance.
     *
     * @return Count of entries received by historical rebalance.
     */
    public long histEntries() {
        return histEntries.sum();
    }

    /**
     * Return count of bytes received by full rebalance.
     *
     * @return Count of bytes received by full rebalance.
     */
    public long fullBytes() {
        return fullBytes.sum();
    }

    /**
     * Return count of bytes received by historical rebalance.
     *
     * @return Count of bytes received by historical rebalance.
     */
    public long histBytes() {
        return histBytes.sum();
    }
}
