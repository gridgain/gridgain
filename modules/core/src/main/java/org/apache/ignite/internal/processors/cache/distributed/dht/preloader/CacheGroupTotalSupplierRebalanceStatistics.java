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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Total statistics for rebalance cache group by supplier.
 */
public class CacheGroupTotalSupplierRebalanceStatistics {
    /** Start time of rebalance in milliseconds. */
    private final AtomicLong start = new AtomicLong();

    /** End time of rebalance in milliseconds. */
    private final AtomicLong end = new AtomicLong();

    /** Counter of partitions received by full rebalance. */
    private final LongAdder fullParts = new LongAdder();

    /** Counter of partitions received by historical rebalance. */
    private final LongAdder histParts = new LongAdder();

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
     * @param s Start time of rebalance in milliseconds.
     * @param e End time of rebalance in milliseconds.
     * @param fp Count of partitions received by full rebalance.
     * @param hp Count of partitions received by historical rebalance.
     * @param fe Count of entries received by full rebalance.
     * @param he Count of partitions received by entries rebalance.
     * @param fb Count of bytes received by full rebalance.
     * @param hb Count of bytes received by historical rebalance.
     */
    public void update(long s, long e, long fp, long hp, long fe, long he, long fb, long hb) {
        start.getAndUpdate(prev -> prev == 0 ? s : min(s, prev));
        end.getAndUpdate(prev -> max(e, prev));

        fullParts.add(fp);
        histParts.add(hp);
        fullEntries.add(fe);
        histEntries.add(he);
        fullBytes.add(fb);
        histBytes.add(hb);
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
     * Return count of partitions received by full rebalance.
     *
     * @return Count of partitions received by full rebalance.
     */
    public long fullParts() {
        return fullParts.sum();
    }

    /**
     * Return count of partitions received by historical rebalance.
     *
     * @return Count of partitions received by historical rebalance.
     */
    public long histParts() {
        return histParts.sum();
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
