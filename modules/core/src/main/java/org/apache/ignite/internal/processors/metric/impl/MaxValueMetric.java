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

package org.apache.ignite.internal.processors.metric.impl;

import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Accumulates approximate maximum value statistics.
 * Calculates maximum value in last {@code timeInterval} milliseconds.
 *
 * Algorithm is based on circular array of {@code size} values, each is responsible for last corresponding time
 * subinterval of {@code timeInterval}/{@code size} milliseconds. Resulting value is the max across all subintervals.
 *
 * Implementation is nonblocking and protected from values loss.
 * Maximum relative error is 1/{@code size}.
 */
public class MaxValueMetric extends AbstractMetric implements LongMetric {
    /** Metric instance. */
    private volatile MaxValueMetricImpl cntr;

    /**
     * @param name Name.
     * @param desc Description.
     * @param timeInterval Time interval in milliseconds.
     * @param size Values array size (number of buckets).
     */
    public MaxValueMetric(String name, @Nullable String desc, long timeInterval, int size) {
        super(name, desc);

        cntr = new MaxValueMetricImpl(timeInterval, size);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        MaxValueMetricImpl cntr0 = cntr;

        cntr = new MaxValueMetricImpl(cntr0.timeInterval, cntr0.size);
    }

    /**
     * Resets metric with the new parameters.
     *
     * @param timeInterval New time interval.
     * @param size New values array size.
     */
    public void reset(long timeInterval, int size) {
        cntr = new MaxValueMetricImpl(timeInterval, size);
    }

    /**
     * Accumulate x value to the metric.
     *
     * @param x Value to be accumulated.
     */
    public void update(long x) {
        cntr.update(x);
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return cntr.value();
    }

    /**
     * Actual metric implementation.
     */
    private static class MaxValueMetricImpl {
        /** Bits that store actual value. */
        private static final int TAG_OFFSET = 56;

        /** Tag mask. */
        private static final long TAG_MASK = -1L << TAG_OFFSET;

        /** Useful part mask. */
        private static final long NO_TAG_MASK = ~TAG_MASK;

        /** Time interval when values are counted, in milliseconds. */
        private final long timeInterval;

        /** Counters array size. */
        private final int size;

        /** Last value times. */
        private final AtomicLongArray lastValTimes;

        /** Tagged values. */
        private final AtomicLongArray taggedVals;

        /**
         * @param timeInterval Time interval.
         * @param size Number of buckets.
         */
        MaxValueMetricImpl(long timeInterval, int size) {
            A.ensure(timeInterval > 0, "timeInterval should be positive");
            A.ensure(size > 1, "Minimum value for size is 2");

            this.timeInterval = timeInterval;
            this.size = size;

            taggedVals = new AtomicLongArray(size);
            lastValTimes = new AtomicLongArray(size);
        }

        /**
         * Accumulates val to the metric.
         *
         * @param val Value.
         */
        void update(long val) {
            long curTs = U.currentTimeMillis();

            int curPos = position(curTs);

            clearIfObsolete(curTs, curPos);

            lastValTimes.set(curPos, curTs);

            // Order is important. Value won't be cleared by concurrent clearIfObsolete.
            accumulateBucket(curPos, val);
        }

        /**
         * @return Maximum value in last {@link #timeInterval} milliseconds.
         */
        long value() {
            long curTs = U.currentTimeMillis();

            long res = 0;

            for (int i = 0; i < size; i++) {
                clearIfObsolete(curTs, i);

                res = Math.max(res, untag(taggedVals.get(i)));
            }

            return res;
        }

        /**
         * Updates bucket with CAS to keep the maximum value.
         */
        private void accumulateBucket(int bucket, long val) {
            long oldVal;
            long val0;

            do {
                oldVal = taggedVals.get(bucket);

                val0 = (val & NO_TAG_MASK) | (oldVal & TAG_MASK);

                if (val0 <= oldVal)
                    return;
            }
            while (!taggedVals.compareAndSet(bucket, oldVal, val0));
        }

        /**
         * Clears bucket if its data is too old.
         */
        private void clearIfObsolete(long curTs, int i) {
            long cur = taggedVals.get(i);

            byte curTag = getTag(cur);

            long lastTs = lastValTimes.get(i);

            if (isObsolete(curTs, lastTs)) {
                if (taggedVals.compareAndSet(i, cur, taggedLongZero(++curTag)))
                    lastValTimes.set(i, curTs);
            }
        }

        private boolean isObsolete(long curTs, long lastValTime) {
            return curTs - lastValTime > timeInterval * (size - 1) / size;
        }

        private int position(long time) {
            return (int)((time % timeInterval * size) / timeInterval);
        }

        private static long taggedLongZero(byte tag) {
            return ((long)tag << TAG_OFFSET);
        }

        private static long untag(long l) {
            return l & NO_TAG_MASK;
        }

        private static byte getTag(long taggedLong) {
            return (byte)(taggedLong >> TAG_OFFSET);
        }
    }
}
