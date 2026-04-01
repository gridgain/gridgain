/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.spi.metric.otlp;

import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.DoubleExemplarData;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.spi.metric.HistogramMetric;

import static io.opentelemetry.sdk.metrics.data.AggregationTemporality.CUMULATIVE;
import static io.opentelemetry.sdk.metrics.data.MetricDataType.HISTOGRAM;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Metric data that holds histogram metric.
 */
class IgniteHistogramMetricData extends IgniteMetricData<HistogramMetric> {
    private final IgniteHistogramData data;

    IgniteHistogramMetricData(Resource resource, InstrumentationScopeInfo scope, HistogramMetric metric) {
        super(resource, scope, metric);

        data = new IgniteHistogramData(new IgniteDistributionPointData(metric));
    }

    /** {@inheritDoc} */
    @Override public MetricDataType getType() {
        return HISTOGRAM;
    }

    /** {@inheritDoc} */
    @Override public Data<?> getData() {
        return data;
    }

    boolean isValid() {
        return data.isValid();
    }

    static class IgniteHistogramData implements HistogramData {
        private final Collection<HistogramPointData> points;

        IgniteHistogramData(HistogramPointData data) {
            points = singletonList(data);
        }

        /** {@inheritDoc} */
        @Override public AggregationTemporality getAggregationTemporality() {
            return CUMULATIVE;
        }

        /** {@inheritDoc} */
        @Override public Collection<HistogramPointData> getPoints() {
            return points;
        }

        private boolean isValid() {
            for (HistogramPointData p : points) {
                if (((IgniteDistributionPointData) p).isBoundariesChanged())
                    return false;
            }

            return true;
        }
    }

    private static class IgniteDistributionPointData extends IgnitePointData implements HistogramPointData {
        private final HistogramMetric metric;

        private final long[] originalBoundaries;
        private final List<Double> boundaries;

        IgniteDistributionPointData(HistogramMetric metric) {
            this.metric = metric;

            this.originalBoundaries = metric.bounds();
            this.boundaries = asDoubleList(originalBoundaries);
        }

        /** {@inheritDoc} */
        @Override public double getSum() {
            return Double.NaN;
        }

        /** {@inheritDoc} */
        @Override public long getCount() {
            long[] vals = metric.value();

            if (isBoundariesChanged())
                return 0;

            long totalCount = 0;

            for (long c : vals)
                totalCount += c;

            return totalCount;
        }

        /** {@inheritDoc} */
        @Override public boolean hasMin() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public double getMin() {
            return Double.NaN;
        }

        /** {@inheritDoc} */
        @Override public boolean hasMax() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public double getMax() {
            return Double.NaN;
        }

        /** {@inheritDoc} */
        @Override public List<Double> getBoundaries() {
            return boundaries;
        }

        /** {@inheritDoc} */
        @Override public List<Long> getCounts() {
            long[] vals = metric.value();
            boolean changed = isBoundariesChanged();

            List<Long> counts = new ArrayList<>(boundaries.size() + 1);

            for (long val : vals)
                counts.add(changed ? 0 : val);

            return counts;
        }

        /** {@inheritDoc} */
        @Override public List<DoubleExemplarData> getExemplars() {
            return emptyList();
        }

        private static List<Double> asDoubleList(long[] array) {
            ArrayList<Double> result = new ArrayList<>(array.length);

            for (long el : array)
                result.add((double) el);

            return result;
        }

        /**
         * Returns {@code true} if the underlying histogram has changed in incompatible way,
         * and {@code false} otherwise.
         */
        private boolean isBoundariesChanged() {
            return originalBoundaries != metric.bounds();
        }
    }
}
