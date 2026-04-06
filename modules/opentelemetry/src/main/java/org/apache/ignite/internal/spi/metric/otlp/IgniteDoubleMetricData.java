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
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.DoubleExemplarData;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.resources.Resource;
import java.util.List;
import org.apache.ignite.spi.metric.DoubleMetric;

import static io.opentelemetry.sdk.metrics.data.MetricDataType.DOUBLE_GAUGE;
import static java.util.Collections.emptyList;

/**
 * Metric data that holds double metric.
 */
class IgniteDoubleMetricData extends IgniteMetricData<DoubleMetric> {
    private final Data<IgniteDoublePointData> data;

    IgniteDoubleMetricData(Resource resource, InstrumentationScopeInfo scope, DoubleMetric metric) {
        super(resource, scope, metric);

        data = new IgniteGaugeData<>(new IgniteDoublePointData(metric));
    }

    /** {@inheritDoc} */
    @Override public MetricDataType getType() {
        return DOUBLE_GAUGE;
    }

    /** {@inheritDoc} */
    @Override public Data<?> getData() {
        return data;
    }

    private static class IgniteDoublePointData extends IgnitePointData implements DoublePointData {
        private final DoubleMetric metric;

        IgniteDoublePointData(DoubleMetric metric) {
            this.metric = metric;
        }

        /** {@inheritDoc} */
        @Override public double getValue() {
            return metric.value();
        }

        /** {@inheritDoc} */
        @Override public List<DoubleExemplarData> getExemplars() {
            return emptyList();
        }
    }
}
