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
import io.opentelemetry.sdk.metrics.data.LongExemplarData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Date;
import java.util.List;
import org.apache.ignite.spi.metric.ObjectMetric;

import static io.opentelemetry.sdk.metrics.data.MetricDataType.LONG_GAUGE;
import static java.util.Collections.emptyList;

/**
 * Metric data that holds Date metric.
 */
public class IgniteDateMetricData extends IgniteMetricData<ObjectMetric<Date>> {
    private final Data<IgniteLongPointData> data;

    IgniteDateMetricData(Resource resource, InstrumentationScopeInfo scope, ObjectMetric<Date> metric) {
        super(resource, scope, metric);

        this.data = new IgniteGaugeData<>(new IgniteLongPointData(metric));
    }

    /** {@inheritDoc} */
    @Override public MetricDataType getType() {
        return LONG_GAUGE;
    }

    /** {@inheritDoc} */
    @Override public Data<?> getData() {
        return data;
    }

    private static class IgniteLongPointData extends IgnitePointData implements LongPointData {
        private final ObjectMetric<Date> metric;

        IgniteLongPointData(ObjectMetric<Date> metric) {
            this.metric = metric;
        }

        /** {@inheritDoc} */
        @Override public long getValue() {
            return metric.value().getTime();
        }

        /** {@inheritDoc} */
        @Override public List<LongExemplarData> getExemplars() {
            return emptyList();
        }
    }
}
