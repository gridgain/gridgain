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
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import org.apache.ignite.spi.metric.Metric;

/**
 * Metric data represents the aggregated measurements of an instrument.
 *
 * @param <T> A type of the metric.
 */
abstract class IgniteMetricData<T extends Metric> implements MetricData {
    private final Resource resource;
    private final InstrumentationScopeInfo scope;
    private final T metric;

    IgniteMetricData(Resource resource, InstrumentationScopeInfo scope, T metric) {
        this.resource = resource;
        this.scope = scope;
        this.metric = metric;
    }

    /** {@inheritDoc} */
    @Override public Resource getResource() {
        return resource;
    }

    /** {@inheritDoc} */
    @Override public InstrumentationScopeInfo getInstrumentationScopeInfo() {
        return scope;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return metric.name();
    }

    /** {@inheritDoc} */
    @Override public String getDescription() {
        // Can't be null.
        return metric.description() == null ? "" : metric.description();
    }

    /** {@inheritDoc} */
    @Override public String getUnit() {
        // Can't be null.
        return "";
    }
}
