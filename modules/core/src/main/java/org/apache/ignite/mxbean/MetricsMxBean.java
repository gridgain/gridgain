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

package org.apache.ignite.mxbean;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.spi.metric.HistogramMetric;

/**
 * Metrics MXBean interface.
 */
@MXBeanDescription("MBean that provides access to Ignite metrics management methods.")
public interface MetricsMxBean {
    /**
     * Resets metrics for of a given registry.
     *
     * @param registry Metrics registry name.
     */
    @MXBeanDescription("Resets metrics of a given registry.")
    public void resetMetrics(
        @MXBeanParameter(name = "registry", description = "Metrics registry.") String registry
    );

    /**
     * Change {@link HitRateMetric} configuration.
     * Call of this method will change metric configuration across all cluster nodes.
     *
     * @param name Metric name.
     * @param rateTimeInterval New rate time interval.
     * @throws IgniteException If some error occured.
     */
    @MXBeanDescription("Configure hitrate metric.")
    public void configureHitRateMetric(
        @MXBeanParameter(name = "name", description = "Metric name.") String name,
        @MXBeanParameter(name = "cfg", description = "New rate time interval.") long rateTimeInterval
    ) throws IgniteException;

    /**
     * Change {@link HistogramMetric} configuration.
     * Call of this method will change metric configuration across all cluster nodes.
     *
     * @param name Metric name.
     * @param bounds New bounds.
     * @throws IgniteException If some error occured.
     */
    @MXBeanDescription("Configure histogram metric.")
    public void configureHistogramMetric(
        @MXBeanParameter(name = "name", description = "Metric name.") String name,
        @MXBeanParameter(name = "cfg", description = "New bounds.") long[] bounds
    ) throws IgniteException;
}
