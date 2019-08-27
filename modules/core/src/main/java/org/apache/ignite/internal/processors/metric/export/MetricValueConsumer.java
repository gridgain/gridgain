package org.apache.ignite.internal.processors.metric.export;

import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;

/**
 * <p>Visitor interface that should be implemented and passed to
 * {@link MetricResponse#processData(MetricSchema, MetricValueConsumer)} method.</p>
 *
 * <p>Despite the fact that metric types set is more than {@code boolean}, {@code double}, {@code int}, and
 * {@code long}, all complex metrics (e.g. {@link HistogramMetric}) can be represented as set of primitive types.</p>
 */
public interface MetricValueConsumer {
    /**
     * Callback for {@code boolean} metric value.
     *
     * @param name Metric name.
     * @param val Metric value.
     */
    public void onBoolean(String name, boolean val);

    /**
     * Callback for {@code int} metric value.
     *
     * @param name Metric name.
     * @param val Metric value.
     */
    public void onInt(String name, int val);

    /**
     * Callback for {@code long} metric value.
     *
     * @param name Metric name.
     * @param val Metric value.
     */
    public void onLong(String name, long val);

    /**
     * Callback for {@code double} metric value.
     *
     * @param name Metric name.
     * @param val Metric value.
     */
    public void onDouble(String name, double val);
}
