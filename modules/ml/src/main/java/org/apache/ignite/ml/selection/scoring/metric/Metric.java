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

package org.apache.ignite.ml.selection.scoring.metric;

import java.io.Serializable;
import org.apache.ignite.ml.selection.scoring.evaluator.aggregator.MetricStatsAggregator;
import org.apache.ignite.ml.selection.scoring.evaluator.context.EvaluationContext;

/**
 * This class represents a container with computed value of metric and it provides a factory for metric
 * statistics aggregation class.
 *
 * @param <L> Type of label.
 * @param <C> Type of evaluation context.
 * @param <A> Type of statistics aggregator.
 */
public interface Metric<L, C extends EvaluationContext<L, C>, A extends MetricStatsAggregator<L, C, A>> extends Serializable {
    /**
     * Creates statistics aggregator.
     *
     * @return Statistics aggregator
     */
    public A makeAggregator();

    /**
     * Initializes metric value by statistics aggregator.
     *
     * @param aggr Aggregator.
     */
    public Metric<L, C, A> initBy(A aggr);

    /**
     * Returns metric value.
     *
     * @return Metric value.
     */
    public double value();

    /**
     * Returns metric name.
     *
     * @return Metric name.
     */
    public MetricName name();
}
