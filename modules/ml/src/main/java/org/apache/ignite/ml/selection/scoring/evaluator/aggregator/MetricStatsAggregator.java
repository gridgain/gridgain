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

package org.apache.ignite.ml.selection.scoring.evaluator.aggregator;

import java.io.Serializable;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Classes with such interface are responsible for metric statistics aggregations used for metric computations
 * over given dataset.
 *
 * @param <L> Label class.
 * @param <Ctx> Context class.
 * @param <Self> Aggregator class.
 */
public interface MetricStatsAggregator<L, Ctx, Self extends MetricStatsAggregator<L, Ctx, ? super Self>> extends Serializable {
    /**
     * Aggregates statistics for metric computation given model and vector with answer.
     *
     * @param model Model.
     * @param vector Vector.
     */
    public void aggregate(IgniteModel<Vector, L> model, LabeledVector<L> vector);

    /**
     * Merges statistics of two aggregators to new aggreagator.
     *
     * @param other Other aggregator.
     * @return New aggregator.
     */
    public Self mergeWith(Self other);

    /**
     * Returns unitialized context.
     *
     * @return Unitialized evaluation context.
     */
    public Ctx createUnitializedContext();

    /**
     * Inits this aggtegator by evaluation context.
     * @param ctx Evaluation context.
     */
    public void initByContext(Ctx ctx);
}
