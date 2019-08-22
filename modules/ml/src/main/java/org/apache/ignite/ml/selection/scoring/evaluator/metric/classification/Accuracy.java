/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.selection.scoring.evaluator.metric.classification;

import java.io.Serializable;
import org.apache.ignite.ml.selection.scoring.evaluator.aggregator.BinaryClassificationPointwiseMetricStatsAggregator;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.MetricName;

/**
 * Accuracy metric class.
 */
public class Accuracy<L extends Serializable> extends BinaryClassificationMetric<L> {
    /** Serial version uid. */
    private static final long serialVersionUID = -7042505196665295151L;

    /**
     * Value.
     */
    private Double accuracy = Double.NaN;

    /**
     * Creates an instance of Accuracy metric.
     *
     * @param truthLabel Truth label.
     * @param falseLabel False label
     */
    public Accuracy(L truthLabel, L falseLabel) {
        super(truthLabel, falseLabel);
    }

    /**
     * Creates an instance of Accuracy metric.
     */
    public Accuracy() {
    }

    /** {@inheritDoc} */
    @Override public Accuracy<L> initBy(BinaryClassificationPointwiseMetricStatsAggregator<L> aggr) {
        accuracy = ((double) (aggr.getTruePositive() + aggr.getTrueNegative())) / aggr.getN();
        return this;
    }

    /** {@inheritDoc} */
    @Override public double value() {
        return accuracy;
    }

    /** {@inheritDoc} */
    @Override public MetricName name() {
        return MetricName.ACCURACY;
    }
}
