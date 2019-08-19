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

package org.apache.ignite.ml.selection.scoring.evaluator.aggregator;

import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.scoring.evaluator.context.BinaryClassificationEvaluationContext;
import org.apache.ignite.ml.structures.LabeledVector;

public class BinaryClassificationPointwiseMetricStatsAggregator implements MetricStatsAggregator<Double, BinaryClassificationEvaluationContext, BinaryClassificationPointwiseMetricStatsAggregator> {
    private Double falseLabel = Double.NaN;
    private Double truthLabel = Double.NaN;

    private int truePositive, falsePositive, trueNegative, falseNegative;

    public BinaryClassificationPointwiseMetricStatsAggregator() {
    }

    public BinaryClassificationPointwiseMetricStatsAggregator(Double falseLabel, Double truthLabel, int truePositive, int falsePositive, int trueNegative, int falseNegative) {
        this.falseLabel = falseLabel;
        this.truthLabel = truthLabel;
        this.truePositive = truePositive;
        this.falsePositive = falsePositive;
        this.trueNegative = trueNegative;
        this.falseNegative = falseNegative;
    }

    @Override public void aggregate(IgniteModel<Vector, Double> model, LabeledVector<Double> vector) {
        Double modelAns = model.predict(vector.features());
        Double realAns = vector.label();

        if (modelAns.equals(falseLabel) && realAns.equals(falseLabel))
            trueNegative += 1;
        else if (modelAns.equals(falseLabel) && realAns.equals(truthLabel))
            falseNegative += 1;
        else if (modelAns.equals(truthLabel) && realAns.equals(truthLabel))
            truePositive += 1;
        else if (modelAns.equals(truthLabel) && realAns.equals(falseLabel))
            falsePositive += 1;
    }

    @Override public BinaryClassificationPointwiseMetricStatsAggregator mergeWith(BinaryClassificationPointwiseMetricStatsAggregator other) {
        return new BinaryClassificationPointwiseMetricStatsAggregator(
            this.falseLabel,
            this.truthLabel,
            this.truePositive + other.truePositive,
            this.falsePositive + other.falsePositive,
            this.trueNegative + other.trueNegative,
            this.falseNegative + other.falseNegative
        );
    }

    @Override public BinaryClassificationEvaluationContext initialContext() {
        return new BinaryClassificationEvaluationContext();
    }

    @Override public void initByContext(BinaryClassificationEvaluationContext context) {
        this.falseLabel = context.getFirstClassLbl();
        this.truthLabel = context.getSecondClassLbl();
    }

    public Double getFalseLabel() {
        return falseLabel;
    }

    public Double getTruthLabel() {
        return truthLabel;
    }

    public int getTruePositive() {
        return truePositive;
    }

    public int getFalsePositive() {
        return falsePositive;
    }

    public int getTrueNegative() {
        return trueNegative;
    }

    public int getFalseNegative() {
        return falseNegative;
    }

    public int getN() {
        return truePositive + falsePositive + trueNegative + falseNegative;
    }
}
