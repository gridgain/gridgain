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

import org.apache.ignite.ml.selection.scoring.evaluator.aggregator.BinaryClassificationPointwiseMetricStatsAggregator;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.MetricName;

public class FScore extends BinaryClassificationMetric {
    private final double betaSquare;
    private final Precision precision = new Precision();
    private final Recall recall = new Recall();

    private Double fscore = Double.NaN;

    public FScore(double beta) {
        betaSquare = Math.pow(beta, 2);
    }

    public FScore() {
        betaSquare = 1;
    }

    public FScore(double truthLabel, double falseLabel, double betaSquare) {
        super(truthLabel, falseLabel);
        this.betaSquare = betaSquare;
    }

    public FScore(double truthLabel, double falseLabel) {
        super(truthLabel, falseLabel);
        this.betaSquare = 1;
    }

    @Override public FScore initBy(BinaryClassificationPointwiseMetricStatsAggregator aggr) {
        precision.initBy(aggr);
        recall.initBy(aggr);

        double nom = (1 + betaSquare) * precision.value() * recall.value();
        double denom = (betaSquare * precision.value() + recall.value());
        fscore = nom / denom;
        return this;
    }

    @Override public double value() {
        return fscore;
    }

    @Override public MetricName name() {
        return MetricName.F_MEASURE;
    }
}
