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
import org.apache.ignite.ml.selection.scoring.evaluator.context.EmptyContext;
import org.apache.ignite.ml.structures.LabeledVector;

public class RegressionMetricStatsAggregator implements MetricStatsAggregator<Double, EmptyContext, RegressionMetricStatsAggregator> {
    private long N = 0;
    private double absoluteError = Double.NaN;
    private double rss = Double.NaN;
    private double sumOfYs = Double.NaN;
    private double sumOfSquaredYs = Double.NaN;

    public RegressionMetricStatsAggregator() {
    }

    public RegressionMetricStatsAggregator(long n, double absoluteError, double rss, double sumOfYs,
        double sumOfSquaredYs) {
        N = n;
        this.absoluteError = absoluteError;
        this.rss = rss;
        this.sumOfYs = sumOfYs;
        this.sumOfSquaredYs = sumOfSquaredYs;
    }

    @Override public void aggregate(IgniteModel<Vector, Double> model, LabeledVector<Double> vector) {
        N += 1;
        Double prediction = model.predict(vector.features());
        Double truth = vector.label();
        double error = truth - prediction;
        System.out.println("[D] Prediction = " + prediction + "; Truth = " + truth + "; Error = " + Math.pow(error, 2.0));

        absoluteError = sum(Math.abs(error), absoluteError);
        rss = sum(Math.pow(error, 2), rss);
        sumOfYs = sum(truth, sumOfYs);
        sumOfSquaredYs = sum(Math.pow(truth, 2), sumOfSquaredYs);
    }

    @Override public RegressionMetricStatsAggregator mergeWith(RegressionMetricStatsAggregator other) {
        long n = this.N + other.N;
        double absoluteError = sum(this.absoluteError, other.absoluteError);
        double squaredError = sum(this.rss, other.rss);
        double sumOfYs = sum(this.sumOfYs, other.sumOfYs);
        double sumOfSquaredYs = sum(this.sumOfSquaredYs, other.sumOfSquaredYs);

        return new RegressionMetricStatsAggregator(n, absoluteError, squaredError, sumOfYs, sumOfSquaredYs);
    }

    @Override public EmptyContext initialContext() {
        return new EmptyContext();
    }

    @Override public void initByContext(EmptyContext context) {

    }

    public double getMAE() {
        return absoluteError / Math.max(N, 1);
    }

    public double getMSE() {
        return rss / Math.max(N, 1);
    }

    public double ssReg() {
        return rss;
    }

    public double ssTot() {
        return ysVariance() * Math.max(N, 1);
    }

    public double ysVariance() {
        return (sumOfSquaredYs - Math.pow(sumOfYs, 2)) / Math.max(N, 1);
    }

    public double getRss() {
        return rss;
    }

    private double sum(double v1, double v2) {
        if (Double.isNaN(v1))
            return v2;
        else if (Double.isNaN(v2))
            return v1;
        else
            return v1 + v2;
    }
}
