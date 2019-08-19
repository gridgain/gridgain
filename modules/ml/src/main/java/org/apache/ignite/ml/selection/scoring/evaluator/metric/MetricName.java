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

package org.apache.ignite.ml.selection.scoring.evaluator.metric;

import org.apache.ignite.ml.selection.scoring.evaluator.metric.classification.Accuracy;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.classification.FScore;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.classification.Precision;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.classification.Recall;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.regression.Mae;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.regression.Mse;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.regression.R2;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.regression.Rmse;

public enum MetricName {
    // binary classification metrics
    ACCURACY("Accuracy"),
    PRECISION("Precision"),
    RECALL("Recall"),
    F_MEASURE("F-measure"),

    // regression metrics
    MAE("MAE"),
    R2("R2"),
    RMSE("RMSE"),
    MSE("MSE");

    private final String prettyName;

    MetricName(String prettyName) {
        this.prettyName = prettyName;
    }

    public Metric create() {
        switch (this) {
            case ACCURACY:
                return new Accuracy();
            case PRECISION:
                return new Precision();
            case RECALL:
                return new Recall();
            case F_MEASURE:
                return new FScore();
            case MSE:
                return new Mse();
            case MAE:
                return new Mae();
            case R2:
                return new R2();
            case RMSE:
                return new Rmse();
        }

        throw new IllegalArgumentException("Cannot define metric by name: " + name());
    }

    public String getPrettyName() {
        return prettyName;
    }
}
