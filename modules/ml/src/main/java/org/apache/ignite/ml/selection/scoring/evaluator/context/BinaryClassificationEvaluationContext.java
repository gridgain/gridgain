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

package org.apache.ignite.ml.selection.scoring.evaluator.context;

import java.util.Arrays;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.structures.LabeledVector;

public class BinaryClassificationEvaluationContext implements EvaluationContext<Double, BinaryClassificationEvaluationContext> {
    private Double firstClassLbl;
    private Double secondClassLbl;

    public BinaryClassificationEvaluationContext() {
        this.firstClassLbl = Double.NaN;
        this.secondClassLbl = Double.NaN;
    }

    public BinaryClassificationEvaluationContext(Double firstClassLbl, Double secondClassLbl) {
        this.firstClassLbl = firstClassLbl;
        this.secondClassLbl = secondClassLbl;
    }

    @Override public void aggregate(LabeledVector<Double> vector) {
        Double label = vector.label();
        if (firstClassLbl.isNaN())
            this.firstClassLbl = label;
        else if (secondClassLbl.isNaN() && !label.equals(firstClassLbl)) {
            secondClassLbl = Math.max(firstClassLbl, label);
            firstClassLbl = Math.min(firstClassLbl, label);
        }
        else
            checkNewLabel(label);
    }

    @Override public BinaryClassificationEvaluationContext mergeWith(BinaryClassificationEvaluationContext other) {
        checkNewLabel(other.firstClassLbl);
        checkNewLabel(other.secondClassLbl);

        double[] labels = Arrays.stream(new double[] {this.firstClassLbl, this.secondClassLbl, other.firstClassLbl, other.secondClassLbl})
            .filter(x -> !Double.isNaN(x)).sorted().distinct().toArray();

        A.ensure(labels.length < 3, "labels.length < 3");
        return new BinaryClassificationEvaluationContext(
            labels.length == 0 ? Double.NaN : labels[0],
            labels.length < 2 ? Double.NaN : labels[1]
        );
    }

    public Double getFirstClassLbl() {
        return firstClassLbl;
    }

    public Double getSecondClassLbl() {
        return secondClassLbl;
    }

    private void checkNewLabel(Double label) {
        A.ensure(
            firstClassLbl.isNaN() || secondClassLbl.isNaN() || label.isNaN() ||
                label.equals(firstClassLbl) || label.equals(secondClassLbl),
            "Unable to collect binary classification ctx stat. There are more than two labels. " +
                "First label = " + firstClassLbl +
                ", second label = " + secondClassLbl +
                ", another label = " + label
        );
    }
}
