/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * 
 * Commons Clause Restriction
 * 
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 * 
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 * 
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.selection.scoring.metric.classification;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.apache.commons.math3.util.Pair;
import org.apache.ignite.ml.selection.scoring.LabelPair;
import org.apache.ignite.ml.selection.scoring.metric.AbstractMetrics;
import org.apache.ignite.ml.selection.scoring.metric.exceptions.UnknownClassLabelException;

/**
 * Binary classification metrics calculator.
 * It could be used in two ways: to caculate all binary classification metrics or specific metric.
 */
public class BinaryClassificationMetrics extends AbstractMetrics<BinaryClassificationMetricValues> {
    /** Positive class label. */
    private double positiveClsLb = 1.0;

    /** Negative class label. Default value is 0.0. */
    private double negativeClsLb;

    /** This flag enabels ROC AUC calculation that is hard for perfromance due to internal implementation. */
    private boolean enableROCAUC;

    {
        metric = BinaryClassificationMetricValues::accuracy;
    }
    /**
     * Calculates binary metrics values.
     *
     * @param iter Iterator that supplies pairs of truth values and predicated.
     * @return Scores for all binary metrics.
     */
    @Override public BinaryClassificationMetricValues scoreAll(Iterator<LabelPair<Double>> iter) {
        long tp = 0;
        long tn = 0;
        long fp = 0;
        long fn = 0;
        double rocauc = Double.NaN;

        // for ROC AUC calculation
        long pos = 0;
        long neg = 0;
        PriorityQueue<Pair<Double, Double>> queue = new PriorityQueue<>(Comparator.comparingDouble(Pair::getKey));

        while (iter.hasNext()) {
            LabelPair<Double> e = iter.next();

            double prediction = e.getPrediction();
            double truth = e.getTruth();

            if (prediction != negativeClsLb && prediction != positiveClsLb)
                throw new UnknownClassLabelException(prediction, positiveClsLb, negativeClsLb);
            if (truth != negativeClsLb && truth != positiveClsLb)
                throw new UnknownClassLabelException(truth, positiveClsLb, negativeClsLb);

            if (truth == positiveClsLb && prediction == positiveClsLb) tp++;
            else if (truth == positiveClsLb && prediction == negativeClsLb) fn++;
            else if (truth == negativeClsLb && prediction == negativeClsLb) tn++;
            else if (truth == negativeClsLb && prediction == positiveClsLb) fp++;


            if(enableROCAUC) {
                queue.add(new Pair<>(prediction, truth));

                if (truth == positiveClsLb)
                    pos++;
                else if (truth == negativeClsLb)
                    neg++;
                else
                    throw new UnknownClassLabelException(truth, positiveClsLb, negativeClsLb);
            }

        }

        if (enableROCAUC)
            rocauc = ROCAUC.calculateROCAUC(queue, pos, neg, positiveClsLb);


        return new BinaryClassificationMetricValues(tp, tn, fp, fn, rocauc);
    }

    /** */
    public double positiveClsLb() {
        return positiveClsLb;
    }

    /** */
    public BinaryClassificationMetrics withPositiveClsLb(double positiveClsLb) {
        if (Double.isFinite(positiveClsLb))
            this.positiveClsLb = positiveClsLb;
        return this;
    }

    /** */
    public double negativeClsLb() {
        return negativeClsLb;
    }

    /** */
    public BinaryClassificationMetrics withNegativeClsLb(double negativeClsLb) {
        if (Double.isFinite(negativeClsLb))
            this.negativeClsLb = negativeClsLb;
        return this;
    }

    /** */
    public BinaryClassificationMetrics withEnablingROCAUC(boolean enableROCAUC) {
        this.enableROCAUC = this.enableROCAUC;
        return this;
    }


    /** */
    public boolean isROCAUCenabled() {
        return enableROCAUC;
    }


    /** {@inheritDoc} */
    @Override public String name() {
        return "Binary classification metrics";
    }
}
