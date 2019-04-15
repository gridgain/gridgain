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

package org.apache.ignite.ml.naivebayes.gaussian;

import java.io.Serializable;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Simple naive Bayes model which predicts result value {@code y} belongs to a class {@code C_k, k in [0..K]} as {@code
 * p(C_k,y) = p(C_k)*p(y_1,C_k) *...*p(y_n,C_k) / p(y)}. Return the number of the most possible class.
 */
public class GaussianNaiveBayesModel implements IgniteModel<Vector, Double>, Exportable<GaussianNaiveBayesModel>, Serializable {
    /** */
    private static final long serialVersionUID = -127386523291350345L;
    /** Means of features for all classes. kth row contains means for labels[k] class. */
    private final double[][] means;
    /** Variances of features for all classes. kth row contains variances for labels[k] class */
    private final double[][] variances;
    /** Prior probabilities of each class */
    private final double[] classProbabilities;
    /** Labels. */
    private final double[] labels;
    /** Feature sum, squared sum and count per label. */
    private final GaussianNaiveBayesSumsHolder sumsHolder;

    /**
     * @param means Means of features for all classes.
     * @param variances Variances of features for all classes.
     * @param classProbabilities Probabilities for all classes.
     * @param labels Labels.
     * @param sumsHolder Feature sum, squared sum and count sum per label. This data is used for future model updating.
     */
    public GaussianNaiveBayesModel(double[][] means, double[][] variances,
        double[] classProbabilities, double[] labels, GaussianNaiveBayesSumsHolder sumsHolder) {
        this.means = means;
        this.variances = variances;
        this.classProbabilities = classProbabilities;
        this.labels = labels;
        this.sumsHolder = sumsHolder;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<GaussianNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** Returns a number of class to which the input belongs. */
    @Override public Double predict(Vector vector) {
        int k = classProbabilities.length;

        double maxProbability = .0;
        int max = 0;

        for (int i = 0; i < k; i++) {
            double p = classProbabilities[i];
            for (int j = 0; j < vector.size(); j++) {
                double x = vector.get(j);
                double g = gauss(x, means[i][j], variances[i][j]);
                p *= g;
            }
            if (p > maxProbability) {
                max = i;
                maxProbability = p;
            }
        }
        return labels[max];
    }

    /** */
    public double[][] getMeans() {
        return means;
    }

    /** */
    public double[][] getVariances() {
        return variances;
    }

    /** */
    public double[] getClassProbabilities() {
        return classProbabilities;
    }

    /** */
    public GaussianNaiveBayesSumsHolder getSumsHolder() {
        return sumsHolder;
    }

    /** Gauss distribution */
    private double gauss(double x, double mean, double variance) {
        return Math.exp(-1. * Math.pow(x - mean, 2) / (2. * variance)) / Math.sqrt(2. * Math.PI * variance);
    }
}
