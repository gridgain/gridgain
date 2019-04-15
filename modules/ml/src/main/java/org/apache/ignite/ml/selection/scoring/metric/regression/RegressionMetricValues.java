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

package org.apache.ignite.ml.selection.scoring.metric.regression;

import org.apache.ignite.ml.selection.scoring.metric.MetricValues;

/**
 * Provides access to regression metric values.
 */
public class RegressionMetricValues implements MetricValues {
    /** Mean absolute error. */
    private double mae;

    /** Mean squared error. */
    private double mse;

    /** Residual sum of squares. */
    private double rss;

    /** Root mean squared error. */
    private double rmse;

    /** Coefficient of determination. */
    private double r2;

    /**
     * Initalize an instance.
     *
     * @param totalAmount Total amount of observations.
     * @param rss         Residual sum of squares.
     * @param mae         Mean absolute error.
     * @param r2          Coefficient of determintaion.
     */
    public RegressionMetricValues(int totalAmount, double rss, double mae, double r2) {
        this.rss = rss;
        this.mse = rss / totalAmount;
        this.rmse = Math.sqrt(this.mse);
        this.mae = mae;
        this.r2 = r2;
    }

    /** Returns mean absolute error. */
    public double mae() {
        return mae;
    }

    /** Returns mean squared error. */
    public double mse() {
        return mse;
    }

    /** Returns residual sum of squares. */
    public double rss() {
        return rss;
    }

    /** Returns root mean squared error. */
    public double rmse() {
        return rmse;
    }

    /** Returns coefficient of determination. */
    public double r2() {
        return r2;
    }
}
