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

package org.apache.ignite.ml.sparkmodelparser;

/**
 * List of supported Spark models.
 *
 * It doesn't support old models from MLLib package.
 *
 * NOTE: Valid for Spark 2.2-2.4.
 */
public enum SupportedSparkModels {
    /** Logistic regression. */
    LOG_REGRESSION("org.apache.spark.ml.classification.LogisticRegressionModel"),

    /** Linear regression. */
    LINEAR_REGRESSION("org.apache.spark.ml.regression.LinearRegressionModel"),

    /** Decision tree. */
    DECISION_TREE("org.apache.spark.ml.classification.DecisionTreeClassificationModel"),

    /** Support Vector Machine . */
    LINEAR_SVM("org.apache.spark.ml.classification.LinearSVCModel"),

    /** Random forest. */
    RANDOM_FOREST("org.apache.spark.ml.classification.RandomForestClassificationModel"),

    /** K-Means. */
    KMEANS("org.apache.spark.ml.clustering.KMeansModel"),

    /** Decision tree regression. */
    DECISION_TREE_REGRESSION("org.apache.spark.ml.regression.DecisionTreeRegressionModel"),

    /** Random forest regression. */
    RANDOM_FOREST_REGRESSION("org.apache.spark.ml.regression.RandomForestRegressionModel"),

    /** Gradient boosted trees regression. */
    GRADIENT_BOOSTED_TREES_REGRESSION("org.apache.spark.ml.regression.GBTRegressionModel"),

    /**
     * Gradient boosted trees.
     * NOTE: support binary classification only with raw labels 0 and 1
     */
    GRADIENT_BOOSTED_TREES("org.apache.spark.ml.classification.GBTClassificationModel");


    /** The separator between words. */
    private final String mdlClsNameInSpark;

    SupportedSparkModels(String mdlClsNameInSpark) {
        this.mdlClsNameInSpark = mdlClsNameInSpark;
    }

    public String getMdlClsNameInSpark() {
        return mdlClsNameInSpark;
    }
}
