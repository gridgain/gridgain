/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.tree.randomforest.data;

import java.io.Serializable;
import java.util.List;

/**
 * Class represents a split point for decision tree.
 */
public class NodeSplit implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 1331311529596106124L;

    /** Feature id in feature vector. */
    private final int featureId;

    /** Feature split value. */
    private final double val;

    /** Impurity at this split point. */
    private final double impurity;

    /** Gain at this split point. */
    private final double gain;

    /**
     * Creates an instance of NodeSplit.
     *
     * @param featureId Feature id.
     * @param val Feature split value.
     * @param gain Gain value.
     * @param impurity Impurity value.
     */
    public NodeSplit(int featureId, double val, double gain, double impurity) {
        this.featureId = featureId;
        this.val = val;
        this.gain = gain;
        this.impurity = impurity;
    }

    /**
     * Split node from parameter onto two children nodes.
     *
     * @param node Node.
     * @return List of children.
     */
    public List<TreeNode> split(TreeNode node) {
        List<TreeNode> children = node.toConditional(featureId, val);
        node.setImpurity(impurity);
        return children;
    }

    /**
     * Convert node to leaf.
     *
     * @param node Node.
     */
    public void createLeaf(TreeNode node) {
        node.setImpurity(impurity);
        node.toLeaf(0.0); //values will be set in last stage if training
    }

    /** */
    public double getImpurity() {
        return impurity;
    }

    /** */
    public double getGain() {
        return gain;
    }

    /** */
    public double getVal() {
        return val;
    }
}
