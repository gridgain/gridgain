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

package org.apache.ignite.ml.dataset.primitive.data;

import org.apache.ignite.ml.dataset.primitive.SimpleLabeledDataset;

/**
 * A partition {@code data} of the {@link SimpleLabeledDataset} containing matrix of features in flat column-major
 * format stored in heap and vector of labels stored in heap as well.
 */
public class SimpleLabeledDatasetData implements AutoCloseable {
    /** Matrix with features in a dense flat column-major format. */
    private final double[] features;

    /** Vector with labels. */
    private final double[] labels;

    /** Number of rows. */
    private final int rows;

    /**
     * Constructs a new instance of partition {@code data} of the {@link SimpleLabeledDataset} containing matrix of
     * features in flat column-major format stored in heap and vector of labels stored in heap as well.
     *
     * @param features Matrix with features in a dense flat column-major format.
     * @param labels Vector with labels.
     * @param rows Number of rows.
     */
    public SimpleLabeledDatasetData(double[] features, double[] labels, int rows) {
        this.features = features;
        this.labels = labels;
        this.rows = rows;
    }

    /** */
    public double[] getFeatures() {
        return features;
    }

    /** */
    public int getRows() {
        return rows;
    }

    /** */
    public double[] getLabels() {
        return labels;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
