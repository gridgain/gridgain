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

package org.apache.ignite.ml.tree.randomforest.data.impurity;

import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Helper class for ImpurityHistograms.
 */
public abstract class ImpurityHistogram implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -8982240673834216798L;

    /** Bucket ids. */
    protected final Set<Integer> bucketIds = new TreeSet<>();

    /** Feature id. */
    protected final int featureId;

    /**
     * Creates an instance of ImpurityHistogram.
     *
     * @param featureId Feature id.
     */
    public ImpurityHistogram(int featureId) {
        this.featureId = featureId;
    }

    /**
     * Checks split value validity and return Optional-wrap of it.
     * In other case returns Optional.empty
     *
     * @param bestBucketId Best bucket id.
     * @param bestSplitVal Best split value.
     * @param bestGain Best gain.
     * @param impurity Node's impurity.
     * @return Best split value.
     */
    protected Optional<NodeSplit> checkAndReturnSplitValue(int bestBucketId, double bestSplitVal, double bestGain, double impurity) {
        if (isLastBucket(bestBucketId))
            return Optional.empty();
        else
            return Optional.of(new NodeSplit(featureId, bestSplitVal, bestGain, impurity));
    }

    /**
     * @param bestBucketId Best bucket id.
     * @return True if best found bucket is last within all bucketIds.
     */
    private boolean isLastBucket(int bestBucketId) {
        int minBucketId = Integer.MAX_VALUE;
        int maxBucketId = Integer.MIN_VALUE;
        for (Integer bucketId : bucketIds) {
            minBucketId = Math.min(minBucketId, bucketId);
            maxBucketId = Math.max(maxBucketId, bucketId);
        }

        return bestBucketId == maxBucketId;
    }
}
