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

import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.feature.ObjectHistogram;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.apache.ignite.ml.tree.randomforest.data.impurity.basic.CountersHistogram;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.math3.util.Precision.EPSILON;

/**
 * Class contains implementation of splitting point finding algorithm based on Gini metric (see
 * https://en.wikipedia.org/wiki/Gini_coefficient) and represents a set of histograms in according to this metric.
 */
public class GiniHistogram extends ImpurityHistogram implements ImpurityComputer<BootstrappedVector, GiniHistogram> {
    /** Serial version uid. */
    private static final long serialVersionUID = 5780670356098827667L;

    /** Bucket meta. */
    private final BucketMeta bucketMeta;

    /** Sample id. */
    private final int sampleId;

    /** Hists of counters for each labels. */
    private final ArrayList<ObjectHistogram<BootstrappedVector>> hists;

    /** Label mapping to internal representation. */
    private final Map<Double, Integer> lblMapping;

    /** Bucket ids. */
    private final Set<Integer> bucketIds;

    /**
     * Creates an instance of GiniHistogram.
     *
     * @param sampleId Sample id.
     * @param lblMapping Label mapping.
     * @param bucketMeta Bucket meta.
     */
    public GiniHistogram(int sampleId, Map<Double, Integer> lblMapping, BucketMeta bucketMeta) {
        super(bucketMeta.getFeatureMeta().getFeatureId());
        this.hists = new ArrayList<>(lblMapping.size());
        this.sampleId = sampleId;
        this.bucketMeta = bucketMeta;
        this.lblMapping = lblMapping;
        this.bucketIds = new TreeSet<>();

        for (int i = 0; i < lblMapping.size(); i++)
            hists.add(new CountersHistogram(bucketIds, bucketMeta, featureId, sampleId));
    }

    /** {@inheritDoc} */
    @Override public void addElement(BootstrappedVector vector) {
        Integer lblId = lblMapping.get(vector.label());
        hists.get(lblId).addElement(vector);
    }

    /** {@inheritDoc} */
    @Override public Optional<Double> getValue(Integer bucketId) {
        throw new IllegalStateException("Gini histogram doesn't support 'getValue' method");
    }

    /** {@inheritDoc} */
    @Override public GiniHistogram plus(GiniHistogram other) {
        GiniHistogram res = new GiniHistogram(sampleId, lblMapping, bucketMeta);
        res.bucketIds.addAll(this.bucketIds);
        res.bucketIds.addAll(other.bucketIds);
        for (int i = 0; i < hists.size(); i++)
            res.hists.set(i, this.hists.get(i).plus(other.hists.get(i)));
        return res;
    }

    /** {@inheritDoc} */
    @Override public Optional<NodeSplit> findBestSplit() {
        if (bucketIds.size() < 2)
            return Optional.empty();

        double bestSplitVal = Double.NEGATIVE_INFINITY;
        int bestBucketId = -1;
        double bestGain = 0;
        double nodeImpurity = 0;

        List<TreeMap<Integer, Double>> countersDistribPerCls = hists.stream()
            .map(ObjectHistogram::computeDistributionFunction)
            .collect(Collectors.toList());

        double[] totalSampleCntPerLb = countersDistribPerCls.stream()
            .mapToDouble(x -> x.isEmpty() ? 0.0 : x.lastEntry().getValue())
            .toArray();

        double totalSampleCnt = Arrays.stream(totalSampleCntPerLb).sum();

        for (int lbId = 0; lbId < lblMapping.size(); lbId++) {
            double lblProbability = totalSampleCntPerLb[lbId] / totalSampleCnt;
            nodeImpurity += (lblProbability * (1 - lblProbability));
        }

        if (nodeImpurity < EPSILON)
            return Optional.empty();

        Map<Integer, Double> lastLeftValues = new HashMap<>();
        for (int i = 0; i < lblMapping.size(); i++)
            lastLeftValues.put(i, 0.0);

        for (Integer bucketId : bucketIds) {
            double totalToleftCnt = 0;
            double totalToRightCnt = 0;

            double leftImpurity = 0;
            double rightImpurity = 0;

            //Compute number of samples left and right in according to split by bucketId
            for (int lbId = 0; lbId < lblMapping.size(); lbId++) {
                Double left = countersDistribPerCls.get(lbId).get(bucketId);
                if (left == null)
                    left = lastLeftValues.get(lbId);

                totalToleftCnt += left;
                totalToRightCnt += totalSampleCntPerLb[lbId] - left;

                lastLeftValues.put(lbId, left);
            }

            for (int lbId = 0; lbId < lblMapping.size(); lbId++) {
                //count of samples with label [corresponding lblId] to the left of bucket
                Double toLeftCnt = countersDistribPerCls.get(lbId).getOrDefault(bucketId, lastLeftValues.get(lbId));

                if (toLeftCnt > 0) {
                    double lblLeftProbability = toLeftCnt / totalToleftCnt;
                    leftImpurity += (lblLeftProbability * (1 - lblLeftProbability));
                }

                //number of samples to the right of bucket = total samples count - toLeftCnt
                double toRightCnt = totalSampleCntPerLb[lbId] - toLeftCnt;
                if (toRightCnt > 0) {
                    double lblRightProbability = toRightCnt / totalToRightCnt;
                    rightImpurity += (lblRightProbability * (1 - lblRightProbability));
                }
            }

            double leftWeight = totalToleftCnt / (totalToleftCnt + totalToRightCnt);
            double rightWeight = totalToRightCnt / (totalToleftCnt + totalToRightCnt);

            double weightedSplitImpurity = leftImpurity * leftWeight + rightImpurity * rightWeight;

            double gain = nodeImpurity - weightedSplitImpurity;

            if (gain > bestGain) {
                bestSplitVal = bucketMeta.bucketIdToValue(bucketId);
                bestBucketId = bucketId;
                bestGain = gain;
            }
        }

        return checkAndReturnSplitValue(bestBucketId, bestSplitVal, bestGain, nodeImpurity);
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> buckets() {
        return bucketIds;
    }

    /**
     * Returns counters histogram for class-label.
     *
     * @param lbl Label.
     * @return Counters histogram for class-label.
     */
    ObjectHistogram<BootstrappedVector> getHistForLabel(Double lbl) {
        return hists.get(lblMapping.get(lbl));
    }

    /** {@inheritDoc} */
    @Override public boolean isEqualTo(GiniHistogram other) {
        HashSet<Integer> unionBuckets = new HashSet<>(buckets());
        unionBuckets.addAll(other.bucketIds);
        if (unionBuckets.size() != bucketIds.size())
            return false;

        HashSet<Double> unionMappings = new HashSet<>(lblMapping.keySet());
        unionMappings.addAll(other.lblMapping.keySet());
        if (unionMappings.size() != lblMapping.size())
            return false;

        for (Double lbl : unionMappings) {
            if (lblMapping.get(lbl) != other.lblMapping.get(lbl))
                return false;

            ObjectHistogram<BootstrappedVector> thisHist = getHistForLabel(lbl);
            ObjectHistogram<BootstrappedVector> otherHist = other.getHistForLabel(lbl);
            if (!thisHist.isEqualTo(otherHist))
                return false;
        }

        return true;
    }

}
