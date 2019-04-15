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

package org.apache.ignite.ml.tree.randomforest.data.impurity.basic;

import java.util.Set;
import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.feature.ObjectHistogram;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;

/**
 * Histogram for bootstrapped vectors with predefined bucket mapping logic for feature id == featureId.
 */
public abstract class BootstrappedVectorsHistogram extends ObjectHistogram<BootstrappedVector> {
    /** Serial version uid. */
    private static final long serialVersionUID = 6369546706769440897L;

    /** Bucket ids. */
    protected final Set<Integer> bucketIds;

    /** Bucket meta. */
    protected final BucketMeta bucketMeta;

    /** Feature id. */
    protected final int featureId;

    /**
     * Creates an instance of BootstrappedVectorsHistogram.
     *
     * @param bucketIds Bucket ids.
     * @param bucketMeta Bucket meta.
     * @param featureId Feature Id.
     */
    public BootstrappedVectorsHistogram(Set<Integer> bucketIds, BucketMeta bucketMeta, int featureId) {
        this.bucketIds = bucketIds;
        this.bucketMeta = bucketMeta;
        this.featureId = featureId;
    }

    /** {@inheritDoc} */
    @Override public Integer mapToBucket(BootstrappedVector vec) {
        int bucketId = bucketMeta.getBucketId(vec.features().get(featureId));
        this.bucketIds.add(bucketId);
        return bucketId;
    }
}
