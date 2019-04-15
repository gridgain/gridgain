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

package org.apache.ignite.internal.sql.optimizer.affinity;

import java.util.Collection;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition extraction result.
 */
public class PartitionResult {
    /** Tree. */
    @GridToStringInclude
    private final PartitionNode tree;

    /** Affinity function. */
    private final PartitionTableAffinityDescriptor aff;

    /**
     * Constructor.
     *
     * @param tree Tree.
     * @param aff Affinity function.
     */
    public PartitionResult(PartitionNode tree, PartitionTableAffinityDescriptor aff) {
        this.tree = tree;
        this.aff = aff;
    }

    /**
     * Tree.
     */
    public PartitionNode tree() {
        return tree;
    }

    /**
     * @return Affinity function.
     */
    public PartitionTableAffinityDescriptor affinity() {
        return aff;
    }

    /**
     * Calculate partitions for the query.
     *
     * @param explicitParts Explicit partitions provided in SqlFieldsQuery.partitions property.
     * @param derivedParts Derived partitions found during partition pruning.
     * @param args Arguments.
     * @return Calculated partitions or {@code null} if failed to calculate and there should be a broadcast.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public static int[] calculatePartitions(int[] explicitParts, PartitionResult derivedParts, Object[] args) {
        if (!F.isEmpty(explicitParts))
            return explicitParts;
        else if (derivedParts != null) {
            try {
                Collection<Integer> realParts = derivedParts.tree().apply(null, args);

                if (realParts == null)
                    return null;
                else if (realParts.isEmpty())
                    return IgniteUtils.EMPTY_INTS;
                else {
                    int[] realParts0 = new int[realParts.size()];

                    int i = 0;

                    for (Integer realPart : realParts)
                        realParts0[i++] = realPart;

                    return realParts0;
                }
            }
            catch (IgniteCheckedException e) {
                throw new CacheException("Failed to calculate derived partitions for query.", e);
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionResult.class, this);
    }
}
