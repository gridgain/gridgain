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

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Serializable;

/**
 * Affinity function descriptor. Used to compare affinity functions of two tables.
 */
public class PartitionTableAffinityDescriptor implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Affinity function type. */
    private final PartitionAffinityFunctionType affFunc;

    /** Number of partitions. */
    private final int parts;

    /** Whether node filter is set. */
    private final boolean hasNodeFilter;

    /** Data region name. */
    private final String dataRegion;

    /**
     * Constructor.
     *
     * @param affFunc Affinity function type.
     * @param parts Number of partitions.
     * @param hasNodeFilter Whether node filter is set.
     * @param dataRegion Data region.
     */
    public PartitionTableAffinityDescriptor(
        PartitionAffinityFunctionType affFunc,
        int parts,
        boolean hasNodeFilter,
        String dataRegion
    ) {
        this.affFunc = affFunc;
        this.parts = parts;
        this.hasNodeFilter = hasNodeFilter;
        this.dataRegion = dataRegion;
    }

    /**
     * Check is provided descriptor is compatible with this instance (i.e. can be used in the same co-location group).
     *
     * @param other Other descriptor.
     * @return {@code True} if compatible.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isCompatible(PartitionTableAffinityDescriptor other) {
        if (other == null)
            return false;

        // Rendezvous affinity function is deterministic and doesn't depend on previous cluster view changes.
        // In future other user affinity functions would be applicable as well if explicityl marked deterministic.
        if (affFunc == PartitionAffinityFunctionType.RENDEZVOUS) {
            // We cannot be sure that two caches are co-located if custom node filter is present.
            // Nota that technically we may try to compare two filters. However, this adds unnecessary complexity
            // and potential deserialization issues when SQL is called from client nodes or thin clients.
            if (!hasNodeFilter) {
                return
                    other.affFunc == PartitionAffinityFunctionType.RENDEZVOUS &&
                    !other.hasNodeFilter &&
                    other.parts == parts &&
                    F.eq(other.dataRegion, dataRegion);
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionTableAffinityDescriptor.class, this);
    }
}
