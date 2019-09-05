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


package org.apache.ignite.internal.visor.cache.affinityView;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Result object for {@link VisorAffinityViewTask}
 */
public class VisorAffinityViewTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     *  Map containing partitions ids for <@code ClusterNode}>.
     *  First <@code IgniteBiTuple> item contains primary partitions ids,
     *  second item contains backup partitions ids.
     * */
    private Map<ClusterNode, IgniteBiTuple<char[], char[]>> assignment;

    /** */
    private Set<Integer> primariesDifferentToIdeal;

    /**
     * Default constructor. Required for {@link Externalizable} support.
     */
    public VisorAffinityViewTaskResult() {
        // No-op.
    }

    /** */
    public VisorAffinityViewTaskResult(Map<ClusterNode, IgniteBiTuple<char[], char[]>> assignment,
        Set<Integer> primariesDifferentToIdeal)
    {
        this.assignment = assignment;
        this.primariesDifferentToIdeal = primariesDifferentToIdeal;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, assignment);
        U.writeCollection(out, primariesDifferentToIdeal);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        assignment = U.readMap(in);
        primariesDifferentToIdeal = U.readSet(in);
    }

    /**
     * @return {@code assignment}
     */
    public Map<ClusterNode, IgniteBiTuple<char[], char[]>> getAssignment() {
        return assignment;
    }

    /**
     * @return {@code primariesDifferentToIdeal}
     */
    public Set<Integer> getPrimariesDifferentToIdeal() {
        return primariesDifferentToIdeal;
    }
}
