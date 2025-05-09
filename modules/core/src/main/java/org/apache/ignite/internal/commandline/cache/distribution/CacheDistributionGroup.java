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
package org.apache.ignite.internal.commandline.cache.distribution;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * DTO for CacheDistributionTask, contains information about group
 */
public class CacheDistributionGroup extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Group identifier. */
    private int grpId;

    /** Group name. */
    private String grpName;

    /** List of partitions. */
    private List<CacheDistributionPartition> partitions;

    /** list of cache names that belong to the cache group. */
    private List<String> cacheNames;

    /** Default constructor. */
    public CacheDistributionGroup() {
    }

    /**
     * @param grpId Group identifier.
     * @param grpName Group name.
     * @param cacheNames list of cache names that belong to the cache group.
     * @param partitions List of partitions.
     */
    public CacheDistributionGroup(
        int grpId,
        String grpName,
        List<String> cacheNames,
        List<CacheDistributionPartition> partitions
    ) {
        this.grpId = grpId;
        this.grpName = grpName;
        this.partitions = partitions;
        this.cacheNames = cacheNames;
    }

    /** */
    public int getGroupId() {
        return grpId;
    }

    /** */
    public void setGroupId(int grpId) {
        this.grpId = grpId;
    }

    /** */
    public String getGroupName() {
        return grpName;
    }

    /** */
    public void setGroupName(String grpName) {
        this.grpName = grpName;
    }

    /** */
    public List<CacheDistributionPartition> getPartitions() {
        return partitions;
    }

    /** */
    public void setPartitions(
        List<CacheDistributionPartition> partitions) {
        this.partitions = partitions;
    }

    /**
     * Returns a list of cache names that belong to the cache group.
     * @return
     */
    public List<String> cacheNames() {
        return cacheNames;
    }

    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(grpId);
        U.writeString(out, grpName);
        U.writeCollection(out, partitions);
        U.writeCollection(out, cacheNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        grpId = in.readInt();
        grpName = U.readString(in);
        partitions = U.readList(in);

        if (protoVer > V1)
            cacheNames = U.readList(in);
    }
}
