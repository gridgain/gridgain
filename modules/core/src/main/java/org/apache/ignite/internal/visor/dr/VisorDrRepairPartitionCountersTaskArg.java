/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.visor.dr;

import static org.apache.ignite.internal.util.IgniteUtils.readSet;
import static org.apache.ignite.internal.util.IgniteUtils.writeCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DR repair partition counters task arguments.
 */
public class VisorDrRepairPartitionCountersTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Caches. */
    private Set<String> caches;

    /** Batch size. */
    private int batchSize;

    /** Keep binary flag. */
    private boolean keepBinary;

    /**
     * Default constructor.
     *
     * @param caches Cache names.
     * @param batchSize Batch size.
     * @param keepBinary Keep binary flag.
     */
    public VisorDrRepairPartitionCountersTaskArg(Set<String> caches, int batchSize,
            boolean keepBinary) {
        this.batchSize = batchSize;
        this.keepBinary = keepBinary;
        this.caches = caches;
    }

    /**
     * Default constructor.
     */
    public VisorDrRepairPartitionCountersTaskArg() {
        //No-op
    }

    /**
     * @return Caches.
     */
    public Set<String> getCaches() {
        return caches;
    }

    /**
     * @return BatchSize.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * @return KeepBinary flag.
     */
    public boolean isKeepBinary() {
        return keepBinary;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        writeCollection(out, caches);
        out.writeInt(batchSize);
        out.writeBoolean(keepBinary);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        caches = readSet(in);
        batchSize = in.readInt();
        keepBinary = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrRepairPartitionCountersTaskArg.class, this);
    }
}
