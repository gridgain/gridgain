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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DR incremental transfer task args.
 */
public class VisorDrIncrementalTransferCmdArgs extends IgniteDataTransferObject {
    /** Serial version id. */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Snapshot id. */
    private long snapshotId;

    /** Data center id. */
    private byte dcId;

    /**
     * Default constructor.
     */
    public VisorDrIncrementalTransferCmdArgs() {
        // No-op.
    }

    /** */
    public VisorDrIncrementalTransferCmdArgs(String cacheName, long snapshotId, byte dcId) {
        this.cacheName = cacheName;
        this.snapshotId = snapshotId;
        this.dcId = dcId;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeUTF(cacheName);
        out.writeLong(snapshotId);
        out.writeByte(dcId);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException {
        cacheName = in.readUTF();
        snapshotId = in.readLong();
        dcId = in.readByte();
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Snapshot id.
     */
    public long snapshotId() {
        return snapshotId;
    }

    /**
     * @return Data center id.
     */
    public byte dcId() {
        return dcId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrIncrementalTransferCmdArgs.class, this);
    }
}
