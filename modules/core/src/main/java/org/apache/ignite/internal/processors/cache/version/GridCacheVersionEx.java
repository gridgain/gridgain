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

package org.apache.ignite.internal.processors.cache.version;

import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

/**
 * Extended cache version which also has additional DR version.
 */
@IgniteCodeGeneratingFail
public class GridCacheVersionEx extends GridCacheVersion {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** DR version. */
    private GridCacheVersion drVer;

    /**
     * {@link Externalizable} support.
     */
    public GridCacheVersionEx() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param topVer Topology version.
     * @param order Order.
     * @param nodeOrder Node order.
     * @param dataCenterId Data center ID.
     * @param drVer DR version.
     * @deprecated use {@link #GridCacheVersionEx(int, long, int, byte, long, GridCacheVersion)} instead.
     */
    @Deprecated
    public GridCacheVersionEx(
        int topVer,
        long order,
        int nodeOrder,
        byte dataCenterId,
        GridCacheVersion drVer
    ) {
        super(topVer, order, nodeOrder, dataCenterId);

        assert drVer != null && !(drVer instanceof GridCacheVersionEx); // DR version can only be plain here.

        this.drVer = drVer;
    }

    /**
     * Constructor.
     *
     * @param topVer Topology version.
     * @param order Order.
     * @param nodeOrder Node order.
     * @param dataCenterId Data center ID.
     * @param updateCntr Update counter.
     * @param drVer DR version.
     */
    public GridCacheVersionEx(
        int topVer,
        long order,
        int nodeOrder,
        byte dataCenterId,
        long updateCntr,
        GridCacheVersion drVer
    ) {
        super(topVer, order, nodeOrder, dataCenterId);

        assert drVer != null && !(drVer instanceof GridCacheVersionEx); // DR version can only be plain here.

        this.drVer = drVer;

        this.updateCounter(updateCntr);
    }

    /**
     * Constructor.
     *
     * @param topVer Topology version.
     * @param nodeOrderDrId Node order and DR ID.
     * @param order Version order.
     * @param drVer DR version.
     */
    public GridCacheVersionEx(int topVer, int nodeOrderDrId, long order, GridCacheVersion drVer) {
        super(topVer, nodeOrderDrId, order);

        assert drVer != null && !(drVer instanceof GridCacheVersionEx); // DR version can only be plain here.

        this.drVer = drVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion conflictVersion() {
        return drVer;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 104;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt("nodeOrderDrId", nodeOrderDrId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("order", order))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("topVer", topVer))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMessage("drVer", drVer))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeLong("updateCounter", updateCounter))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                nodeOrderDrId = reader.readInt("nodeOrderDrId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                order = reader.readLong("order");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                topVer = reader.readInt("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                drVer = reader.readMessage("drVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                updateCounter = reader.readLong("updateCounter");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridCacheVersionEx.class);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        readExternalV1(in);

        drVer = new GridCacheVersion();

        drVer.readExternal(in);

        readExternalV2(in);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        writeExternalV1(out);

        drVer.writeExternal(out);

        writeExternalV2(out);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridCacheVersionEx [topVer=" + topologyVersion() +
            ", order=" + order() +
            ", nodeOrder=" + nodeOrder() +
            ", drVer=" + drVer + ']';
    }
}
