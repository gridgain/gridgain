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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.TimeLoggableResponse;

/**
 * Transactions recovery check response.
 */
public class GridCacheTxRecoveryResponse extends GridDistributedBaseMessage implements IgniteTxStateAware, TimeLoggableResponse {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Flag indicating if all remote transactions were prepared. */
    private boolean success;

    /** Transient TX state. */
    @GridDirectTransient
    private IgniteTxState txState;

    /** @see TimeLoggableResponse#getReqSentTimestamp(). */
    @GridDirectTransient
    private long reqSendTimestamp = INVALID_TIMESTAMP;

    /** @see TimeLoggableResponse#getReqReceivedTimestamp(). */
    @GridDirectTransient
    private long reqReceivedTimestamp = INVALID_TIMESTAMP;

    /** @see TimeLoggableResponse#getResponseSendTimestamp(). */
    private long responseSendTimestamp = INVALID_TIMESTAMP;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCacheTxRecoveryResponse() {
        // No-op.
    }

    /**
     * @param txId Transaction ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param success {@code True} if all remote transactions were prepared, {@code false} otherwise.
     * @param addDepInfo Deployment info flag.
     */
    public GridCacheTxRecoveryResponse(GridCacheVersion txId,
        IgniteUuid futId,
        IgniteUuid miniId,
        boolean success,
        boolean addDepInfo) {
        super(txId, 0, addDepInfo);

        this.futId = futId;
        this.miniId = miniId;
        this.success = success;

        this.addDepInfo = addDepInfo;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return {@code True} if all remote transactions were prepared.
     */
    public boolean success() {
        return success;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxState txState() {
        return txState;
    }

    /** {@inheritDoc} */
    @Override public void txState(IgniteTxState txState) {
        this.txState = txState;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.txRecoveryMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public void setReqSendTimestamp(long reqSendTimestamp) {
        this.reqSendTimestamp = reqSendTimestamp;
    }

    /** {@inheritDoc} */
    @Override public long getReqSentTimestamp() {
        return reqSendTimestamp;
    }

    /** {@inheritDoc} */
    @Override public void setReqReceivedTimestamp(long reqReceivedTimestamp) {
        this.reqReceivedTimestamp = reqReceivedTimestamp;
    }

    /** {@inheritDoc} */
    @Override public long getReqReceivedTimestamp() {
        return reqReceivedTimestamp;
    }

    /** {@inheritDoc} */
    @Override public void setResponseSendTimestamp(long responseSendTimestamp) {
        this.responseSendTimestamp = responseSendTimestamp;
    }

    /** {@inheritDoc} */
    @Override public long getResponseSendTimestamp() {
        return responseSendTimestamp;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeLong("responseSendTimestamp", responseSendTimestamp))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeBoolean("success", success))
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

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 8:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                responseSendTimestamp = reader.readLong("responseSendTimestamp");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                success = reader.readBoolean("success");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheTxRecoveryResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 17;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 12;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTxRecoveryResponse.class, this, "super", super.toString());
    }
}
