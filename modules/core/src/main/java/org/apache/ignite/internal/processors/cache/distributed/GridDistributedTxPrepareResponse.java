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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.extensions.communication.TimeLoggableResponse;

/**
 * Response to prepare request.
 */
public class GridDistributedTxPrepareResponse extends GridDistributedBaseMessage implements IgniteTxStateAware, TimeLoggableResponse {
    /** */
    private static final long serialVersionUID = 0L;

    /** Error. */
    @GridToStringExclude
    @GridDirectTransient
    private Throwable err;

    /** Serialized error. */
    private byte[] errBytes;

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

    /** */
    private int part;

    /** */
    protected byte flags;

    /**
     * Empty constructor (required by {@link Externalizable}).
     */
    public GridDistributedTxPrepareResponse() {
        /* No-op. */
    }

    /**
     * @param part Partition.
     * @param xid Lock or transaction ID.
     * @param addDepInfo Deployment info flag.
     */
    public GridDistributedTxPrepareResponse(int part, GridCacheVersion xid, boolean addDepInfo) {
        super(xid, 0, addDepInfo);

        this.part = part;
    }

    /**
     * @param part Partition.
     * @param xid Lock or transaction ID.
     * @param err Error.
     * @param addDepInfo Deployment info flag.
     */
    public GridDistributedTxPrepareResponse(int part, GridCacheVersion xid, Throwable err, boolean addDepInfo) {
        super(xid, 0, addDepInfo);

        this.part = part;
        this.err = err;
    }

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    protected final void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reags flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    protected final boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        return err;
    }

    /**
     * @param err Error to set.
     */
    public void error(Throwable err) {
        this.err = err;
    }

    /**
     * @return Rollback flag.
     */
    public boolean isRollback() {
        return err != null;
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
        return ctx.txPrepareMessageLogger();
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
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (err != null && errBytes == null)
            errBytes = U.marshal(ctx, err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errBytes != null && err == null)
            err = U.unmarshal(ctx, errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
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
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeInt("part", part))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeLong("responseSendTimestamp", responseSendTimestamp))
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
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                part = reader.readInt("part");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                responseSendTimestamp = reader.readLong("responseSendTimestamp");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDistributedTxPrepareResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 26;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 12;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxPrepareResponse.class, this, "err",
            err == null ? "null" : err.toString(), "super", super.toString());
    }
}
