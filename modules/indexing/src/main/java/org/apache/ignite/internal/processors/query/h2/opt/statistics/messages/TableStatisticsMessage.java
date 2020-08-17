package org.apache.ignite.internal.processors.query.h2.opt.statistics.messages;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.h2.value.Value;

import java.nio.ByteBuffer;

public abstract class TableStatisticsMessage implements Message {


    /** */
    public static final short TYPE_CODE = 172;

    /** */
    private static final long serialVersionUID = 0L;

    /** Request id. */
    private long reqId;

    /** Query id on a node. */
    private long nodeQryId;

    /** Async response flag. */
    private boolean asyncRes;

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return true;
    }
}
