package org.apache.ignite.internal.processors.tracing;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.tracing.messages.TraceContainer;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessage;

import static org.apache.ignite.internal.processors.tracing.TraceTags.CONSISTENT_ID;
import static org.apache.ignite.internal.processors.tracing.TraceTags.NODE;
import static org.apache.ignite.internal.processors.tracing.TraceTags.NODE_ID;
import static org.apache.ignite.internal.processors.tracing.TraceTags.tag;
import static org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesTable.traceName;

/**
 * Helper for processing traceable message.
 */
public class TracingMessagesProcessor {
    /** Context. */
    private final GridKernalContext ctx;
    /** Spi. */
    private final TracingSpi spi;
    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     * @param spi Spi.
     */
    public TracingMessagesProcessor(GridKernalContext ctx, TracingSpi spi) {
        this.ctx = ctx;
        this.spi = spi;
        this.log = ctx.log(TracingMessagesProcessor.class);
    }

    /**
     * Called when message is received.
     * A span with name associated with given message will be created.
     * from contained serialized span {@link TraceContainer#serializedSpanBytes()}
     *
     * @param msg Traceable message.
     * @see org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesTable
     */
    public void afterReceive(TraceableMessage msg) {
        if (log.isDebugEnabled())
            log.debug("Received traceable message: " + msg);

        if (msg.trace().serializedSpanBytes() != null && msg.trace().span() == null)
            msg.trace().span(
                spi.create(traceName(msg.getClass()), msg.trace().serializedSpanBytes())
                    .addTag(NODE_ID, ctx.localNodeId().toString())
                    .addTag(tag(NODE, CONSISTENT_ID), ctx.discovery().localNode().consistentId().toString())
                    .addLog("Received")
            );
    }

    /**
     * Called when message is going to be send.
     * A serialized span will be created and attached to {@link TraceableMessage#trace()}.
     *
     * @param msg Traceable message.
     */
    public void beforeSend(TraceableMessage msg) {
        if (msg.trace().span() != null && msg.trace().serializedSpanBytes() == null)
            msg.trace().serializedSpanBytes(spi.serialize(msg.trace().span()));
    }

    /**
     * Create a child span in given T msg that from span of {@code parent} msg.
     *
     * @param msg
     * @param parent
     * @param <T>
     * @return
     */
    public <T extends TraceableMessage> T branch(T msg, TraceableMessage parent) {
        assert parent.trace().span() != null : parent;

        msg.trace().serializedSpanBytes(
            spi.serialize(parent.trace().span())
        );

        msg.trace().span(
            spi.create(traceName(msg.getClass()), parent.trace().span())
                .addTag(NODE_ID, ctx.localNodeId().toString())
                .addLog("Created")
        );

        return msg;
    }

    /**
     * @param msg Message.
     */
    public void finishProcessing(TraceableMessage msg) {
        if (log.isDebugEnabled())
            log.debug("Processed traceable message: " + msg);

        if (msg.trace().span() != null)
            msg.trace().span()
                .addLog("Processed")
                .end();
        else
            log.warning("There is no deserialized span in message after processing for: " + msg);
    }
}
