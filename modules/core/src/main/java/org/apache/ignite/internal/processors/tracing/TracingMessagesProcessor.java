package org.apache.ignite.internal.processors.tracing;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.tracing.messages.Trace;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessage;

public class TracingMessagesProcessor {
    private final GridKernalContext ctx;
    private final TracingSpi spi;
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
     * A span with name {@link TraceableMessage#traceName()} will be created
     * from contained serialized span {@link Trace#serializedSpan()}
     *
     * @param msg Traceable message.
     */
    public void afterReceive(TraceableMessage msg) {
        log.warning("Received " + msg);

        if (msg.trace().serializedSpan() != null && msg.trace().span() == null)
            msg.trace().span(
                spi.create(msg.traceName(), msg.trace().serializedSpan())
                    .addTag("node.id", ctx.localNodeId().toString())
                    .addTag("node.consistent.id", ctx.discovery().localNode().consistentId().toString())
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
        if (msg.trace().span() != null && msg.trace().serializedSpan() == null)
            msg.trace().serializedSpan(spi.serialize((SpanEx) msg.trace().span()));
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

        msg.trace().serializedSpan(
            spi.serialize((SpanEx) parent.trace().span())
        );

        msg.trace().span(
            spi.create(msg.traceName(), (SpanEx) parent.trace().span())
                .addTag("node.id", ctx.localNodeId().toString())
                .addLog("Created")
        );

        return msg;
    }

    /**
     * @param msg Message.
     */
    public void finishProcessing(TraceableMessage msg) {
        log.warning("Processed " + msg);

        if (msg.trace().span() != null)
            msg.trace().span()
                .addLog("Processed")
                .end();
        else
            log.warning("Null span at " + msg);
    }
}
