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

package org.apache.ignite.internal.processors.tracing.messages;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.SpanManager;

import static org.apache.ignite.internal.processors.tracing.SpanTags.CONSISTENT_ID;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE_ID;
import static org.apache.ignite.internal.processors.tracing.SpanTags.tag;
import static org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesTable.traceName;

/**
 * Helper to handle traceable messages.
 */
public class TraceableMessagesHandler {
    /** Context. */
    private final GridKernalContext ctx;
    /** Spi. */
    private final SpanManager spanMgr;
    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     * @param spanMgr Span manager.
     */
    public TraceableMessagesHandler(GridKernalContext ctx, SpanManager spanMgr) {
        this.ctx = ctx;
        this.spanMgr = spanMgr;
        this.log = ctx.log(TraceableMessagesHandler.class);
    }

    /**
     * Called when message is received.
     * A span with name associated with given message will be created.
     * from contained serialized span {@link SpanContainer#serializedSpanBytes()}
     *
     * @param msg Traceable message.
     * @see org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesTable
     */
    public void afterReceive(TraceableMessage msg) {
        if (log.isDebugEnabled())
            log.debug("Received traceable message: " + msg);

        if (msg.spanContainer().span() == NoopSpan.INSTANCE) {
            if (msg.spanContainer().serializedSpanBytes() != null)
                msg.spanContainer().span(
                    spanMgr.create(traceName(msg.getClass()), msg.spanContainer().serializedSpanBytes())
                        .addTag(NODE_ID, ctx.localNodeId().toString())
                        .addTag(tag(NODE, CONSISTENT_ID), ctx.discovery().localNode().consistentId().toString())
                        .addLog("Received")
                );
        }
    }

    /**
     * Called when message is going to be send.
     * A serialized span will be created and attached to {@link TraceableMessage#spanContainer()}.
     *
     * @param msg Traceable message.
     */
    public void beforeSend(TraceableMessage msg) {
        if (msg.spanContainer().span() != NoopSpan.INSTANCE && msg.spanContainer().serializedSpanBytes() == null)
            msg.spanContainer().serializedSpanBytes(spanMgr.serialize(msg.spanContainer().span()));
    }

    /**
     * Create a child span in given T msg that from span of {@code parent} msg.
     *
     * @param msg Branched message.
     * @param parent Parent message.
     * @param <T> Traceable message type.
     * @return Branched message with span context from parent message.
     */
    public <T extends TraceableMessage> T branch(T msg, TraceableMessage parent) {
        assert parent.spanContainer().span() != null : parent;

        msg.spanContainer().serializedSpanBytes(
            spanMgr.serialize(parent.spanContainer().span())
        );

        msg.spanContainer().span(
            spanMgr.create(traceName(msg.getClass()), parent.spanContainer().span())
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

        if (!msg.spanContainer().span().isEnded())
            msg.spanContainer().span()
                .addLog("Processed")
                .end();
        else
            log.warning("There is no deserialized span in message after processing for: " + msg);
    }
}
