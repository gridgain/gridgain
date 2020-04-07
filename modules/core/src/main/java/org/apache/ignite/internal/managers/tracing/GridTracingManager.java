/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.tracing;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.tracing.DeferredSpan;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.NoopTracingSpi;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesHandler;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE;

/**
 * Tracing Manager.
 */
public class GridTracingManager extends GridManagerAdapter<TracingSpi> implements Tracing {
    /** */
    public static final Scope[] EMPTY_SCOPE_ARRAY = new Scope[0];

    /** Traceable messages handler. */
    private final TraceableMessagesHandler msgHnd;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param useNoopTracingSpi Flag that signals that NoOp tracing spi should be used instead of the one,
     * specified in the context. It's a part of the failover logic that is suitable if an exception is thrown
     * when the manager starts.
     */
    public GridTracingManager(GridKernalContext ctx, boolean useNoopTracingSpi) {
        super(ctx, useNoopTracingSpi ? new NoopTracingSpi() : ctx.config().getTracingSpi());

        msgHnd = new TraceableMessagesHandler(this, ctx.log(GridTracingManager.class));
    }

    /**
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Override public void start() throws IgniteCheckedException {
        try {
            startSpi();
        }
        catch (IgniteSpiException e) {
            log.warning("Failed to start tracing processor with spi: " + getSpi().getName()
                + ". Noop implementation will be used instead.", e);

            throw e;
        }

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /**
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Adds tags with information about local node to given {@code span}.
     *
     * @param span Span.
     * @return Span enriched by local node information.
     */
    private Span enrichWithLocalNodeParameters(@Nullable Span span) {
        if (span == null)
            return null;

        span.addTag(SpanTags.NODE_ID, ctx.localNodeId().toString());
        span.addTag(SpanTags.tag(NODE, SpanTags.NAME), ctx.igniteInstanceName());

        ClusterNode locNode = ctx.discovery().localNode();
        if (locNode != null && locNode.consistentId() != null)
            span.addTag(SpanTags.tag(NODE, SpanTags.CONSISTENT_ID), locNode.consistentId().toString());

        return span;
    }

    /**
     * Generates child span if it's possible due to parent/child supported scopes, othewise returns patent span as is.
     * @param parentSpan Parent span.
     * @param spanTypeToCreate Span type to create.
     * @param supportedScopes Supported scopes.
     * @return Span to propogate with.
     */
    private Span generateSpan(Span parentSpan, SpanType spanTypeToCreate, Scope... supportedScopes) {
        if (parentSpan instanceof DeferredSpan)
            return getSpi().create(spanTypeToCreate, ((DeferredSpan)parentSpan).serializedSpan());

        if (parentSpan == null || parentSpan == NoopSpan.INSTANCE) {
            // If there's no parent span or parent span is NoopSpan then
            // create new span that will be closed when TraceSurroundings.
            // Use union of scope and supportedScopes as span supported scopes.
            return getSpi().create(spanTypeToCreate, null, supportedScopes);
        }
        else {
            // If there's is parent span and parent span supports given scope then...
            if (parentSpan.isChainable(spanTypeToCreate.scope())) {
                // create new span as child span for parent span, using parents span supported scopes.

                Set<Scope> mergedSupportedScopes = new HashSet<>(parentSpan.supportedScopes());
                mergedSupportedScopes.add(parentSpan.type().scope());
                mergedSupportedScopes.remove(spanTypeToCreate.scope());

                return getSpi().create(spanTypeToCreate, parentSpan, mergedSupportedScopes.toArray(EMPTY_SCOPE_ARRAY));
            }
            else {
                // do nothing;
                return NoopSpan.INSTANCE;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType trace, @Nullable Span parentSpan) {
        return enrichWithLocalNodeParameters(generateSpan(parentSpan, trace));
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType trace, @Nullable byte[] serializedSpan) {
        return enrichWithLocalNodeParameters(getSpi().create(trace, serializedSpan));
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType trace, @Nullable Span parentSpan, Scope... supportedScopes) {
        return enrichWithLocalNodeParameters(generateSpan(parentSpan, trace, supportedScopes));
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        return getSpi().serialize(span);
    }

    /** {@inheritDoc} */
    @Override public TraceableMessagesHandler messages() {
        return msgHnd;
    }
}