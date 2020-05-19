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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.tracing.DeferredSpan;
import org.apache.ignite.internal.processors.tracing.NoopTracing;
import org.apache.ignite.internal.processors.tracing.SpanImpl;
import org.apache.ignite.internal.processors.tracing.configuration.GridTracingConfigurationManager;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.NoopTracingSpi;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationManager;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesHandler;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE;
import static org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.internal.processors.tracing.configuration.TracingConfigurationParameters.SAMPLING_RATE_NEVER;
import static org.apache.ignite.internal.util.GridClientByteUtils.bytesToInt;
import static org.apache.ignite.internal.util.GridClientByteUtils.bytesToShort;
import static org.apache.ignite.internal.util.GridClientByteUtils.intToBytes;
import static org.apache.ignite.internal.util.GridClientByteUtils.shortToBytes;

/**
 * Tracing Manager.
 */
public class GridTracingManager extends GridManagerAdapter<TracingSpi> implements Tracing {
    /** Traceable messages handler. */
    private final TraceableMessagesHandler msgHnd;

    /** Tracing configuration */
    private final TracingConfigurationManager tracingConfiguration;

    /**
     * Major span serialization protocol version.
     * Within same major protocol version span serialization should be backward compatible.
     */
    private static final byte MAJOR_PROTOCOL_VERSION = 0;

    /** Minor span serialization protocol version. */
    private static final byte MINOR_PROTOCOL_VERSION = 0;

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

        tracingConfiguration = new GridTracingConfigurationManager(ctx);
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
     * Generates child span if it's possible due to parent/child included scopes, otherwise returns patent span as is.
     * @param parentSpan Parent span.
     * @param spanTypeToCreate Span type to create.
     * @param samplingRate Number between 0 and 1 that more or less reflects the probability of sampling specific trace.
     * 0 and 1 have special meaning here, 0 means never 1 means always. Default value is 0 (never).
     * @param includedScopes Set of {@link Scope} that defines which sub-traces will be included in given trace.
     *  In other words, if child's span scope is equals to parent's scope
     *  or it belongs to the parent's span included scopes, then given child span will be attached to the current trace,
     *  otherwise it'll be skipped.
     *  See {@link Span#isChainable(org.apache.ignite.internal.processors.tracing.Scope)} for more details.
     * @return Span to propagate with.
     */
    private @NotNull Span generateSpan(
        @Nullable Span parentSpan,
        @NotNull SpanType spanTypeToCreate,
        double samplingRate,
        @NotNull Set<Scope> includedScopes
    ) {
        // Optimization
        if (samplingRate == SAMPLING_RATE_NEVER)
            return NoopSpan.INSTANCE;

        if (parentSpan instanceof DeferredSpan)
            return create(spanTypeToCreate, ((DeferredSpan)parentSpan).serializedSpan());

        if (parentSpan == null || parentSpan == NoopSpan.INSTANCE) {
            // If there's no parent span or parent span is NoopSpan then
            // create new span that will be closed when TraceSurroundings.
            // Use union of scope and includedScopes as span included scopes.
            if (spanTypeToCreate.rootSpan()) {
                return new SpanImpl(
                    getSpi().create(
                        spanTypeToCreate.spanName(),
                        null,
                        samplingRate),
                    spanTypeToCreate,
                    includedScopes);
            }
            else
                return NoopSpan.INSTANCE;
        }
        else {
            // If there's is parent span and parent span supports given scope then...
            if (parentSpan.isChainable(spanTypeToCreate.scope())) {
                // create new span as child span for parent span, using parents span included scopes.

                Set<Scope> mergedIncludedScopes = new HashSet<>(parentSpan.includedScopes());

                mergedIncludedScopes.add(parentSpan.type().scope());
                mergedIncludedScopes.remove(spanTypeToCreate.scope());

                return new SpanImpl(
                    getSpi().create(
                        spanTypeToCreate.spanName(),
                        ((SpanImpl)parentSpan).spiSpecificSpan(),
                        samplingRate),
                    spanTypeToCreate,
                    mergedIncludedScopes);
            }
            else {
                // do nothing;
                return NoopSpan.INSTANCE;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType spanType, @Nullable Span parentSpan) {
        return enrichWithLocalNodeParameters(generateSpan(
            parentSpan,
            spanType,
            SAMPLING_RATE_ALWAYS,
            Collections.emptySet()));
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType spanType, @Nullable byte[] serializedParentSpan) {
        // 1 byte: special flags;
        // 1 bytes: spi type;
        // 2 bytes: major protocol version;
        // 2 bytes: minor protocol version;
        // 4 bytes: spi specific serialized span length;
        // n bytes: spi specific serializes span length;
        // 4 bytes: span type
        // 4 bytes included scopes size;
        // 2 * included scopes size: included scopes items one by one;

        Span span;

        try {
            if (serializedParentSpan == null || serializedParentSpan == NoopTracing.NOOP_SERIALIZED_SPAN)
                return create(spanType, NoopSpan.INSTANCE);

            // First byte of the serializedSpan is reserved for special flags - it's not used right now.

            // Deserialize and compare spi types. If they don't match (span was serialized with another spi) then
            // propagate serializedSpan as DeferredSpan.
            if (serializedParentSpan[1] != getSpi().type().index())
                return new DeferredSpan(serializedParentSpan);

            // Deserialize and check major protocol version,
            // cause protocol might be incompatible in case of different protocol versions -
            // propagate serializedSpan as DeferredSpan.
            if (serializedParentSpan[2] != MAJOR_PROTOCOL_VERSION)
                return new DeferredSpan(serializedParentSpan);

            // Deserialize and check minor protocol version.
            // within the scope of the same major protocol version, protocol should be backwards compatible
            byte minProtoVer = serializedParentSpan[3];

            // Deserialize spi specific span size.
            int spiSpecificSpanSize = bytesToInt(
                Arrays.copyOfRange(
                    serializedParentSpan,
                    4,
                    8),
                0);

            SpanType parentSpanType = null;

            Set<Scope> includedScopes = new HashSet<>();

            // Fall through.
            switch (minProtoVer) {
                case 0 : {
                    // Deserialize parent span type.
                    parentSpanType = SpanType.fromIndex(
                        bytesToInt(
                            Arrays.copyOfRange(
                                serializedParentSpan,
                                8 + spiSpecificSpanSize,
                                12 + spiSpecificSpanSize),
                            0));

                    // Deserialize included scopes size.
                    int includedScopesSize = bytesToInt(
                        Arrays.copyOfRange(
                            serializedParentSpan,
                        12 + spiSpecificSpanSize,
                            16 + spiSpecificSpanSize),
                        0);

                    // Deserialize included scopes one by one.
                    for (int i = 0; i < includedScopesSize; i++) {
                        includedScopes.add(Scope.fromIndex(
                            bytesToShort(
                                Arrays.copyOfRange(
                                    serializedParentSpan,
                                    16 + spiSpecificSpanSize + i * 2,
                                    18 + spiSpecificSpanSize + i * 2),
                                0)));
                    }
                }
            }

            assert parentSpanType != null;

            // If there's is parent span and parent span supports given scope then...
            if (parentSpanType.scope() == spanType.scope() || includedScopes.contains(spanType.scope())) {
                // create new span as child span for parent span, using parents span included scopes.

                Set<Scope> mergedIncludedScopes = new HashSet<>(includedScopes);
                mergedIncludedScopes.add(parentSpanType.scope());
                mergedIncludedScopes.remove(spanType.scope());

                span = new SpanImpl(
                    getSpi().create(
                        spanType.spanName(),
                        Arrays.copyOfRange(
                            serializedParentSpan,
                            8,
                            8 + spiSpecificSpanSize)),
                    spanType,
                    mergedIncludedScopes);
            }
            else {
                // do nothing;
                return new DeferredSpan(serializedParentSpan);
                // "suppress" parent span for a while, create new span as separate one.
                // return spi.create(trace, null, includedScopes);
            }
        }
        catch (Exception e) {
            LT.warn(log, "Failed to create span from serialized value " +
                "[serializedValue=" + Arrays.toString(serializedParentSpan) + "]");

            span = NoopSpan.INSTANCE;
        }

        // 1 byte: special flags;
        // 1 bytes: spi type;
        // 2 bytes major protocol version;
        // 2 bytes minor protocol version;
        // 4 bytes spi specific serialized span length;
        // n bytes spi specific serializes span length;
        // span type
        // included scopes size;
        // included scopes items one by one;

        return enrichWithLocalNodeParameters(span);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Span create(
        @NotNull SpanType spanType,
        @Nullable Span parentSpan,
        double samplingRate,
        @NotNull Set<Scope> includedScopes) {
        return enrichWithLocalNodeParameters(generateSpan(
            parentSpan,
            spanType,
            samplingRate,
            includedScopes));
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        // 1 byte: special flags;
        // 1 bytes: spi type;
        // 2 bytes: major protocol version;
        // 2 bytes: minor protocol version;
        // 4 bytes: spi specific serialized span length;
        // n bytes: spi specific serializes span length;
        // 4 bytes: span type
        // 4 bytes included scopes size;
        // 2 * included scopes size: included scopes items one by one;

        if (span instanceof DeferredSpan)
            return ((DeferredSpan)span).serializedSpan();

        // Optimization for NoopSpan.
        if (span == NoopSpan.INSTANCE)
            return NoopTracing.NOOP_SERIALIZED_SPAN;

        // Spi specific serialized span.
        byte[] spiSpecificSerializedSpan = getSpi().serialize(((SpanImpl)span).spiSpecificSpan());

        int serializedSpanLen = 16 + spiSpecificSerializedSpan.length + 4 * span.includedScopes().size();

        byte[] serializedSpanBytes = new byte[serializedSpanLen];

        // Skip special flags bytes.

        // Spi type idx.
        serializedSpanBytes[1] = getSpi().type().index();

        // Major protocol version;
        serializedSpanBytes[2] = MAJOR_PROTOCOL_VERSION;

        // Minor protocol version;
        serializedSpanBytes[3] = MINOR_PROTOCOL_VERSION;

        // Spi specific serialized span length.
        System.arraycopy(
            intToBytes(spiSpecificSerializedSpan.length),
            0,
            serializedSpanBytes,
            4,
            4);

        // Spi specific span.
        System.arraycopy(
            spiSpecificSerializedSpan,
            0,
            serializedSpanBytes,
            8,
            spiSpecificSerializedSpan.length);

        // Span type.
        System.arraycopy(
            intToBytes(span.type().index()),
            0,
            serializedSpanBytes,
            8 + spiSpecificSerializedSpan.length, 4);

        assert span.includedScopes() != null;

        // Supported scope size
        System.arraycopy(
            intToBytes(span.includedScopes().size()),
            0,
            serializedSpanBytes,
            12 + spiSpecificSerializedSpan.length,
            4);

        int includedScopesCnt = 0;

        if (!span.includedScopes().isEmpty()) {
            for (Scope includedScope : span.includedScopes()) {
                System.arraycopy(
                    shortToBytes(includedScope.idx()),
                    0,
                    serializedSpanBytes,
                    16 + spiSpecificSerializedSpan.length + 2 * includedScopesCnt++,
                    2);
            }
        }

        return serializedSpanBytes;
    }

    /** {@inheritDoc} */
    @Override public TraceableMessagesHandler messages() {
        return msgHnd;
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationManager configuration() {
        return tracingConfiguration;
    }
}