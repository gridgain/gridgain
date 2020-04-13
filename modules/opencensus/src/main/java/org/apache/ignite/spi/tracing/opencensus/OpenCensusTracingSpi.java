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

package org.apache.ignite.spi.tracing.opencensus;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import io.opencensus.trace.BlankSpan;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.export.SpanExporter;
import io.opencensus.trace.samplers.Samplers;
import org.apache.ignite.internal.processors.tracing.DeferredSpan;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConsistencyChecked;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.GridClientByteUtils.bytesToInt;
import static org.apache.ignite.internal.util.GridClientByteUtils.bytesToShort;
import static org.apache.ignite.internal.util.GridClientByteUtils.intToBytes;
import static org.apache.ignite.internal.util.GridClientByteUtils.shortToBytes;

/**
 * Tracing SPI implementation based on OpenCensus library.
 *
 * If you have OpenCensus Tracing in your environment use the following code for configuration:
 * <code>
 *     IgniteConfiguration cfg;
 *
 *     cfg.setTracingSpi(new OpenCensusTracingSpi());
 * </code>
 * If you don't have OpenCensus Tracing:
 * <code>
 *     IgniteConfigiration cfg;
 *
 *     cfg.setTracingSpi(new OpenCensusTracingSpi(new ZipkinExporterHandler(...)));
 * </code>
 *
 * See constructors description for detailed explanation.
 */
@IgniteSpiMultipleInstancesSupport(value = true)
@IgniteSpiConsistencyChecked(optional = true)
public class OpenCensusTracingSpi extends IgniteSpiAdapter implements TracingSpi {
    /** Configured exporters. */
    private final List<OpenCensusTraceExporter> exporters;

    /** Flag indicates that external Tracing is used in environment. In this case no exporters will be started. */
    private final boolean externalProvider;

    /**
     * This constructor is used if environment (JVM) already has OpenCensus tracing.
     * In this case traces from the node will go trough externally registered exporters by an user himself.
     *
     * @see Tracing#getExportComponent()
     */
    public OpenCensusTracingSpi() {
        exporters = null;

        externalProvider = true;
    }

    /**
     * This constructor is used if environment (JVM) hasn't OpenCensus tracing.
     * In this case provided exporters will start and traces from the node will go through it.
     *
     * @param exporters Exporters.
     */
    public OpenCensusTracingSpi(SpanExporter.Handler... exporters) {
        this.exporters = Arrays.stream(exporters).map(OpenCensusTraceExporter::new).collect(Collectors.toList());

        externalProvider = false;
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter create(@NotNull SpanType spanType, @Nullable Span parentSpan) {
        try {
            io.opencensus.trace.Span openCensusParent = null;

            if (parentSpan instanceof OpenCensusSpanAdapter)
                openCensusParent = ((OpenCensusSpanAdapter)parentSpan).impl();

            return new OpenCensusSpanAdapter(
                Tracing.getTracer().spanBuilderWithExplicitParent(
                    spanType.traceName(),
                    openCensusParent
                )
                    .setSampler(Samplers.alwaysSample())
                    .startSpan(),
                spanType
            );
        }
        catch (Exception e) {
            LT.warn(log, "Failed to create span from parent " +
                "[spanName=" + spanType.traceName() + ", parentSpan=" + parentSpan + "]");

            return new OpenCensusSpanAdapter(BlankSpan.INSTANCE, null);
        }
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType spanType, @Nullable byte[] parentSerializedSpan) {
        try {
            int openTracingSpanSize = bytesToInt(Arrays.copyOfRange(parentSerializedSpan, 0, 4), 0);

            SpanType parentTrace = SpanType.fromIndex(bytesToInt(Arrays.copyOfRange(parentSerializedSpan,
                4 + openTracingSpanSize, 4 + 4 + openTracingSpanSize), 0));

            Set<Scope> supportedScopes = new HashSet<>();
            for (int i = 0; i < parentSerializedSpan.length - (4 + 4 + openTracingSpanSize); i +=2){
                supportedScopes.add(Scope.fromIndex(bytesToShort(Arrays.copyOfRange(parentSerializedSpan,
                    4 + 4 + openTracingSpanSize + (2 * i),
                    4 + 4 + openTracingSpanSize + (2 * (i + 1))), 0)));
            }

            if (parentSerializedSpan == null || parentSerializedSpan.length == 0) {
                // If there's no parent span or parent span is NoopSpan than =>
                // create new span that will be closed when TraceSurroundings will be closed.
                // Use union of scope and supportedScopes as span supported scopes.
                return create(spanType, NoopSpan.INSTANCE);
            }
            else {
                // If there's is parent span =>
                // If parent span supports given scope =>

                if (parentTrace.scope() == spanType.scope() || supportedScopes.contains(spanType.scope())) {
                    // create new span as child span for parent span, using parents span supported scopes.
                    supportedScopes.add(parentTrace.scope());
                    supportedScopes.remove(spanType.scope());

                    return new OpenCensusSpanAdapter(
                        Tracing.getTracer().spanBuilderWithRemoteParent(
                            spanType.traceName(),
                            Tracing.getPropagationComponent().getBinaryFormat().fromByteArray(
                                Arrays.copyOfRange(parentSerializedSpan, 4, openTracingSpanSize + 4))
                        )
                            .setSampler(Samplers.alwaysSample())
                            .startSpan(),
                        spanType,
                        supportedScopes
                    );
                }
                else {
                    // do nothing;
                    return new OpenCensusDeferredSpanAdapter(parentSerializedSpan);
                    // "suppress" parent span for a while, create new span as separate one.
                    // return spi.create(trace, null, supportedScopes);
                }
            }
        }
        catch (Exception e) {
            LT.warn(log, "Failed to create span from serialized value " +
                "[serializedValue=" + Arrays.toString(parentSerializedSpan) + "]");

            return new OpenCensusSpanAdapter(BlankSpan.INSTANCE, null);
        }
    }

    /** {@inheritDoc} */
    @Override public Span create(@NotNull SpanType spanType, @Nullable Span parentSpan, Scope... supportedScopes) {
        try {
            io.opencensus.trace.Span openCensusParent = null;

            if (parentSpan instanceof OpenCensusSpanAdapter)
                openCensusParent = ((OpenCensusSpanAdapter)parentSpan).impl();

            return new OpenCensusSpanAdapter(
                Tracing.getTracer().spanBuilderWithExplicitParent(
                    spanType.traceName(),
                    openCensusParent
                )
                    .setSampler(Samplers.alwaysSample())
                    .startSpan(),
                spanType,

                new HashSet<>(Arrays.asList(supportedScopes))
            );
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to create span from parent " +
                "[spanName=" + spanType.traceName() + ", parentSpan=" + parentSpan + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        if (span instanceof OpenCensusDeferredSpanAdapter)
            return ((DeferredSpan)span).serializedSpan();

        OpenCensusSpanAdapter spanAdapter = (OpenCensusSpanAdapter) span;

        // Serialized version of inner io.opencensus.trace.Span span.
        byte[] openTracingSerializedSpan = Tracing.getPropagationComponent().getBinaryFormat().toByteArray(
            spanAdapter.impl().getContext());

        // Serialized span size:
        //  4 bytes - int that stores inner io.opencensus.trace.Span size +
        //  serialized version of io.opencensus.trace.Span +
        //  4 bytes - int that is trace id. See Trace for more details.
        //  2 * supported scopes, ids of supported scope binded to given span, every id a short value.
        int openTracingSerializedLen = openTracingSerializedSpan.length;

        byte[] serializedSpanBytes = new byte[4 + openTracingSerializedLen + 4 + (2 * span.supportedScopes().size())];

        // Serialize io.opencensus.trace.Span size.
        System.arraycopy(intToBytes(openTracingSerializedLen), 0, serializedSpanBytes, 0, 4);

        // Serialize io.opencensus.trace.Span
        System.arraycopy(openTracingSerializedSpan, 0, serializedSpanBytes, 4,
            openTracingSerializedLen);

        // Serialize trace.
        System.arraycopy(intToBytes(span.type().idx()), 0, serializedSpanBytes,
            4 + openTracingSerializedLen, 4);

        // Serialize supported scopes
        int supportedScopeIdx = 0;
        for (Scope supportedScope : span.supportedScopes()) {
            System.arraycopy(shortToBytes(supportedScope.idx()), 0, serializedSpanBytes,
                4 + 4 + openTracingSerializedLen + (2 * supportedScopeIdx++), 2);
        }

        return serializedSpanBytes;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return "OpenCensusTracingSpi";
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        if (!externalProvider && exporters != null)
            for (OpenCensusTraceExporter exporter : exporters)
                exporter.start(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (!externalProvider && exporters != null)
            for (OpenCensusTraceExporter exporter : exporters)
                exporter.stop();
    }
}
