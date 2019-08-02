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

package org.apache.ignite.opencensus.spi.tracing;

import java.util.Arrays;
import io.opencensus.trace.samplers.Samplers;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing SPI implementation based on OpenCensus library.
 */
public class OpenCensusTracingSpi extends IgniteSpiAdapter implements TracingSpi {
    /** Configured exporters. */
    private OpenCensusTraceExporter[] exporters;

    /** Trace component. */
    private OpenCensusTracingProvider provider;

    /**
     * Default constructor.
     */
    public OpenCensusTracingSpi() {
    }

    /**
     * @param provider Trace component.
     */
    public OpenCensusTracingSpi(OpenCensusTracingProvider provider) {
        this.provider = provider;
    }

    /**
     * @param exporters Exporter.
     */
    public OpenCensusTracingSpi withExporters(OpenCensusTraceExporter... exporters) {
        this.exporters = exporters.clone();

        return this;
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter create(@NotNull String name, @Nullable Span parentSpan) {
        try {
            OpenCensusSpanAdapter spanAdapter = (OpenCensusSpanAdapter) parentSpan;

            return new OpenCensusSpanAdapter(
                provider.getTracer().spanBuilderWithExplicitParent(
                    name,
                    spanAdapter != null ? spanAdapter.impl() : null
                )
                .setSampler(Samplers.alwaysSample())
                .startSpan()
            );
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to create span from parent " +
                "[spanName=" + name + ", parentSpan=" + parentSpan + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter create(@NotNull String name, @Nullable byte[] serializedSpanBytes) {
        try {
            return new OpenCensusSpanAdapter(
                provider.getTracer().spanBuilderWithRemoteParent(
                    name,
                    provider.getPropagationComponent().getBinaryFormat().fromByteArray(serializedSpanBytes)
                )
                .setSampler(Samplers.alwaysSample())
                .startSpan()
            );
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to create span from serialized value " +
                "[spanName=" + name + ", serializedValue=" + Arrays.toString(serializedSpanBytes) + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        OpenCensusSpanAdapter spanAdapter = (OpenCensusSpanAdapter) span;

        return provider.getPropagationComponent().getBinaryFormat().toByteArray(spanAdapter.impl().getContext());
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return "OpenCensusTracingSpi";
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        if (provider == null) {
            provider = new OpenCensusTracingProvider();

            if (exporters != null)
                for (OpenCensusTraceExporter exporter : exporters)
                    exporter.start(provider, igniteInstanceName);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (exporters != null)
            for (OpenCensusTraceExporter exporter : exporters)
                exporter.stop(provider);
    }
}
