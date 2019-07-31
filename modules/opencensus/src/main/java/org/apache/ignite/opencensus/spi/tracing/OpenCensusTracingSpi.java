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

import io.opencensus.internal.DefaultVisibilityForTesting;
import io.opencensus.internal.Provider;
import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.propagation.SpanContextParseException;
import io.opencensus.trace.samplers.Samplers;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.TraceTags;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing SPI implementation based on OpenCensus library.
 */
public class OpenCensusTracingSpi extends IgniteSpiAdapter implements TracingSpi {
    /** Exporter. */
    private OpenCensusTraceExporter exporter;

    /** Trace component. */
    private TraceComponent traceComponent;

    /**
     * Default constructor.
     */
    public OpenCensusTracingSpi() {
    }

    /**
     * @param traceComponent Trace component.
     */
    public OpenCensusTracingSpi(TraceComponent traceComponent) {
        this.traceComponent = traceComponent;
    }

    /**
     * @param exporter Exporter.
     */
    public OpenCensusTracingSpi withExporter(OpenCensusTraceExporter exporter) {
        this.exporter = exporter;

        return this;
    }

    /**
     *
     */
    @DefaultVisibilityForTesting
    OpenCensusTraceExporter getExporter() {
        return exporter;
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter create(@NotNull String name, @Nullable Span parentSpan) {
        try {
            OpenCensusSpanAdapter spanAdapter = (OpenCensusSpanAdapter) parentSpan;

            return new OpenCensusSpanAdapter(
                traceComponent.getTracer().spanBuilderWithExplicitParent(name, spanAdapter != null ? spanAdapter.impl() : null)
                    .setSampler(Samplers.alwaysSample())
                    .startSpan()
            ).addTag(TraceTags.tag(TraceTags.NODE, TraceTags.NAME), igniteInstanceName);
                //.addTag("stack_trace", U.stackTrace());
        }
        catch (Exception e) {
            throw new IgniteException("Failed to create from parent", e);
        }
    }

    /** {@inheritDoc} */
    @Override public OpenCensusSpanAdapter create(@NotNull String name, @Nullable byte[] serializedSpanBytes) {
        try {
            return new OpenCensusSpanAdapter(
                traceComponent.getTracer().spanBuilderWithRemoteParent(
                    name,
                    traceComponent.getPropagationComponent().getBinaryFormat().fromByteArray(serializedSpanBytes)
                )
                .setSampler(Samplers.alwaysSample())
                .startSpan()
            ).addTag(TraceTags.tag(TraceTags.NODE, TraceTags.NAME), igniteInstanceName);
                //.addTag("stack_trace", U.stackTrace());
        }
        catch (SpanContextParseException e) {
            throw new IgniteException("Failed to create span from serialized value: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(@NotNull Span span) {
        OpenCensusSpanAdapter spanAdapter = (OpenCensusSpanAdapter) span;

        return traceComponent.getPropagationComponent().getBinaryFormat().toByteArray(spanAdapter.impl().getContext());
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return "OpenCensusTracingSpi";
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        if (traceComponent == null) {
            traceComponent = createTraceComponent(getClass().getClassLoader());

            if (exporter != null)
                exporter.start(traceComponent, igniteInstanceName);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (exporter != null)
            exporter.stop(traceComponent);
    }

    /**
     * Any provider that may be used for TraceComponent can be added here.
     *
     * @param classLoader Class loader.
     */
    public static TraceComponent createTraceComponent(@javax.annotation.Nullable ClassLoader classLoader) {
        // Exception that contains possible causes of failed TraceComponent start.
        IgniteSpiException startE;

        try {
            // Call Class.forName with literal string name of the class to help shading tools.
            return Provider.createInstance(
                Class.forName(
                    "io.opencensus.impl.trace.TraceComponentImpl", /*initialize=*/ true, classLoader),
                TraceComponent.class);
        }
        catch (Exception e) {
            startE = new IgniteSpiException("Failed to start TraceComponent.");

            startE.addSuppressed(e);
        }

        try {
            // Call Class.forName with literal string name of the class to help shading tools.
            return Provider.createInstance(
                Class.forName(
                    "io.opencensus.impllite.trace.TraceComponentImplLite",
                    /*initialize=*/ true,
                    classLoader),
                TraceComponent.class);
        }
        catch (Exception e) {
            startE.addSuppressed(e);
        }

        throw startE;
    }
}
