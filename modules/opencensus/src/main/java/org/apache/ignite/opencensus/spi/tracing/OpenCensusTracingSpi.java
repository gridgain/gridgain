package org.apache.ignite.opencensus.spi.tracing;

import io.opencensus.internal.Provider;
import io.opencensus.trace.Span;
import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.propagation.SpanContextParseException;
import io.opencensus.trace.samplers.Samplers;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.tracing.SerializedSpan;
import org.apache.ignite.internal.processors.tracing.SpanEx;
import org.apache.ignite.internal.processors.tracing.TracingSpi;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class OpenCensusTracingSpi extends IgniteSpiAdapter implements TracingSpi<io.opencensus.trace.Span> {
    private OpenCensusTraceExporter exporter;

    private TraceComponent traceComponent;

    public OpenCensusTracingSpi withExporter(OpenCensusTraceExporter exporter) {
        this.exporter = exporter;

        return this;
    }

    /** {@inheritDoc} */
    @Override public SpanEx<Span> create(@NotNull String name, @Nullable SpanEx<io.opencensus.trace.Span> parentSpan) {
        return new SpanAdapter(
            traceComponent.getTracer().spanBuilderWithExplicitParent(name, parentSpan != null ? parentSpan.impl() : null)
                .setSampler(Samplers.alwaysSample())
                .startSpan()
            );
    }

    /** {@inheritDoc} */
    @Override public SpanEx<Span> create(@NotNull String name, @Nullable SerializedSpan serializedSpan) {
        try {
            return new SpanAdapter(
                traceComponent.getTracer().spanBuilderWithRemoteParent(
                    name,
                    traceComponent.getPropagationComponent().getBinaryFormat().fromByteArray(
                        serializedSpan != null ? serializedSpan.value() : null)
                )
                .setSampler(Samplers.alwaysSample())
                .startSpan()
            );
        }
        catch (SpanContextParseException e) {
            throw new IgniteException("Failed to create span from serialized value: " + name, e);
        }
    }

    /** {@inheritDoc} */
    @Override public SerializedSpan serialize(@NotNull SpanEx<Span> span) {
        return new SerializedSpanAdapter(
            traceComponent.getPropagationComponent().getBinaryFormat().toByteArray(span.impl().getContext())
        );
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return "OpenCensusTracingSpi";
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        traceComponent = loadTraceComponent(TraceComponent.class.getClassLoader());

        if (exporter != null)
            exporter.start(traceComponent, igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (exporter != null)
            exporter.stop(traceComponent);
    }

    @Override public TraceComponent traceComponent() {
        return traceComponent;
    }

    /**
     * Any provider that may be used for TraceComponent can be added here.
     *
     * @param classLoader Class loader.
     */
    private TraceComponent loadTraceComponent(@javax.annotation.Nullable ClassLoader classLoader) {
        // Exception that contains possible causes of failed TraceComponent start.
        IgniteSpiException startE;

        try {
            // Call Class.forName with literal string name of the class to help shading tools.
            return Provider.createInstance(
                Class.forName(
                    "io.opencensus.impl.trace.TraceComponentImpl", /*initialize=*/ true, classLoader),
                TraceComponent.class);
        }
        catch (ClassNotFoundException e) {
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
        catch (ClassNotFoundException e) {
            startE.addSuppressed(e);
        }

        throw startE;
    }
}
