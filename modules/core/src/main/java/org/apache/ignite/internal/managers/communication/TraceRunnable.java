package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.Tracing;

/**
 * Wrapper of {@link Runnable} which incject tracing to execution.
 */
public abstract class TraceRunnable implements Runnable {
    /** */
    private final Tracing tracing;

    /** Name of new span. */
    private final String processName;

    /** Parent span from which new span should be created. */
    private final Span parent;

    /**
     *
     * @param tracing Tracing processor.
     * @param name Name of new span.
     */
    public TraceRunnable(Tracing tracing, String name) {
        this.tracing = tracing;
        processName = name;
        this.parent = MTC.span();
    }

    /** {@inheritDoc} */
    @Override public void run() {
        try (TraceSurroundings ignore = tracing.startChild(processName, parent)) {
            execute();
        }
    }

    /**
     * Main code to execution.
     */
    abstract public void execute();
}
