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

import io.opencensus.common.Clock;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.export.ExportComponent;
import io.opencensus.trace.propagation.PropagationComponent;

/**
 * Default provider for OpenCensus Tracing functionality.
 */
public class OpenCensusTracingProvider {
    /**
     * Returns the global {@link Tracer}.
     *
     * @return the global {@code Tracer}.
     * @since 0.5
     */
    public Tracer getTracer() {
        return Tracing.getTracer();
    }

    /**
     * Returns the global {@link PropagationComponent}.
     *
     * @return the global {@code PropagationComponent}.
     * @since 0.5
     */
    public PropagationComponent getPropagationComponent() {
        return Tracing.getPropagationComponent();
    }

    /**
     * Returns the global {@link Clock}.
     *
     * @return the global {@code Clock}.
     * @since 0.5
     */
    public Clock getClock() {
        return Tracing.getClock();
    }

    /**
     * Returns the global {@link ExportComponent}.
     *
     * @return the global {@code ExportComponent}.
     * @since 0.5
     */
    public ExportComponent getExportComponent() {
        return Tracing.getExportComponent();
    }

    /**
     * Returns the global {@link TraceConfig}.
     *
     * @return the global {@code TraceConfig}.
     * @since 0.5
     */
    public TraceConfig getTraceConfig() {
        return Tracing.getTraceConfig();
    }
}
