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

import io.opencensus.trace.TraceComponent;

/**
 * Interface for OpenCensus trace exporters adopted for Ignite lifecycle.
 */
public interface OpenCensusTraceExporter {
    /**
     * @param traceComponent Trace component.
     * @param igniteInstanceName Ignite instance name.
     */
    public void start(TraceComponent traceComponent, String igniteInstanceName);
    /**
     * @param traceComponent Trace component.
     */
    public void stop(TraceComponent traceComponent);
}
