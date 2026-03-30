/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.metric.otlp;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.PushMetricsExporterAdapter;
import org.apache.ignite.internal.spi.metric.otlp.MetricReporter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

public class OpenTelemetryMetricExporterSpi extends PushMetricsExporterAdapter {
    /** Logical name of a system or application under a common namespace. This a namespace for {@link #srvcName}. */
    private String srvcNamespace;

    /**
     * Logical name of the service.
     * TODO: currently it is not possible to get a cluster name during {@link #spiStart(String)}
     * or {@link #onContextInitialized0(IgniteSpiContext)}, therefore it is not possible to initialize otlp exporter.
     *
     * Need to find out a solution to address the issue.
     * The possible options are:
     *  - use user defined service name
     *
     *  - postpone creating otlp exporter (this means, that metrics will not be exported)
     *    until the cluster name becomes available.
     *    [see control center agent to enable the required event and follow the updates.]
     */
    private String srvcName;

    /** OTLP endpoint to connect to. */
    private String endpoint;

    // TODO OTLP protocol.
    // grpc, http(s)

    // TODO connection headers

    // TODO security configuration
    // ssl

    // TODO compression
    // gzip, none

    /** Otlp metric exporter. */
    private MetricReporter exporter;

    /** {@inheritDoc} */
    @Override public void export() {
        if (exporter != null) {
            exporter.report(mreg, filter);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        super.spiStart(igniteInstanceName);

        // Is it the right place to create a reporter?
        this.exporter = createExporter();
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        super.spiStop();

        if (exporter != null) {
            exporter.close();
            exporter = null;
        }
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        if (srvcName == null || srvcName.isEmpty()) {
            // TODO
            // fix deadlock - srvcName = ((IgniteEx)ignite()).context().cluster().clusterName();
            // register event listener to get updated cluster name.
            srvcName = "cluster.name";
        }

        // TODO setup service identifier.
        String srvcId = ((IgniteEx)ignite()).context().discovery().localNode().consistentId().toString();

        // Start executor service to push metrics.
        super.onContextInitialized0(spiCtx);
    }

    /**
     * Sets a logical name of a system or application under a common namespace. This a namespace for the service name.
     * 
     * @param srvcNamespace Service namespace.
     * @see #setServiceName(String) 
     */
    public void setServiceNamespace(String srvcNamespace) {
        this.srvcNamespace = srvcNamespace;
    }

    /**
     * Returns service namespace.
     * 
     * @return Service namespace.
     * @see #setServiceName(String) 
     */
    public String getServiceNamespace() {
        return srvcNamespace;
    }

    /**
     * Sets a logical name of the service.
     * If {@code srvcName} is {@code null} then the cluster name is used.
     * 
     * @param srvcName
     * @see #setServiceNamespace(String) 
     */
    public void setServiceName(String srvcName) {
        this.srvcName = srvcName;
    }

    /**
     * Returns a logical name of the service.
     * 
     * @return Logical name of the service.
     * @see #setServiceName(String) 
     */
    public String getServiceName() {
        return srvcName;
    }

    /**
     * Sets the OTLP endpoint to connect to.
     * The endpoint must start with either http:// or https:// and follow the pattern http(s)://host:port.
     *
     * @param endpoint Endpoint to connect to.
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Returns the configured endpoint.
     *
     * @return Configured endpoint.
     * @see #setEndpoint(String)
     */
    public String getEndpoint() {
        return endpoint;
    }

    private MetricReporter createExporter() {
        MetricReporter reporter = new MetricReporter(log, srvcNamespace, srvcName, "test.seviceid.12", endpoint);

        // TODO
        // if we want to cache already created metrics.
        mreg.addMetricRegistryRemoveListener(reporter::removeMetricSet);

        return reporter;
    }
}
