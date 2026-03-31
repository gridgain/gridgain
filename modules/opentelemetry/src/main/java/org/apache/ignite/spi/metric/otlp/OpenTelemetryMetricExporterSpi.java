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

import java.util.Arrays;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.PushMetricsExporterAdapter;
import org.apache.ignite.internal.spi.metric.otlp.MetricReporter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

public class OpenTelemetryMetricExporterSpi extends PushMetricsExporterAdapter {
    /** Default protocol type that is used to export metrics. */
    public static final Protocol DEFAULT_PROTOCOL = Protocol.GRPC;

    /** Default compression type. */
    public static final Compression DEFAULT_COMPRESSION = Compression.NONE;

    /** Default endpoint URL. */
    public static final String DEFAULT_ENDPOINT = "http://localhost:4317";

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

    /** Service identifier (node consistent id). */
    private String srvcId;

    /** OTLP endpoint to connect to. */
    private String endpoint = DEFAULT_ENDPOINT;

    /** Protocol that is used to export metrics. */
    private Protocol protocol = DEFAULT_PROTOCOL;

    /** Compression type. */
    private Compression compression = DEFAULT_COMPRESSION;

    // TODO connection headers

    // TODO security configuration
    // ssl

    /** Otlp metric exporter. */
    private volatile MetricReporter exporter;

    /** {@inheritDoc} */
    @Override public void export() {
        MetricReporter exporter0 = exporter;

        if (exporter0 != null) {
            exporter0.report(mreg, filter);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        super.spiStart(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        super.spiStop();

        MetricReporter exporter0 = exporter;

        if (exporter0 != null) {
            exporter0.close();
            exporter = null;
        }
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        super.onContextInitialized0(spiCtx);

        if (srvcName == null || srvcName.isEmpty()) {
            // TODO
            // fix deadlock - srvcName = ((IgniteEx)ignite()).context().cluster().clusterName();
            // register event listener to get updated cluster name.
            srvcName = "cluster.name";
        }

        srvcId = ((IgniteEx)ignite()).context().discovery().localNode().consistentId().toString();

        exporter = createExporter();
    }

    /**
     * Sets a logical name of a system or application under a common namespace. This a namespace for the service name.
     * 
     * @param srvcNamespace Service namespace.
     * @see #setServiceName(String)
     * @see <a href="https://opentelemetry.io/docs/specs/semconv/resource/service">Service semantic conventions</a>
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
     * The default value is {@code null}.
     * 
     * @param srvcName Service name.
     * @see #setServiceNamespace(String)
     * @see <a href="https://opentelemetry.io/docs/specs/semconv/resource/service">Service semantic conventions</a>
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
     * The default value is {@link #DEFAULT_ENDPOINT}.
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

    /**
     * Sets the OTLP protocol to export metrics.
     * The default value is {@link #DEFAULT_PROTOCOL}.
     * 
     * @param protocol Protocol to export metrics.
     * @throws IllegalArgumentException when the given {@code protocol} is not supported.
     * @see Protocol
     */
    public void setProtocol(String protocol) {
        Protocol p = Protocol.of(protocol);

        if (p == null) {
            throw new IllegalArgumentException("Unsupported protocol [" +
                "type=" + protocol + "]. Supported protocols are " + Arrays.toString(Protocol.values()));
        }
        
        setProtocol(p);
    }

    /**
     * Sets the OTLP protocol to export metrics.
     *
     * @param protocol Protocol to export metrics.
     * @see Protocol
     * @see #setProtocol(Protocol) 
     */
    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    /**
     * Returns the configured protocol.
     *
     * @return Configured protocol.
     * @see #setProtocol(Protocol)
     * @see Protocol
     */
    public Protocol getProtocol() {
        return protocol;
    }

    /**
     * Sets compression type.
     * The default value is {@link #DEFAULT_COMPRESSION}.
     *
     * @param compression Compression type.
     * @throws IllegalArgumentException when the given {@code compression} is not supported.
     * @see Compression
     */
    public void setCompression(String compression) {
        Compression c = Compression.of(compression);

        if (c == null) {
            throw new IllegalArgumentException("Unsupported compression type [" +
                "type=" + compression + "]. Supported compression types are " + Arrays.toString(Compression.values()));
        }

        setCompression(c);
    }

    /**
     * Sets compression type.
     * The default value is {@link #DEFAULT_COMPRESSION}.
     *
     * @param compression Compression type.
     * @see Compression
     */
    public void setCompression(Compression compression) {
        this.compression = compression;
    }

    /**
     * Returns the configured compression type.
     *
     * @return Configured compression type.
     * @see #setCompression(Compression)
     * @see Compression
     */
    public Compression getCompression() {
        return compression;
    }

    private MetricReporter createExporter() {
        MetricReporter reporter = new MetricReporter(
            log, srvcNamespace, srvcName, srvcId, endpoint, protocol, compression
        );

        // TODO
        // if we want to cache already created metrics.
        mreg.addMetricRegistryRemoveListener(reporter::removeMetricSet);

        return reporter;
    }
}
