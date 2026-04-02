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
import java.util.Collections;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.PushMetricsExporterAdapter;
import org.apache.ignite.internal.spi.metric.otlp.MetricReporter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.ssl.SslContextFactory;

/**
 * SPI implementation that periodically exports Ignite metrics to an
 * <a href="https://opentelemetry.io/docs/specs/otlp/">OpenTelemetry Protocol (OTLP)</a> compatible backend
 * (e.g. OpenTelemetry Collector, Grafana OTLP endpoint, Jaeger, etc.).
 *
 * <h2>Overview</h2>
 * <p>
 * This exporter runs a background task that fires every {@link #getPeriod()} milliseconds and pushes the current
 * snapshot of all metric registries to the configured OTLP endpoint. Each Ignite node exports its own metrics and
 * identifies itself via the
 * <a href="https://opentelemetry.io/docs/specs/semconv/resource/service">OpenTelemetry service resource attributes</a>:
 * <ul>
 *   <li>{@code service.namespace} — set via {@link #setServiceNamespace(String)}</li>
 *   <li>{@code service.name} — set via {@link #setServiceName(String)};
 *       defaults to the cluster tag (see {@link IgniteCluster#tag()}) if not specified</li>
 *   <li>{@code service.instance.id} — automatically set to the node's consistent ID</li>
 * </ul>
 *
 * <h2>Transport</h2>
 * <p>
 * Two wire protocols are supported (see {@link Protocol}):
 * <ul>
 *   <li>{@link Protocol#GRPC} (default) — OTLP over gRPC</li>
 *   <li>{@link Protocol#HTTP} — OTLP over HTTP/protobuf</li>
 * </ul>
 * The target endpoint is configured via {@link #setEndpoint(String)} and must follow the pattern
 * {@code http(s)://host:port}.  The default endpoint is {@value #DEFAULT_ENDPOINT}.
 *
 * <h2>Compression</h2>
 * <p>
 * Payload compression is controlled by {@link #setCompression(Compression)}.  Supported types are
 * {@link Compression#NONE} (default) and {@link Compression#GZIP}.
 *
 * <h2>SSL/TLS</h2>
 * <p>
 * Secure connections are enabled by setting {@link #setSslEnabled(boolean)} to {@code true}.
 * By default, the SPI will reuse the {@link javax.cache.configuration.Factory Factory&lt;SSLContext&gt;} already
 * configured in {@link org.apache.ignite.configuration.IgniteConfiguration#setSslContextFactory(Factory)}
 * (controlled by {@link #setUseIgniteSslContextFactory(boolean)}).
 * <p>
 * A dedicated SSL context factory can be provided via {@link #setSslContextFactory(Factory)}.
 * An instance of {@link org.apache.ignite.ssl.SslContextFactory} is the recommended implementation because it
 * supplies both the {@link javax.net.ssl.SSLContext} and the {@link javax.net.ssl.TrustManager} from a single
 * object.  When a different {@link javax.cache.configuration.Factory Factory&lt;SSLContext&gt;} is used, a
 * separate {@link #setTrustManagerFactory(Factory) trust-manager factory} must be configured.
 *
 * <h2>Custom headers</h2>
 * <p>
 * Arbitrary HTTP/gRPC metadata headers (e.g. authentication tokens) can be injected into every export request
 * via {@link #setConnectionHeaders(Map)}.
 *
 * <h2>Configuration example</h2>
 * <pre>{@code
 * OpenTelemetryMetricExporterSpi spi = new OpenTelemetryMetricExporterSpi();
 *
 * spi.setEndpoint("http://otel-collector:4317");
 * spi.setProtocol(Protocol.GRPC);
 * spi.setCompression(Compression.GZIP);
 * spi.setPeriod(30_000L);                   // export every 30 seconds
 * spi.setServiceNamespace("my-org");
 * spi.setServiceName("ignite-cluster");
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 * cfg.setMetricExporterSpi(spi);
 * }</pre>
 *
 * @see Protocol
 * @see Compression
 * @see PushMetricsExporterAdapter
 * @see <a href="https://opentelemetry.io/docs/specs/otlp/">OTLP specification</a>
 * @see <a href="https://opentelemetry.io/docs/specs/semconv/resource/service">Service semantic conventions</a>
 */
public class OpenTelemetryMetricExporterSpi extends PushMetricsExporterAdapter {
    /** Default protocol type that is used to export metrics. */
    public static final Protocol DEFAULT_PROTOCOL = Protocol.GRPC;

    /** Default compression type. */
    public static final Compression DEFAULT_COMPRESSION = Compression.NONE;

    /** Default endpoint URL. */
    public static final String DEFAULT_ENDPOINT = "http://localhost:4317";

    /**
     * By default OTLP exporter uses SSL context factory from Ignite configuration.
     * @see IgniteConfiguration#setSslContextFactory(Factory)
     */
    public static final boolean DFLT_USE_IGNITE_SSL_CTX_FACTORY = true;

    /** Logical name of a system or application under a common namespace. This a namespace for {@link #srvcName}. */
    private String srvcNamespace;

    /** Logical name of the service. If it is not specified then cluster tag is used. */
    private String srvcName;

    /** Service identifier (node consistent id). */
    private String srvcId;

    /** OTLP endpoint to connect to. */
    private String endpoint = DEFAULT_ENDPOINT;

    /** Protocol that is used to export metrics. */
    private Protocol protocol = DEFAULT_PROTOCOL;

    /** Compression type. */
    private Compression compression = DEFAULT_COMPRESSION;

    /** Connection headers. */
    private Map<String, String> headers = Collections.emptyMap();

    /** SSL enable flag, default is disabled. */
    private boolean sslEnabled;

    /** If set to {@code true}, this SPI will use SSL context factory from Ignite configuration. */
    private boolean useIgniteSslCtxFactory = DFLT_USE_IGNITE_SSL_CTX_FACTORY;

    /** Factory to create SSLContext. */
    private Factory<SSLContext> sslFactory;

    /** Factory to create TrustManager. */
    private Factory<TrustManager> trustFactory;

    /** Otlp metric exporter. */
    private MetricReporter exporter;

    /** {@inheritDoc} */
    @Override public void export() {
        exporter.report(mreg, filter);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        super.spiStop();

        if (exporter != null)
            exporter.close();
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        srvcId = ((IgniteEx) ignite()).context().discovery().localNode().consistentId().toString();

        if (srvcName == null || srvcName.isEmpty())
            srvcName = ((IgniteEx) ignite()).context().cluster().getTag();

        exporter = createExporter();

        // Start executor after the exporter has been created and available.
        super.onContextInitialized0(spiCtx);
    }

    /**
     * Sets a logical name of a system or application under a common namespace. This is a namespace for the service name.
     * 
     * @param srvcNamespace Service namespace.
     * @see <a href="https://opentelemetry.io/docs/specs/semconv/resource/service">Service semantic conventions</a>
     */
    public void setServiceNamespace(String srvcNamespace) {
        this.srvcNamespace = srvcNamespace;
    }

    /**
     * Returns service namespace.
     * 
     * @return Service namespace.
     * @see #setServiceNamespace(String)
     */
    public String getServiceNamespace() {
        return srvcNamespace;
    }

    /**
     * Sets a logical name of the service.
     * <p>
     * If {@code srvcName} is {@code null} or empty then the cluster tag is used. The default value is {@code null}.
     * 
     * @param srvcName Service name.
     * @see <a href="https://opentelemetry.io/docs/specs/semconv/resource/service">Service semantic conventions</a>
     * @see IgniteCluster#tag()
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

    /**
     * Sets connection headers.
     *
     * @param headers Connection headers.
     */
    public void setConnectionHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    /**
     * Returns the configured connection headers.
     *
     * @return Configured connection headers.
     * @see #setConnectionHeaders(Map)
     */
    public Map<String, String> getConnectionHeaders() {
        return headers;
    }

    /**
     * Sets whether Secure Socket Layer should be enabled.
     * <p>
     * Note that if this flag is set to {@code true}, then a valid instance of {@code Factory&lt;SSLContext&gt;}
     * should be provided. The default value is {@code false}.
     *
     * @param sslEnabled {@code true} if SSL should be enabled and {@code false} otherwise.
     * @see #setSslContextFactory(Factory)
     * @see #setUseIgniteSslContextFactory(boolean)
     * @see IgniteConfiguration#setSslContextFactory(Factory)
     */
    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    /**
     * Returns {@code true} if Secure Socket Layer is enabled and {@code false} otherwise.
     *
     * @return Returns {@code true} if Secure Socket Layer is enabled and {@code false} otherwise.
     * @see #setSslEnabled(boolean)
     */
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    /**
     * Sets whether to use Ignite SSL context factory.
     *
     * @param useIgniteSslCtxFactory Whether to use Ignite SSL context factory.
     * @see IgniteConfiguration#setSslContextFactory(Factory)
     */
    public void setUseIgniteSslContextFactory(boolean useIgniteSslCtxFactory) {
        this.useIgniteSslCtxFactory = useIgniteSslCtxFactory;
    }

    /**
     * Gets whether to use Ignite SSL context factory configured through
     * {@link IgniteConfiguration#getSslContextFactory()} if {@link #getSslContextFactory()} is not set.
     *
     * @return {@code true} if Ignite SSL context factory should be used.
     * @see #setUseIgniteSslContextFactory(boolean)
     */
    public boolean isUseIgniteSslContextFactory() {
        return useIgniteSslCtxFactory;
    }

    /**
     * Sets the given instance of {@link Factory} that will be used to create an instance of {@link SSLContext}
     * for Secure Socket Layer. This factory will only be used if {@link #setSslEnabled(boolean)} is set to {@code true}.
     * <p>
     * An instance of {@link SslContextFactory} class can be used
     * in order to provide {@link SSLContext} and {@link TrustManager} at the same time.
     * <pre>
     * {@code
     *   // Create and setup the factory.
     *   SslContextFactory factory = new SslContextFactory();
     *
     *   factory.setKeyStoreFilePath(keyStorePath);
     *   factory.setKeyStorePassword(keyPass);
     *   factory.setTrustStoreFilePath(trustStorePath);
     *   factory.setTrustStorePassword(trustPass);
     *   ...
     *
     *   OpenTelemetryMetricExporterSpi spi = new OpenTelemetryMetricExporterSpi();
     *   // This call overrides {@link #setTrustManagerFactory(Factory)}
     *   // TrustManager is obtained from the {@code factory}.
     *   spi.setSslContextFactory(factory);
     * }
     * </pre>
     *
     * @param sslFactory Instance of {@link Factory}.
     * @see SslContextFactory
     */
    public void setSslContextFactory(Factory<SSLContext> sslFactory) {
        this.sslFactory = sslFactory;
    }

    /**
     * Returns the configured instance of {@link Factory} that will be used to create an instance of {@link SSLContext}.
     *
     * @return Factory to create {@link SSLContext}.
     * @see #setSslContextFactory(Factory)
     */
    public Factory<SSLContext> getSslContextFactory() {
        return sslFactory;
    }

    /**
     * Sets the given instance of {@link Factory} that will be used to create an instance of {@link TrustManager}.
     * This factory will only be used if {@link #setSslEnabled(boolean)} is set to {@code true}.
     *
     * @param trustFactory Instance of {@link Factory}.
     * @see SslContextFactory
     */
    public void setTrustManagerFactory(Factory<TrustManager> trustFactory) {
        this.trustFactory = trustFactory;
    }

    /**
     * Returns the configured factory to create an instance of {@link TrustManager}.
     *
     * @return Factory to create {@link TrustManager}.
     * @see #setTrustManagerFactory(Factory)
     */
    public Factory<TrustManager> getTrustManagerFactory() {
        return trustFactory;
    }

    private MetricReporter createExporter() {
        SSLContext sslContext = null;
        TrustManager trustManager = null;

        if (sslEnabled) {
            Factory<SSLContext> factory = useIgniteSslCtxFactory
                ? ignite().configuration().getSslContextFactory()
                : sslFactory;

            if (factory == null)
                throw new IgniteSpiException("SSL is enabled, but SSL context factory is not configured.");

            if (factory instanceof SslContextFactory) {
                SslContextFactory contextFactory = (SslContextFactory) factory;

                sslContext = contextFactory.create();
                trustManager = contextFactory.getTrustManagers().length > 0
                    ? contextFactory.getTrustManagers()[0]
                    : null;
            }
            else {
                if (trustFactory == null)
                    throw new IgniteSpiException("SSL is enabled, but TrustManager factory is not configured.");

                sslContext = factory.create();
                trustManager = trustFactory.create();
            }

            if (sslContext == null)
                throw new IgniteSpiException("SSL context factory returned null SSLContext.");

            if (trustManager == null)
                throw new IgniteSpiException("TrustManager factory returned null TrustManager.");

            if (!(trustManager instanceof X509TrustManager)) {
                throw new IgniteSpiException("TrustManager is not an instance of X509TrustManager [class=" +
                    trustManager.getClass().getSimpleName() + ']');
            }
        }

        MetricReporter reporter = new MetricReporter(
            log, srvcNamespace, srvcName, srvcId,
            endpoint, protocol, compression, headers,
            sslEnabled, sslContext, (X509TrustManager) trustManager
        );

        mreg.addMetricRegistryRemoveListener(reporter::removeMetricSet);

        return reporter;
    }
}
