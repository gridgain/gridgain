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

package org.apache.ignite.internal.spi.metric.otlp;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.metric.otlp.Compression;
import org.apache.ignite.spi.metric.otlp.Protocol;
import org.jetbrains.annotations.Nullable;

import static io.opentelemetry.sdk.common.export.MemoryMode.REUSABLE_DATA;

/**
 * A reporter which outputs measurements to a {@link MetricExporter}.
 */
public class MetricReporter implements AutoCloseable {
    /**
     * Service namespace attribute.
     * @see <a href="https://opentelemetry.io/docs/specs/semconv/resource/service">Service semantic conventions</a>
     */
    private static final String SERVICE_NAMESPACE = "service.namespace";

    /**
     * Service name attribute.
     * @see <a href="https://opentelemetry.io/docs/specs/semconv/resource/service">Service semantic conventions</a>
     */
    private static final String SERVICE_NAME = "service.name";

    /**
     * Service instance identifier represented by the local node consistentId.
     * @see <a href="https://opentelemetry.io/docs/specs/semconv/resource/service">Service semantic conventions</a>
     */
    private static final String SERVICE_INSTANCE_ID = "service.instance.id";

    private final IgniteLogger log;

    /** Represents a resource, which capture identifying information about the entities for which stats are reported.*/
    private final Resource resource;

    private final MetricExporter exporter;

    /** Lock to update collection of actual metrics and cluster name changes. */
    private final ReentrantLock updateLock = new ReentrantLock();

    /** Collection of actual metrics protected by {@link #updateLock}. */
    private final Map<String, MetricData> metricsByName = new HashMap<>();

    /**
     * Creates a new instance of {@link MetricReporter}.
     *
     * @param log Logger.
     * @param srvcNamespace Service namespace.
     * @param srvcName Service name.
     * @param srvcId Service Identifier.
     * @param endpoint Endpoint to connect to.
     * @param protocol Protocol type to export metrics.
     * @param compression Compression type.
     * @param headers Connection headers.
     * @param sslEnabled {@code true} is SSL enabled.
     * @param sslContext SSL context instance to propagate to the OTLP exporter.
     * @param trustManager Trust manager to propagate to the OTLP exporter.
     */
    public MetricReporter(
        IgniteLogger log,
        @Nullable String srvcNamespace,
        String srvcName,
        String srvcId,
        String endpoint,
        Protocol protocol,
        Compression compression,
        Map<String, String> headers,
        boolean sslEnabled,
        SSLContext sslContext,
        X509TrustManager trustManager
    ) {
        assert srvcName != null && !srvcName.isEmpty() : "Service name must be specified.";
        assert srvcId != null && !srvcId.isEmpty() : "Service id must be specified.";

        this.log = log;
        this.resource = createResource(srvcNamespace, srvcName, srvcId);
        this.exporter = createExporter(
            endpoint, protocol, compression, headers,
            sslEnabled, sslContext, trustManager
        );
    }

    /** {@inheritDoc} */
    @Override public void close() {
        exporter.close();
    }

    /**
     * Pushes the given {@code mreg} metrics to the configured endpoint (OTEL collector).
     *
     * @param mreg Collection of metric registries to report.
     * @param filter Optional predicate to filter metric registries.
     */
    public void report(ReadOnlyMetricManager mreg, @Nullable Predicate<ReadOnlyMetricRegistry> filter) {
        Collection<MetricData> metricsToExport = new ArrayList<>();

        updateLock.lock();
        try {
            mreg.forEach(metricSet -> {
                if (filter != null && !filter.test(metricSet))
                    return;

                InstrumentationScopeInfo scope = InstrumentationScopeInfo.builder(metricSet.name()).build();

                for (Metric metric : metricSet) {
                    MetricData metricData = metricsByName.computeIfAbsent(
                        metric.name(),
                        name -> toMetricData(resource, scope, metric));

                    if (metricData instanceof IgniteHistogramMetricData) {
                        // check that the metric was not re-set
                        if (!((IgniteHistogramMetricData) metricData).isValid()) {
                            // need to re-create the metric, because histogram bounds were changed.
                            metricData = toMetricData(resource, scope, metric);

                            metricsByName.put(metric.name(), metricData);
                        }
                    }

                    if (metricData != null)
                        metricsToExport.add(metricData);
                }
            });
        }
        finally {
            updateLock.unlock();
        }

        CompletableResultCode res = exporter.export(metricsToExport);
        res.whenComplete(() -> {
            if (!res.isSuccess()) {
                Throwable err = res.getFailureThrowable();

                log.warning("Failed to export metrics [err=" + ((err != null) ? err.getMessage() : "N/A") + ']');
            }
        });
    }

    public void removeMetricSet(ReadOnlyMetricRegistry metricSet) {
        updateLock.lock();
        try {
            metricSet.forEach(metric -> metricsByName.remove(metric.name()));
        }
        finally {
            updateLock.unlock();
        }
    }

    private Resource createResource(@Nullable String srvcNamespace, String srvcName, String srvcId) {
        ResourceBuilder b = Resource.builder();

        if (srvcNamespace != null && !srvcNamespace.isEmpty())
            b.put(SERVICE_NAMESPACE, srvcNamespace);

        b.put(SERVICE_NAME, srvcName);
        b.put(SERVICE_INSTANCE_ID, srvcId);

        return Resource.getDefault().merge(b.build());
    }

    private MetricExporter createExporter(
        String endpoint,
        Protocol protocol,
        Compression compression,
        Map<String, String> headers,
        boolean sslEnabled,
        SSLContext sslContext,
        X509TrustManager trustManager
    ) {
        switch (protocol) {
            case HTTP: {
                OtlpHttpMetricExporterBuilder builder = OtlpHttpMetricExporter.builder()
                    .setEndpoint(createEndpoint(endpoint, protocol))
                    .setCompression(compression.type())
                    .setMemoryMode(REUSABLE_DATA);

                if (headers != null)
                    builder.setHeaders(() -> headers);

                if (sslEnabled)
                    builder.setSslContext(sslContext, trustManager);

                return builder.build();
            }

            case GRPC: {
                OtlpGrpcMetricExporterBuilder builder = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(createEndpoint(endpoint, protocol))
                    .setCompression(compression.type())
                    .setMemoryMode(REUSABLE_DATA);

                if (headers != null)
                    builder.setHeaders(() -> headers);

                if (sslEnabled)
                    builder.setSslContext(sslContext, trustManager);

                return builder.build();
            }

            default:
                throw new IllegalArgumentException();
        }
    }

    private static String createEndpoint(String endpoint, Protocol protocol) {
        URI uri = URI.create(endpoint);
        StringBuilder sb = new StringBuilder();

        if (Protocol.HTTP == protocol) {
            String basePath = uri.getPath() != null ? uri.getPath() : "";

            if (!basePath.isEmpty())
                sb.append(basePath);

            if (!basePath.endsWith("v1/metrics") && !basePath.endsWith("v1/metrics/")) {
                if (!basePath.endsWith("/"))
                    sb.append('/');

                sb.append("v1/metrics");
            }
        }

        try {
            return new URI(
                uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), sb.toString(), null, null
            ).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unexpected exception creating URL.", e);
        }
    }

    @SuppressWarnings("unchecked")
    private @Nullable MetricData toMetricData(Resource resource, InstrumentationScopeInfo scope, Metric metric) {
        if (metric instanceof IntMetric)
            return new IgniteIntMetricData(resource, scope, (IntMetric) metric);

        if (metric instanceof LongMetric)
            return new IgniteLongMetricData(resource, scope, (LongMetric) metric);

        if (metric instanceof DoubleMetric)
            return new IgniteDoubleMetricData(resource, scope, (DoubleMetric) metric);

        if (metric instanceof BooleanMetric)
            return new IgniteBooleanMetricData(resource, scope, (BooleanMetric) metric);

        if (metric instanceof HistogramMetric)
            return new IgniteHistogramMetricData(resource, scope, (HistogramMetric) metric);

        if (metric instanceof ObjectMetric) {
            ObjectMetric<?> objectMetric = (ObjectMetric<?>) metric;

            if (objectMetric.type() == java.util.Date.class)
                return new IgniteDateMetricData(resource, scope, (ObjectMetric<Date>) metric);
            else if (objectMetric.type() == java.time.OffsetDateTime.class)
                return new IgniteOffsetDateTimeMetricData(resource, scope, (ObjectMetric<OffsetDateTime>) metric);
        }

        if (log.isDebugEnabled()) {
            log.debug("Unknown metric class for export [" +
                "name=" + metric.name() + ", class=" + metric.getClass() + ']');
        }

        return null;
    }
}
