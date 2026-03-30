package org.apache.ignite.spi.metric.otlp;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class OtlpMetricsExporterTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setMetricExporterSpi(getMetricExporterSpi());

        return cfg;
    }

    /**
     * @return Tracing SPI to be used within tests.
     */
    protected MetricExporterSpi getMetricExporterSpi() {
        OpenTelemetryMetricExporterSpi spi = new OpenTelemetryMetricExporterSpi();

        spi.setPeriod(10_000);
        spi.setServiceNamespace("test.namespace");
        spi.setServiceName("test.service.name");
        spi.setEndpoint("http://localhost:4318/v1/metrics");

        return spi;
    }

    @BeforeClass
    public static void beforeTests() {
        // Start otel collector to see exported metrics.
        // docker run --rm -v $(pwd)/otel-config.yaml:/etc/otelcol-contrib/config.yaml
        // -p 4317:4317 -p 4318:4318 -p 8889:8889
        // otel/opentelemetry-collector-contrib:0.148.0 --config /etc/otelcol-contrib/config.yaml

        // Zipkin or in-memory?
    }

    @Test
    public void testAAA() throws Exception {
        Ignite crd = startGrid();

        doSleep(10_000);

        crd.getOrCreateCache("test-cache");

        doSleep(30_000);

        crd.cache("test-cache").destroy();

        doSleep(30_000);
    }
}
