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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.AbstractExporterSpiTest;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Integration tests for {@link OpenTelemetryMetricExporterSpi} backed by a real OpenTelemetry
 * Collector running inside a Docker container (managed by Testcontainers).
 *
 * <p>The collector is configured with an OTLP/HTTP receiver and a Prometheus exporter so that
 * tests can verify what was actually exported by scraping the human-readable Prometheus endpoint —
 * no binary protobuf parsing is required.
 *
 * <p><b>Prerequisites:</b> a working Docker daemon must be reachable from the test JVM.
 *
 * <p>The pipeline inside the collector:
 * <pre>
 *   Ignite SPI  --[OTLP/HTTP:4318]-->  OTel Collector  --[Prometheus:8889]-->  test assertions
 * </pre>
 */
public class OpenTelemetryMetricExporterSpiIntegrationTest extends AbstractExporterSpiTest {
    /** Docker image for the OpenTelemetry Collector (contrib distribution includes Prometheus exporter). */
    private static final String COLLECTOR_IMAGE = "otel/opentelemetry-collector-contrib:0.148.0";

    /** Port inside the container where the OTLP HTTP receiver listens. */
    private static final int OTLP_HTTP_PORT = 4318;

    /** Port inside the container where the Prometheus scrape endpoint is exposed. */
    private static final int PROMETHEUS_PORT = 8889;

    /** How often the SPI pushes metrics (ms). Keep short so tests finish quickly. */
    private static final long EXPORT_PERIOD_MS = 500;

    /** Maximum time to wait for metrics to appear in Prometheus output (ms). */
    private static final long WAIT_TIMEOUT_MS = EXPORT_PERIOD_MS * 10;

    /** Service name written into the OTel resource and expected as a Prometheus label. */
    private static final String SERVICE_NAME = "test-ignite-cluster";

    /** Service namespace written into the OTel resource and expected as a Prometheus label. */
    private static final String SERVICE_NAMESPACE = "test-namespace";

    /** Collector container — started once for the whole test class. */
    private static GenericContainer<?> collector;

    /** Host-side port mapped from {@link #OTLP_HTTP_PORT}. Determined after container start. */
    private static int otlpPort;

    /** Host-side port mapped from {@link #PROMETHEUS_PORT}. Determined after container start. */
    private static int prometheusPort;

    /** Ignite node under test — shared across all tests in this class. */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        collector = new GenericContainer<>(COLLECTOR_IMAGE)
            .withClasspathResourceMapping(
                "otel-collector-config.yaml",
                "/etc/otelcol-contrib/config.yaml",
                BindMode.READ_ONLY)
            .withCommand("--config", "/etc/otelcol-contrib/config.yaml")
            .withCreateContainerCmdModifier(cmd ->
                cmd.getHostConfig()
                    .withMemory(256 * 1024 * 1024L) // 256 MB in bytes
            )
            .withExposedPorts(OTLP_HTTP_PORT, PROMETHEUS_PORT)
            .waitingFor(Wait.forHttp("/metrics").forPort(PROMETHEUS_PORT).forStatusCode(200));

        collector.start();

        otlpPort = collector.getMappedPort(OTLP_HTTP_PORT);
        prometheusPort = collector.getMappedPort(PROMETHEUS_PORT);

        ignite = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        if (collector != null)
            collector.stop();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        OpenTelemetryMetricExporterSpi spi = new OpenTelemetryMetricExporterSpi();

        spi.setProtocol(Protocol.HTTP);
        spi.setEndpoint("http://" + collector.getHost() + ":" + otlpPort);
        spi.setPeriod(EXPORT_PERIOD_MS);
        spi.setServiceName(SERVICE_NAME);
        spi.setServiceNamespace(SERVICE_NAMESPACE);
        spi.setExportFilter(reg -> !reg.name().startsWith(FILTERED_PREFIX));

        cfg.setMetricExporterSpi(spi);

        return cfg;
    }

    /**
     * Verifies that after the node starts, Ignite's own internal metrics appear in the
     * Prometheus output with the expected {@code service_name} and {@code service_namespace} labels.
     */
    @Test
    public void testInternalMetricsExported() throws Exception {
        assertTrue(
            "Expected Ignite internal metrics to appear in Prometheus output",
            waitForCondition(() -> {
                try {
                    String metrics = scrapePrometheus();

                    return metrics.contains(SERVICE_NAMESPACE + '/' + SERVICE_NAME);
                }
                catch (Exception e) {
                    return false;
                }
            }, WAIT_TIMEOUT_MS)
        );
    }

    /**
     * Verifies that a user-defined metric is exported with the correct value.
     * A {@code LongMetric} with value 42 is registered, and the test waits until the
     * Prometheus endpoint reflects that exact value.
     */
    @Test
    public void testMetricValueIsExported() throws Exception {
        MetricRegistry reg = ignite.context().metric().registry("test.registry");

        reg.longMetric("my.counter", "test counter").add(42);

        // Metric name: "my.counter" -> Prometheus normalizes dots to underscores: "my_counter"
        // Scope name: "test.registry" -> appears as label otel_scope_name="test.registry"
        assertTrue(
            "Expected user metric 'my_counter' with value 42 in Prometheus output",
            waitForCondition(() -> {
                try {
                    String metrics = scrapePrometheus();

                    return metrics.contains("otel_scope_name=\"test.registry\"")
                        && metrics.contains("my_counter")
                        && metrics.matches("(?s).*my_counter\\{[^}]*}\\s+42.*");
                }
                catch (Exception e) {
                    return false;
                }
            }, WAIT_TIMEOUT_MS)
        );
    }

    /**
     * Verifies that a metric value update is reflected in a subsequent export.
     * The metric is first exported with value 10, then updated to 99, and the test
     * waits for the new value to appear in Prometheus output.
     */
    @Test
    public void testMetricValueUpdateIsExported() throws Exception {
        MetricRegistry reg = ignite.context().metric().registry("update.registry");

        AtomicLongMetric m = reg.longMetric("update.metric", "");

        m.add(10);

        assertTrue("Initial value 10 should appear", waitForCondition(() -> {
            try {
                return scrapePrometheus().matches("(?s).*update_metric\\{[^}]*}\\s+10.*");
            }
            catch (Exception e) {
                return false;
            }
        }, WAIT_TIMEOUT_MS));

        m.add(89);

        assertTrue("Updated value 99 should appear", waitForCondition(() -> {
            try {
                return scrapePrometheus().matches("(?s).*update_metric\\{[^}]*}\\s+99.*");
            }
            catch (Exception e) {
                return false;
            }
        }, WAIT_TIMEOUT_MS));
    }

    /**
     * Verifies that changing a histogram metric bounds is reflected in a subsequent export.
     */
    @Test
    public void testHistogramMetricBoundsUpdateIsExported() throws Exception {
        MetricRegistry reg = ignite.context().metric().registry("update.registry");

        long[] initialBounds = {10, 100};
        HistogramMetricImpl hist = reg.histogram("update.histogram.metric", initialBounds, "");

        hist.value(42);

        assertTrue(
            "Initial value should appear",
            waitForCondition(() -> checkHistogramMetric(hist.name(), initialBounds, 1), WAIT_TIMEOUT_MS));

        long[] newBounds = {100, 500, 1000};
        hist.reset(newBounds);

        assertTrue(
            "New bounds should appear",
            waitForCondition(() -> checkHistogramMetric(hist.name(), newBounds, 0), WAIT_TIMEOUT_MS));

        hist.value(1000);

        assertTrue(
            "New bounds should appear",
            waitForCondition(() -> checkHistogramMetric(hist.name(), newBounds, 1), WAIT_TIMEOUT_MS));
    }

    private boolean checkHistogramMetric(String metricName, long[] bounds, int count) {
        String prometheusName = metricName.replace(".", "_");
        String bucketName = prometheusName + "_bucket";
        String countName = prometheusName + "_count";

        try {
            String prometheusMetrics = scrapePrometheus();

            // check buckets
            boolean res = true;
            for (long l : bounds)
                res = res && prometheusMetrics.matches("(?s).*" + bucketName + "\\{.*le=\"" + l + "\"}.*");

            res = res && prometheusMetrics.matches("(?s).*" + bucketName + "\\{.*le=\"\\+Inf\"}.*");
            res = res && prometheusMetrics.matches("(?s).*" + countName + "\\{[^}]*}\\s+" + count + ".*");

            return res;
        }
        catch (Exception e) {
            return false;
        }
    }

    /**
     * Verifies that registries whose name starts with {@link #FILTERED_PREFIX} are excluded from
     * exports. The test creates one filtered and one allowed registry, waits for the allowed one
     * to appear, and then asserts the filtered scope name is absent.
     */
    @Test
    public void testFilteredMetricsAreNotExported() throws Exception {
        MetricRegistry filtered = ignite.context().metric().registry(FILTERED_PREFIX + ".excluded");
        filtered.longMetric("secret", "").add(1);

        MetricRegistry allowed = ignite.context().metric().registry("visible.registry");
        allowed.longMetric("visible.metric", "").add(7);

        // Wait until the allowed metric is present before checking for the filtered one.
        assertTrue("Allowed metric must be exported", waitForCondition(() -> {
            try {
                return scrapePrometheus().contains("otel_scope_name=\"visible.registry\"");
            }
            catch (Exception e) {
                return false;
            }
        }, WAIT_TIMEOUT_MS));

        assertFalse(
            "Filtered registry must not appear in Prometheus output",
            scrapePrometheus().contains("otel_scope_name=\"" + FILTERED_PREFIX + ".excluded\"")
        );
    }

    /**
     * Fetches the current Prometheus text output from the collector's scrape endpoint.
     *
     * @return Prometheus exposition text.
     * @throws Exception if the HTTP request fails.
     */
    private String scrapePrometheus() throws Exception {
        URL url = new URL("http://" + collector.getHost() + ":" + prometheusPort + "/metrics");

        URLConnection conn = url.openConnection();

        SB s = new SB();
        try (InputStream in = conn.getInputStream()) {
            if (in == null)
                return "";

            BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8));
            for (String line; (line = reader.readLine()) != null; ) {
                if (line.isEmpty())
                    continue;

                s.a(line).a(U.nl());
            }
        }
        String res = s.toString();

        if (log.isDebugEnabled())
            log.debug(">>>>> prometheus collector output: " + res);

        return res;
    }
}
