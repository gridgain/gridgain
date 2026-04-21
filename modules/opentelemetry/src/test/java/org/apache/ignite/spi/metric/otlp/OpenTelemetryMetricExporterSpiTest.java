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

import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link OpenTelemetryMetricExporterSpi} configuration methods.
 * Tests cover default values, getters/setters, and validation of protocol and compression strings.
 * No Ignite node is started — these tests exercise only the SPI's own configuration logic.
 */
public class OpenTelemetryMetricExporterSpiTest {
    /** SPI under test, re-created before every test. */
    private OpenTelemetryMetricExporterSpi spi;

    /** */
    @Before
    public void setUp() {
        spi = new OpenTelemetryMetricExporterSpi();
    }

    /** Default protocol must be {@link OpenTelemetryMetricExporterSpi#DEFAULT_PROTOCOL}. */
    @Test
    public void testDefaultProtocol() {
        assertEquals(OpenTelemetryMetricExporterSpi.DEFAULT_PROTOCOL, spi.getProtocol());
        assertEquals(Protocol.GRPC, spi.getProtocol());
    }

    /** Default compression must be {@link OpenTelemetryMetricExporterSpi#DEFAULT_COMPRESSION}. */
    @Test
    public void testDefaultCompression() {
        assertEquals(OpenTelemetryMetricExporterSpi.DEFAULT_COMPRESSION, spi.getCompression());
        assertEquals(Compression.NONE, spi.getCompression());
    }

    /** Default endpoint must be {@link OpenTelemetryMetricExporterSpi#DEFAULT_ENDPOINT}. */
    @Test
    public void testDefaultEndpoint() {
        assertEquals(OpenTelemetryMetricExporterSpi.DEFAULT_ENDPOINT, spi.getEndpoint());
    }

    /** SSL must be disabled by default. */
    @Test
    public void testDefaultSslDisabled() {
        assertFalse(spi.isSslEnabled());
    }

    /** Ignite SSL context factory reuse must be enabled by default. */
    @Test
    public void testDefaultUseIgniteSslContextFactory() {
        assertTrue(spi.isUseIgniteSslContextFactory());
        assertEquals(OpenTelemetryMetricExporterSpi.DFLT_USE_IGNITE_SSL_CTX_FACTORY,
            spi.isUseIgniteSslContextFactory());
    }

    /** Service name must be {@code null} by default. */
    @Test
    public void testDefaultServiceName() {
        assertNull(spi.getServiceName());
    }

    /** Service namespace must be {@code null} by default. */
    @Test
    public void testDefaultServiceNamespace() {
        assertNull(spi.getServiceNamespace());
    }

    /** SSL context factory must be {@code null} by default. */
    @Test
    public void testDefaultSslContextFactory() {
        assertNull(spi.getSslContextFactory());
    }

    /** Trust manager factory must be {@code null} by default. */
    @Test
    public void testDefaultTrustManagerFactory() {
        assertNull(spi.getTrustManagerFactory());
    }

    /** {@link Protocol} enum values must be stored and returned unchanged. */
    @Test
    public void testSetProtocolEnum() {
        spi.setProtocol(Protocol.HTTP);
        assertEquals(Protocol.HTTP, spi.getProtocol());

        spi.setProtocol(Protocol.GRPC);
        assertEquals(Protocol.GRPC, spi.getProtocol());
    }

    /** Valid lower-case protocol strings must be accepted. */
    @Test
    public void testSetProtocolStringLowerCase() {
        spi.setProtocol("http");
        assertEquals(Protocol.HTTP, spi.getProtocol());

        spi.setProtocol("grpc");
        assertEquals(Protocol.GRPC, spi.getProtocol());
    }

    /** Protocol string matching must be case-insensitive. */
    @Test
    public void testSetProtocolStringCaseInsensitive() {
        spi.setProtocol("HTTP");
        assertEquals(Protocol.HTTP, spi.getProtocol());

        spi.setProtocol("GRPC");
        assertEquals(Protocol.GRPC, spi.getProtocol());

        spi.setProtocol("Http");
        assertEquals(Protocol.HTTP, spi.getProtocol());
    }

    /** An unrecognised protocol string must throw {@link IllegalArgumentException}. */
    @Test(expected = IllegalArgumentException.class)
    public void testSetProtocolStringInvalid() {
        spi.setProtocol("tcp");
    }

    /** An empty protocol string must throw {@link IllegalArgumentException}. */
    @Test(expected = IllegalArgumentException.class)
    public void testSetProtocolStringEmpty() {
        spi.setProtocol("");
    }

    /** {@link Compression} enum values must be stored and returned unchanged. */
    @Test
    public void testSetCompressionEnum() {
        spi.setCompression(Compression.GZIP);
        assertEquals(Compression.GZIP, spi.getCompression());

        spi.setCompression(Compression.NONE);
        assertEquals(Compression.NONE, spi.getCompression());
    }

    /** Valid lower-case compression strings must be accepted. */
    @Test
    public void testSetCompressionStringLowerCase() {
        spi.setCompression("gzip");
        assertEquals(Compression.GZIP, spi.getCompression());

        spi.setCompression("none");
        assertEquals(Compression.NONE, spi.getCompression());
    }

    /** Compression string matching must be case-insensitive. */
    @Test
    public void testSetCompressionStringCaseInsensitive() {
        spi.setCompression("GZIP");
        assertEquals(Compression.GZIP, spi.getCompression());

        spi.setCompression("NONE");
        assertEquals(Compression.NONE, spi.getCompression());

        spi.setCompression("GZip");
        assertEquals(Compression.GZIP, spi.getCompression());
    }

    /** An unrecognised compression string must throw {@link IllegalArgumentException}. */
    @Test(expected = IllegalArgumentException.class)
    public void testSetCompressionStringInvalid() {
        spi.setCompression("lz4");
    }

    /** An empty compression string must throw {@link IllegalArgumentException}. */
    @Test(expected = IllegalArgumentException.class)
    public void testSetCompressionStringEmpty() {
        spi.setCompression("");
    }

    /** Custom endpoint must be stored and returned unchanged. */
    @Test
    public void testSetEndpoint() {
        spi.setEndpoint("http://otel-collector:4317");
        assertEquals("http://otel-collector:4317", spi.getEndpoint());
    }

    /** Service name must be stored and returned unchanged. */
    @Test
    public void testSetServiceName() {
        spi.setServiceName("my-cluster");
        assertEquals("my-cluster", spi.getServiceName());
    }

    /** Service namespace must be stored and returned unchanged. */
    @Test
    public void testSetServiceNamespace() {
        spi.setServiceNamespace("my-org");
        assertEquals("my-org", spi.getServiceNamespace());
    }

    /** Connection headers must be stored and returned unchanged. */
    @Test
    public void testSetConnectionHeaders() {
        Map<String, String> headers = Collections.singletonMap("Authorization", "Bearer token");
        spi.setConnectionHeaders(headers);
        assertSame(headers, spi.getConnectionHeaders());
    }

    /** SSL enabled flag must be stored and returned correctly. */
    @Test
    public void testSetSslEnabled() {
        spi.setSslEnabled(true);
        assertTrue(spi.isSslEnabled());

        spi.setSslEnabled(false);
        assertFalse(spi.isSslEnabled());
    }

    /** {@code useIgniteSslContextFactory} flag must be stored and returned correctly. */
    @Test
    public void testSetUseIgniteSslContextFactory() {
        spi.setUseIgniteSslContextFactory(false);
        assertFalse(spi.isUseIgniteSslContextFactory());

        spi.setUseIgniteSslContextFactory(true);
        assertTrue(spi.isUseIgniteSslContextFactory());
    }

    /** SSL context factory must be stored and returned as the same instance. */
    @Test
    public void testSetSslContextFactory() {
        Factory<SSLContext> factory = () -> {
            try {
                return SSLContext.getDefault();
            }
            catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        };
        spi.setSslContextFactory(factory);
        assertSame(factory, spi.getSslContextFactory());
    }

    /** Trust manager factory must be stored and returned as the same instance. */
    @Test
    public void testSetTrustManagerFactory() {
        Factory<TrustManager> factory = () -> null;
        spi.setTrustManagerFactory(factory);
        assertSame(factory, spi.getTrustManagerFactory());
    }
}
