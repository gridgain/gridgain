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

package org.apache.ignite.client;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.util.Arrays;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link DnsClientAddressFinder}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DnsClientAddressFinderTest {
    private static final int DFLT = ClientConnectorConfiguration.DFLT_PORT;

    @Mock
    private IgniteLogger log;

    /** {@code null} addrs must yield an empty array without NPE. */
    @Test
    public void testNullAddrs() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(null, log);
        String[] result = finder.getAddresses();

        assertNotNull(result);
        assertEquals(0, result.length);
    }

    /** Empty addrs array must yield an empty array. */
    @Test
    public void testEmptyAddrs() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[0], log);
        String[] result = finder.getAddresses();

        assertNotNull(result);
        assertEquals(0, result.length);
    }

    /** Host without a port must use the default thin-client port. */
    @Test
    public void testHostOnlyUsesDefaultPort() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"127.0.0.1"}, log);
        String[] result = finder.getAddresses();

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("127.0.0.1:" + DFLT + ".." + DFLT, result[0]);
    }

    /** {@code host:port} must produce {@code ip:port..port}. */
    @Test
    public void testExplicitSinglePort() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"127.0.0.1:9999"}, log);
        String[] result = finder.getAddresses();

        assertEquals(1, result.length);
        assertEquals("127.0.0.1:9999..9999", result[0]);
    }

    /** {@code host:portFrom..portTo} must preserve the full port range. */
    @Test
    public void testPortRange() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"127.0.0.1:10800..10900"}, log);
        String[] result = finder.getAddresses();

        assertEquals(1, result.length);
        assertEquals("127.0.0.1:10800..10900", result[0]);
    }

    /** A bare IPv4 literal must be resolved to itself with the default port. */
    @Test
    public void testIpv4AddressDefaultPort() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"192.168.1.1"}, log);
        String[] result = finder.getAddresses();

        assertEquals(1, result.length);
        assertEquals("192.168.1.1:" + DFLT + ".." + DFLT, result[0]);
    }

    /** A bare IPv4 literal with an explicit port must use that port. */
    @Test
    public void testIpv4AddressExplicitPort() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"192.168.1.1:8080"}, log);
        String[] result = finder.getAddresses();

        assertEquals(1, result.length);
        assertEquals("192.168.1.1:8080..8080", result[0]);
    }

    /** Bracketed IPv6 loopback without a port must resolve and use the default port. */
    @Test
    public void testIpv6LoopbackDefaultPort() throws Exception {
        String expected = InetAddress.getByName("::1").getHostAddress();

        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"[::1]"}, log);
        String[] result = finder.getAddresses();

        assertEquals(1, result.length);
        assertEquals(expected + ":" + DFLT + ".." + DFLT, result[0]);
    }

    /** Bracketed IPv6 loopback with an explicit port must use that port. */
    @Test
    public void testIpv6LoopbackExplicitPort() throws Exception {
        String expected = InetAddress.getByName("::1").getHostAddress();

        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"[::1]:8080"}, log);
        String[] result = finder.getAddresses();

        assertEquals(1, result.length);
        assertEquals(expected + ":8080..8080", result[0]);
    }

    /** Multiple valid addresses must all be resolved and returned. */
    @Test
    public void testMultipleAddresses() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(
                new String[]{"127.0.0.1:10800", "127.0.0.1:10801"}, log);
        String[] result = finder.getAddresses();

        assertEquals(2, result.length);
        assertEquals("127.0.0.1:10800..10800", result[0]);
        assertEquals("127.0.0.1:10801..10801", result[1]);
    }

    // ── DNS resolution failure ─────────────────────────────────────────────

    /**
     * A hostname that cannot be resolved must be skipped and a warning must be logged;
     * other valid addresses must still be returned.
     *
     * <p>RFC 2606 / RFC 6761 guarantee that {@code .invalid} TLD never resolves.</p>
     */
    @Test
    public void testUnresolvableHostLogsWarningAndContinues() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(
                new String[]{"this-host-does-not-exist.invalid", "127.0.0.1"}, log);
        String[] result = finder.getAddresses();

        // Only the resolvable address is returned.
        assertEquals(1, result.length);
        assertEquals("127.0.0.1:" + DFLT + ".." + DFLT, result[0]);

        // A warning must have been logged for the unresolvable host.
        verify(log).warning(
                ArgumentMatchers.contains("this-host-does-not-exist.invalid"),
                ArgumentMatchers.any(Throwable.class)
        );
    }

    /** When all addresses fail to resolve the result is an empty array. */
    @Test
    public void testAllUnresolvableReturnsEmpty() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(
                new String[]{"totally-bogus.invalid"}, log);
        String[] result = finder.getAddresses();

        assertNotNull(result);
        assertEquals(0, result.length);
    }

    // ── malformed address strings ──────────────────────────────────────────

    /** An empty address string must throw {@link ClientException}. */
    @Test
    public void testEmptyAddressStringThrows() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{""}, log);
        assertThrows(null, finder::getAddresses, ClientException.class, null);
    }

    /** A host-only address with a colon but no port must throw {@link ClientException}. */
    @Test
    public void testMissingPortAfterColonThrows() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"hostname:"}, log);
        assertThrows(null, finder::getAddresses, ClientException.class, null);
    }

    /** A non-numeric port must throw {@link ClientException}. */
    @Test
    public void testNonNumericPortThrows() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"127.0.0.1:abc"}, log);
        assertThrows(null, finder::getAddresses, ClientException.class, null);
    }

    /** A port out of range (> 65535) must throw {@link ClientException}. */
    @Test
    public void testPortOutOfRangeThrows() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"127.0.0.1:99999"}, log);
        assertThrows(null, finder::getAddresses, ClientException.class, null);
    }

    /** An inverted port range (from > to) must throw {@link ClientException}. */
    @Test
    public void testInvertedPortRangeThrows() {
        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"127.0.0.1:10900..10800"}, log);
        assertThrows(null, finder::getAddresses, ClientException.class, null);
    }

    /**
     * When a hostname resolves to multiple IP addresses (simulated by using "localhost" which on some
     * systems returns both 127.0.0.1 and ::1), all resolved IPs must be included in the result.
     *
     * <p>We assert that at least one result matches 127.0.0.1 and that the format is correct.</p>
     */
    @Test
    public void testHostnameResolvingToMultipleIps() throws Exception {
        InetAddress[] allByName = InetAddress.getAllByName("localhost");

        DnsClientAddressFinder finder = new DnsClientAddressFinder(new String[]{"localhost"}, log);
        String[] result = finder.getAddresses();

        assertNotNull(result);
        // Result count must match the number of addresses returned by DNS.
        assertEquals(allByName.length, result.length);

        // Every result must follow the ip:portFrom..portTo format with the default port.
        for (String addr : result) {
            assertTrue("Unexpected address format: " + addr,
                    addr.endsWith(":" + DFLT + ".." + DFLT));
        }

        // All resolved IPs from the DNS lookup must appear in the result.
        for (InetAddress ia : allByName) {
            String expected = ia.getHostAddress() + ":" + DFLT + ".." + DFLT;
            assertTrue("Expected address not found: " + expected,
                    Arrays.asList(result).contains(expected));
        }
    }
}
