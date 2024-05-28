/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.spi.communication.tcp.internal;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.BlacklistFilter;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationTcpUtils.IPv4Matcher;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests for {@link CommunicationTcpUtils}.
 */
public class CommunicationTcpUtilsTest extends GridCommonAbstractTest {
    @Test
    public void testExactIPv4Matcher() throws Exception {
        String netInterface = "127.127.127.127";

        IPv4Matcher matcher = new IPv4Matcher(netInterface);

        assertTrue(matcher.matches(InetAddress.getByName(netInterface)));
        assertFalse(matcher.matches(InetAddress.getByName("127.10.127.10")));
    }

    @Test
    public void testIPv4WrongNetworkInterface() {
        // Wrong segment value.
        String w1 = "127.127.127.300";
        String w2 = "127.127.127.-1";
        String w3 = "a.b.c.d";

        assertThrows(log, () -> new IPv4Matcher(w1), IllegalArgumentException.class, "Invalid IPv4 address: " + w1);
        assertThrows(log, () -> new IPv4Matcher(w2), IllegalArgumentException.class, "Invalid IPv4 address: " + w2);
        assertThrows(log, () -> new IPv4Matcher(w3), IllegalArgumentException.class, "Invalid IPv4 address: " + w3);

        // Wrong number of segments.
        String w4 = "127.127.127";
        String w5 = "127..127";
        String w6 = "";
        String w7 = "2001:db8:85a3:8d3:1319:8a2e:370:7348";

        assertThrows(log, () -> new IPv4Matcher(w4), IllegalArgumentException.class, "Invalid IPv4 address: " + w4);
        assertThrows(log, () -> new IPv4Matcher(w5), IllegalArgumentException.class, "Invalid IPv4 address: " + w5);
        assertThrows(log, () -> new IPv4Matcher(w6), IllegalArgumentException.class, "Invalid IPv4 address: " + w6);
        assertThrows(log, () -> new IPv4Matcher(w7), IllegalArgumentException.class, "Invalid IPv4 address: " + w7);
    }

    @Test
    public void testIPv4MatcherWildcard() throws Exception {
        String netInterface = "127.127.*.127";

        IPv4Matcher matcher = new IPv4Matcher(netInterface);

        assertTrue(matcher.matches(InetAddress.getByName("127.127.0.127")));
        assertTrue(matcher.matches(InetAddress.getByName("127.127.255.127")));
        assertTrue(matcher.matches(InetAddress.getByName("127.127.127.127")));

        assertFalse(matcher.matches(InetAddress.getByName("127.255.0.127")));
    }

    @Test
    public void testIPv4MatcherRange() throws Exception {
        String netInterface = "127.127.12-127.127";

        IPv4Matcher matcher = new IPv4Matcher(netInterface);

        assertTrue(matcher.matches(InetAddress.getByName("127.127.12.127")));
        assertTrue(matcher.matches(InetAddress.getByName("127.127.127.127")));
        assertTrue(matcher.matches(InetAddress.getByName("127.127.64.127")));

        assertFalse(matcher.matches(InetAddress.getByName("127.127.11.127")));
        assertFalse(matcher.matches(InetAddress.getByName("127.127.128.127")));
    }

    @Test
    public void testBlacklistFilter() throws Exception {
        List<String> blacklist = new ArrayList<>();
        blacklist.add("127.127.127.127");
        blacklist.add("127.127.255.*");
        blacklist.add("127.255.127.127-250");

        // BlacklistFilter returns {@code true} if the given address is not in the blacklist and {@code false} otherwise.
        BlacklistFilter filter = new BlacklistFilter(blacklist);

        assertFalse(filter.apply(InetAddress.getByName("127.127.127.127")));

        assertFalse(filter.apply(InetAddress.getByName("127.127.255.0")));
        assertFalse(filter.apply(InetAddress.getByName("127.127.255.255")));
        assertFalse(filter.apply(InetAddress.getByName("127.127.255.99")));

        assertFalse(filter.apply(InetAddress.getByName("127.255.127.200")));
        assertTrue(filter.apply(InetAddress.getByName("127.255.127.126")));
        assertTrue(filter.apply(InetAddress.getByName("127.255.127.251")));

        assertTrue(filter.apply(InetAddress.getByName("100.127.127.127")));
    }
}
