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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link ClientAddressFinder} that resolves hostnames via DNS on every {@link #getAddresses()} call.
 *
 * <p>Accepts the same address formats as the rest of the thin-client stack:
 * {@code host}, {@code host:port}, {@code host:portFrom..portTo},
 * {@code [ipv6]:port}, bare IPv4 / IPv6 literals, etc.
 * When no port is specified {@link ClientConnectorConfiguration#DFLT_PORT} is used.</p>
 */
public class DnsClientAddressFinder implements ClientAddressFinder {
    private final @Nullable String[] addrs;

    private final IgniteLogger log;

    /** Parsed host/port-range entries; populated lazily on the first {@link #getAddresses()} call. */
    private List<HostAndPortRange> hosts = null;

    /**
     * Creates a new finder.
     *
     * @param addrs Address strings to resolve; may be {@code null}, in which case {@link #getAddresses()}
     *              always returns an empty array.
     * @param log   Logger used to emit warnings when a hostname cannot be resolved.
     */
    public DnsClientAddressFinder(@Nullable String[] addrs, IgniteLogger log) {
        this.addrs = addrs;
        this.log = log;
    }

    /**
     * Resolves all configured hostnames via DNS and returns the resulting {@code "ip:portFrom..portTo"} strings.
     *
     * <p>If a hostname cannot be resolved a warning is logged and that entry is silently skipped;
     * other addresses are still returned.  If an address string cannot be parsed a
     * {@link ClientException} is thrown.</p>
     *
     * @return Array of resolved addresses in {@code "ip:portFrom..portTo"} format; never {@code null}.
     * @throws ClientException if any configured address string cannot be parsed.
     */
    @Override public String[] getAddresses() {
        if (hosts == null) {
            if (addrs == null) {
                hosts = Collections.emptyList();
            } else {
                hosts = new ArrayList<>(addrs.length);
                for (String addr : addrs) {
                    try {
                        HostAndPortRange e = HostAndPortRange.parse(
                                addr,
                                ClientConnectorConfiguration.DFLT_PORT,
                                ClientConnectorConfiguration.DFLT_PORT,
                                "Failed to parse Ignite server address in DnsClientAddressFinder"
                        );

                        hosts.add(e);
                    }
                    catch (IgniteCheckedException e) {
                        throw new ClientException(e);
                    }
                }
            }
        }

        List<String> ret = new ArrayList<>(hosts.size());
        for (HostAndPortRange hapr : hosts) {
            String host = hapr.host();
            try {
                InetAddress[] addresses = InetAddress.getAllByName(host);
                for (InetAddress address : addresses) {
                    String e = address.getHostAddress() + ":" + hapr.portFrom() + ".." + hapr.portTo();
                    ret.add(e);
                }
            } catch (UnknownHostException e) {
                log.warning("Failed to resolve address: " + host, e);
            }
        }

        return ret.toArray(new String[0]);
    }
}
