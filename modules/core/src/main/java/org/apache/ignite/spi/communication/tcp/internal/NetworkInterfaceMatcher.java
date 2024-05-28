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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Base class for network interface matchers.
 *
 * An implementation of the interface should support using ranges and wildcards in the list of interfaces.
 * Wildcard symbol {@code *} represents a range of values between {@code 0} and {@code 255}. For example, {@code 12.12.12.*} refers to
 * addresses from {@code 12.12.12.0} to {@code 12.12.12.255}. Range symbol {@code -} represents a range of values. For example,
 * {@code 12.12.12.12-24} refers to addresses from {@code 12.12.12.12} to {@code 12.12.12.24}.
 */
public abstract class NetworkInterfaceMatcher {
    /** Pattern of network interface. */
    private final String netInterfacePattern;

    /**
     * Creates a new instance of network interface matcher.
     * @param iface Pattern of network interface.
     */
    public NetworkInterfaceMatcher(String iface) {
        netInterfacePattern = iface;
    }

    /**
     * Gets the pattern of network interface.
     * @return Pattern of network interface.
     */
    public String networkInterfacePattern() {
        return netInterfacePattern;
    }

    /**
     * Matches the given address against the pattern.
     *
     * @param addr Address to match.
     * @return {@code true} if the address matches the pattern, {@code false} otherwise.
     */
    abstract boolean matches(InetAddress addr);

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NetworkInterfaceMatcher.class, this);
    }
}
