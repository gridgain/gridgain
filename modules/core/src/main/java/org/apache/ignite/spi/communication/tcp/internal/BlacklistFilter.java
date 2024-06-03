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
import java.util.Collection;
import java.util.List;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Represents a filter basen on blacklist of network interfaces.
 *
 * This filter returns {@code true} if the address to be checked is not in the blacklist and {@code false} otherwise.
 */
public class BlacklistFilter implements IgnitePredicate<InetAddress> {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** List of matchers. */
    private final List<NetworkInterfaceMatcher> matchers;

    /**
     * Creates a new instance of network interface filter.
     *
     * @param blacklist List of network interface patterns.
     */
    public BlacklistFilter(Collection<String> blacklist) {
        matchers = new ArrayList<>(blacklist.size());

        for (String pattern : blacklist) {
            if (pattern == null || pattern.trim().isEmpty())
                continue;

            matchers.add(new IPv4Matcher(pattern));
        }
    }

    /**
     * Returns {@code true} if the given address is not in the blacklist and {@code false} otherwise.
     *
     * @param inetAddress Address to be checked
     * @return {@code true} if the given address is not in the blacklist and {@code false} otherwise.
     */
    @Override public boolean apply(InetAddress inetAddress) {
        for (NetworkInterfaceMatcher m : matchers)
            if (m.matches(inetAddress))
                return false;

        return true;
    }
}
