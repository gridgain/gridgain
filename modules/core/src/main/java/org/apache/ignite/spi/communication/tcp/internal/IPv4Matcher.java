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
import java.util.regex.Pattern;
import org.apache.ignite.internal.util.lang.gridfunc.AlwaysTruePredicate;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Matcher for IPv4 addresses.
 */
public class IPv4Matcher extends NetworkInterfaceMatcher {
    /** Pattern for matching a single octet. */
    private static final Pattern OCTET_PATTERN = Pattern.compile("\\d{1,3}");

    /** Pattern for matching a range. */
    private static final Pattern RANGE_PATTERN = Pattern.compile("\\d{1,3}\\s*-\\s*\\d{1,3}");

    /** Pattern for matching a wildcard. */
    private static final String WILDCARD_PATTERN = "*";

    /** Maximum allowed value of IPv4 octet. */
    private static final int IPV4_MAX_OCTET_VALUE = 255;

    /** Error message for invalid IPv4 address pattern. */
    private static final String INVALID_IPv4_PATTERN_ERR_MSG = "Invalid IPv4 address pattern: ";

    /** Shared predicate that always returns {@code true}. */
    private static final IgnitePredicate<String> ALWAYS_TRUE = new AlwaysTruePredicate<>();

    /** List of predicates for each segment. */
    private final IgnitePredicate<String>[] segmentPred = new IgnitePredicate[4];

    /**
     * Creates a new instance of IPv4 address matcher.
     * @param interfacePattern Pattern of network interface.
     */
    public IPv4Matcher(String interfacePattern) {
        super(interfacePattern);

        String[] segments = networkInterface().split("\\.");
        if (segments.length != 4)
            throw new IllegalArgumentException(INVALID_IPv4_PATTERN_ERR_MSG + networkInterface());

        try {
            for (int s = 0; s < segments.length; ++s) {
                String segment = segments[s];

                if (OCTET_PATTERN.matcher(segment).matches()) {
                    int seg = Integer.parseInt(segment);
                    if (seg < 0 || seg > IPV4_MAX_OCTET_VALUE)
                        throw new IllegalArgumentException(INVALID_IPv4_PATTERN_ERR_MSG + networkInterface());

                    segmentPred[s] = segmentToCheck -> segment.equals(segmentToCheck);
                }
                else if (RANGE_PATTERN.matcher(segment).matches()) {
                    final String[] range = segment.split("-");
                    if (range.length != 2)
                        throw new IllegalArgumentException(INVALID_IPv4_PATTERN_ERR_MSG + networkInterface());

                    final int min = Integer.parseInt(range[0].trim());
                    final int max = Integer.parseInt(range[1].trim());
                    if (min > max)
                        throw new IllegalArgumentException(INVALID_IPv4_PATTERN_ERR_MSG + networkInterface());

                    segmentPred[s] = segmentToCheck -> {
                        try {
                            int seg = Integer.parseInt(segmentToCheck);
                            return seg >= min && seg <= max;
                        } catch (NumberFormatException e) {
                            return false;
                        }
                    };
                }
                else if (WILDCARD_PATTERN.equals(segments[s]))
                    segmentPred[s] = ALWAYS_TRUE;
                else
                    throw new IllegalArgumentException(INVALID_IPv4_PATTERN_ERR_MSG + networkInterface());
            }
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(INVALID_IPv4_PATTERN_ERR_MSG + networkInterface(), e);
        }
    }

    /** {@inheritDoc} */
    @Override boolean matches(InetAddress addr) {
        String[] segments = addr.getHostAddress().split("\\.");

        if (segments.length != 4)
            return false;

        for (int s = 0; s < segments.length; ++s) {
            if (!segmentPred[s].apply(segments[s]))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IPv4Matcher.class, this, super.toString());
    }
}
