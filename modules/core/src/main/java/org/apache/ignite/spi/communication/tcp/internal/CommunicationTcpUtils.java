/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.lang.gridfunc.AlwaysTruePredicate;
import org.apache.ignite.internal.util.nio.GridNioException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.communication.tcp.AttributeNames;

/**
 * Common communication spi logic.
 */
public class CommunicationTcpUtils {
    /** No-op runnable. */
    public static final IgniteRunnable NOOP = () -> {};

    /** */
    private static final boolean THROUBLESHOOTING_LOG_ENABLED = IgniteSystemProperties
        .getBoolean(IgniteSystemProperties.IGNITE_TROUBLESHOOTING_LOGGER);

    /**
     * @param node Node.
     * @return {@code True} if can use in/out connection pair for communication.
     */
    public static boolean usePairedConnections(ClusterNode node, String attributeName) {
        Boolean attr = node.attribute(attributeName);

        return attr != null && attr;
    }

    /**
     * Write message type to output stream.
     *
     * @param os Output stream.
     * @param type Message type.
     * @throws IOException On error.
     */
    public static void writeMessageType(OutputStream os, short type) throws IOException {
        os.write((byte)(type & 0xFF));
        os.write((byte)((type >> 8) & 0xFF));
    }

    /**
     * @param node Node.
     * @param filterReachableAddrs Filter addresses flag.
     * @return Node addresses.
     * @throws IgniteCheckedException If node does not have addresses.
     */
    public static Collection<InetSocketAddress> nodeAddresses(
        ClusterNode node,
        boolean filterReachableAddrs,
        AttributeNames attrs,
        Supplier<ClusterNode> localNode
    )
        throws IgniteCheckedException {
        Collection<String> rmtAddrs0 = node.attribute(attrs.addresses());
        Collection<String> rmtHostNames0 = node.attribute(attrs.hostNames());
        Integer boundPort = node.attribute(attrs.port());
        Collection<InetSocketAddress> extAddrs = node.attribute(attrs.externalizableAttributes());

        boolean isRmtAddrsExist = (!F.isEmpty(rmtAddrs0) && boundPort != null);
        boolean isExtAddrsExist = !F.isEmpty(extAddrs);

        if (!isRmtAddrsExist && !isExtAddrsExist)
            throw new IgniteCheckedException("Failed to send message to the destination node. Node doesn't have any " +
                "TCP communication addresses or mapped external addresses. Check configuration and make sure " +
                "that you use the same communication SPI on all nodes. Remote node id: " + node.id());

        LinkedHashSet<InetSocketAddress> addrs;

        // Try to connect first on bound addresses.
        if (isRmtAddrsExist) {
            List<InetSocketAddress> addrs0 = new ArrayList<>(U.toSocketAddresses(rmtAddrs0, rmtHostNames0, boundPort, true));

            boolean sameHost = U.sameMacs(localNode.get(), node);

            addrs0.sort(U.inetAddressesComparator(sameHost));

            addrs = new LinkedHashSet<>(addrs0);
        }
        else
            addrs = new LinkedHashSet<>();

        // Then on mapped external addresses.
        if (isExtAddrsExist)
            addrs.addAll(extAddrs);

        if (filterReachableAddrs) {
            Set<InetAddress> allInetAddrs = U.newHashSet(addrs.size());

            for (InetSocketAddress addr : addrs) {
                // Skip unresolved as addr.getAddress() can return null.
                if (!addr.isUnresolved())
                    allInetAddrs.add(addr.getAddress());
            }

            List<InetAddress> reachableInetAddrs = U.filterReachable(allInetAddrs);

            if (reachableInetAddrs.size() < allInetAddrs.size()) {
                LinkedHashSet<InetSocketAddress> addrs0 = U.newLinkedHashSet(addrs.size());

                List<InetSocketAddress> unreachableInetAddr = new ArrayList<>(allInetAddrs.size() - reachableInetAddrs.size());

                for (InetSocketAddress addr : addrs) {
                    if (reachableInetAddrs.contains(addr.getAddress()))
                        addrs0.add(addr);
                    else
                        unreachableInetAddr.add(addr);
                }

                addrs0.addAll(unreachableInetAddr);

                addrs = addrs0;
            }
        }

        return addrs;
    }

    /**
     * Returns handshake exception with specific message.
     */
    public static IgniteSpiOperationTimeoutException handshakeTimeoutException() {
        return new IgniteSpiOperationTimeoutException("Failed to perform handshake due to timeout " +
            "(consider increasing 'connectionTimeout' configuration property).");
    }

    /**
     * @param errs Error.
     * @return {@code True} if error was caused by some connection IO error or IgniteCheckedException due to timeout.
     */
    public static boolean isRecoverableException(Throwable errs) {
        return X.hasCause(
            errs,
            IOException.class,
            HandshakeException.class,
            IgniteSpiOperationTimeoutException.class,
            GridNioException.class
        );
    }

    /**
     * Forcibly fails client node.
     *
     * Is used in a single situation if a client node is visible to discovery but is not reachable via comm protocol.
     *
     * @param nodeToFail Client node to forcible fail.
     * @param spiCtx Context to request node failing.
     * @param err Error to fail client with.
     * @param log Logger to print message about failed node to.
     */
    public static void failNode(ClusterNode nodeToFail,
        IgniteSpiContext spiCtx,
        Throwable err,
        IgniteLogger log
    ) {
        assert nodeToFail.isClient();

        String logMsg = "TcpCommunicationSpi failed to establish connection to node, node will be dropped from " +
            "cluster [rmtNode=" + nodeToFail + ']';

        if (THROUBLESHOOTING_LOG_ENABLED)
            U.error(log, logMsg, err);
        else
            U.warn(log, logMsg);

        spiCtx.failNode(nodeToFail.id(), "TcpCommunicationSpi failed to establish connection to node [" +
            "rmtNode=" + nodeToFail +
            ", err=" + err +
            ", connectErrs=" + X.getSuppressedList(err) + ']');
    }

    /**
     * Base class for network interface matchers.
     */
    public abstract static class NetworkInterfaceMatcher {
        /** Pattern of network interface. */
        private final String iface;

        /**
         * Creates a new instance of network interface matcher.
         * @param iface Pattern of network interface.
         */
        public NetworkInterfaceMatcher(String iface) {
            this.iface = iface;
        }

        /**
         * Gets the pattern of network interface.
         * @return Pattern of network interface.
         */
        public String networkInterface() {
            return iface;
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

    /**
     * Matcher for IPv4 addresses.
     */
    public static class IPv4Matcher extends NetworkInterfaceMatcher {
        /** Pattern for matching a single octet. */
        private static final Pattern DIGIT_PATTERN = Pattern.compile("\\d{1,3}");

        /** Pattern for matching a range. */
        private static final Pattern RANGE_PATTERN = Pattern.compile("\\d{1,3}\\s*-\\s*\\d{1,3}");

        /** Pattern for matching a wildcard. */
        private static final String WILDCARD_PATTERN = "*";

        /** Maximum allowed value of IPv4 octet. */
        private static final int IPV4_MAX_OCTET_VALUE = 255;

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
                throw new IllegalArgumentException("Invalid IPv4 address: " + networkInterface());

            try {
                for (int s = 0; s < segments.length; ++s) {
                    String segment = segments[s];

                    if (DIGIT_PATTERN.matcher(segment).matches()) {
                        int seg = Integer.parseInt(segment);
                        if (seg < 0 || seg > IPV4_MAX_OCTET_VALUE)
                            throw new IllegalArgumentException("Invalid IPv4 address: " + networkInterface());

                        segmentPred[s] = addr -> segment.equals(addr);
                    }
                    else if (RANGE_PATTERN.matcher(segment).matches()) {
                        final String[] range = segment.split("-");
                        if (range.length != 2)
                            throw new IllegalArgumentException("Invalid IPv4 address: " + networkInterface());

                        final int min = Integer.parseInt(range[0].trim());
                        final int max = Integer.parseInt(range[1].trim());

                        segmentPred[s] = addr -> {
                            try {
                                int seg = Integer.parseInt(addr);
                                return seg >= min && seg <= max;
                            } catch (NumberFormatException e) {
                                return false;
                            }
                        };
                    }
                    else if (WILDCARD_PATTERN.equals(segments[s]))
                        segmentPred[s] = ALWAYS_TRUE;
                    else
                        throw new IllegalArgumentException("Invalid IPv4 address: " + networkInterface());
                }
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid IPv4 address: " + networkInterface(), e);
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
    }

    /**
     * Represents a filter basen on blacklist of network interfaces.
     */
    public static class BlacklistFilter implements IgnitePredicate<InetAddress> {
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

            for (String pattern : blacklist)
                matchers.add(new IPv4Matcher(pattern));
        }

        /**
         * Returns {@code true} if the given address is not in the blacklist and {@code false} otherwise.
         *
         * @param inetAddress Addres to be checked
         * @return {@code true} if the given address is not in the blacklist and {@code false} otherwise.
         */
        @Override public boolean apply(InetAddress inetAddress) {
            for (NetworkInterfaceMatcher m : matchers)
                if (m.matches(inetAddress))
                    return false;

            return true;
        }
    }
}
