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

package org.apache.ignite.agent.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.agent.action.Session;

import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Utility methods.
 */
public class AgentUtils {
    /** Agents path. */
    private static final String AGENTS_PATH = "/agents";

    /** */
    public static final String[] EMPTY = {};

    /**
     * Default constructor.
     */
    private AgentUtils() {
        // No-op.
    }

    /**
     * @param s String with sensitive data.
     * @return Secured string.
     */
    public static String secured(String s) {
        int len = s.length();
        int toShow = len > 4 ? 4 : 1;

        return new String(new char[len - toShow]).replace('\0', '*') + s.substring(len - toShow, len);
    }

    /**
     * @param c Collection with sensitive data.
     * @return Secured string.
     */
    public static String secured(Collection<String> c) {
        return c.stream().map(AgentUtils::secured).collect(Collectors.joining(", "));
    }

    /**
     * @return String with short node UUIDs.
     */
    public static String nid8(Collection<UUID> nids) {
        return nids.stream().map(nid -> U.id8(nid).toUpperCase()).collect(Collectors.joining(",", "[", "]"));
    }

    /**
     * Simple entry generator.
     * 
     * @param key Key.
     * @param val Value.
     */
    public static <K, V> Map.Entry<K, V> entry(K key, V val) {
        return new AbstractMap.SimpleEntry<>(key, val);
    }

    /**
     * Collector.
     */
    public static <K, U> Collector<Map.Entry<K, U>, ?, Map<K, U>> entriesToMap() {
        return Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    /**
     * Helper method to get attribute.
     *
     * @param attrs Map with attributes.
     * @param name Attribute name.
     * @return Attribute value.
     */
    public static <T> T attribute(Map<String, Object> attrs, String name) {
        return (T)attrs.get(name);
    }

    /**
     * @param prefix Message prefix.
     * @param e Exception.
     */
    public static String extractErrorMessage(String prefix, Throwable e) {
        String causeMsg = F.isEmpty(e.getMessage()) ? e.getClass().getName() : e.getMessage();

        return prefix + ": " + causeMsg;
    }

    /**
     * @param srvUri Server uri.
     * @param clusterId Cluster ID.
     */
    public static String monitoringUri(String srvUri, UUID clusterId) {
        return srvUri + "/clusters/" + clusterId + "/monitoring-dashboard";
    }

    /**
     * Prepare server uri.
     */
    public static URI toWsUri(String srvUri) {
        URI uri = URI.create(srvUri);

        if (uri.getScheme().startsWith("http")) {
            try {
                uri = new URI("http".equalsIgnoreCase(uri.getScheme()) ? "ws" : "wss",
                        uri.getUserInfo(),
                        uri.getHost(),
                        uri.getPort(),
                        AGENTS_PATH,
                        uri.getQuery(),
                        uri.getFragment()
                );
            }
            catch (URISyntaxException x) {
                throw new IllegalArgumentException(x.getMessage(), x);
            }
        }

        return uri;
    }

    /**
     * @param igniteFut Ignite future.
     */
    public static <T> CompletableFuture completeIgniteFuture(IgniteFuture<T> igniteFut) {
        CompletableFuture<Object> fut = new CompletableFuture<>();
        igniteFut.chain(f -> {
            try {
                fut.complete(f.get());
            }
            catch (Exception ex) {
                fut.completeExceptionally(ex);
            }

            return f;
        });

        return fut;
    }

    /**
     * @param e Exception.
     */
    public static <T> CompletableFuture<T> completeFutureWithException(Throwable e) {
        CompletableFuture<T> fut = new CompletableFuture<>();
        fut.completeExceptionally(e);

        return fut;
    }

    /**
     * Authenticate by session.
     *
     * @param security Security.
     * @param ses Session.
     */
    public static SecurityContext authenticate(IgniteSecurity security, Session ses) throws IgniteAuthenticationException, IgniteCheckedException {
        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(ses.id());
        authCtx.nodeAttributes(Collections.emptyMap());
        authCtx.address(ses.address());
        authCtx.credentials(ses.credentials());

        SecurityContext subjCtx = security.authenticate(authCtx);

        if (subjCtx == null) {
            if (ses.credentials() == null)
                throw new IgniteAuthenticationException("Failed to authenticate remote client (secure session SPI not set?): " + ses);

            throw new IgniteAuthenticationException("Failed to authenticate remote client (invalid credentials?): " + ses);
        }

        return subjCtx;
    }

    /**
     * @param security Security.
     * @param perm Permission.
     */
    public static void authorizeIfNeeded(IgniteSecurity security, SecurityPermission perm) {
        authorizeIfNeeded(security, null, perm);
    }

    /**
     * @param security Security.
     * @param name Name.
     * @param perm Permission.
     */
    public static void authorizeIfNeeded(IgniteSecurity security, String name, SecurityPermission perm) {
        if (security.enabled())
            security.authorize(name, perm);
    }

    /**
     * @param ctx Context.
     * @param nodes Nodes.
     *
     * @return Set of supported cluster features.
     */
    public static Set<String> getClusterFeatures(GridKernalContext ctx, Collection<ClusterNode> nodes) {
        IgniteFeatures[] enums = IgniteFeatures.values();
        Set<String> features = U.newHashSet(enums.length);

        for (IgniteFeatures val : enums)
            if (allNodesSupports(ctx, nodes, val))
                features.add(val.name());

        return features;
    }

    /**
     * @param col Column.
     * @return Empty stream if collection is null else stream of collection elements.
     */
    public static <T> Stream<T> fromNullableCollection(Collection<T> col) {
        return col == null ? Stream.empty() : col.stream();
    }
}
