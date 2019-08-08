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

package org.gridgain.agent;

import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import io.opencensus.common.Function;
import io.opencensus.common.Functions;
import io.opencensus.common.Timestamp;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.MessageEvent;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.Status;
import io.opencensus.trace.export.SpanData;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.Socks4Proxy;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.gridgain.dto.Span;

import static java.net.Proxy.NO_PROXY;
import static java.net.Proxy.Type.SOCKS;
import static java.util.concurrent.TimeUnit.*;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static org.eclipse.jetty.client.api.Authentication.ANY_REALM;

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
}
