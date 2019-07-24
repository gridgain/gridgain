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

package org.apache.ignite.console.discovery;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.DiscoverySpiHistorySupport;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Special discovery for isolated single-node cluster.
 */
@IgniteSpiMultipleInstancesSupport(true)
@DiscoverySpiHistorySupport(true)
public class IsolatedDiscoverySpi extends IgniteSpiAdapter implements DiscoverySpi {
    /** */
    private final ClusterNode locNode = new IsolatedNode();

    /** */
    private final long startTime = System.currentTimeMillis();

    /** */
    private DiscoverySpiListener lsnr;

    /** */
    private Executor exec = Executors.newSingleThreadExecutor();

    /** {@inheritDoc} */
    @Override public Serializable consistentId() throws IgniteSpiException {
        return "standalone-isolated-node";
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> getRemoteNodes() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getLocalNode() {
        return locNode;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getNode(UUID nodeId) {
        return locNode.id().equals(nodeId) ? locNode : null;
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {

    }

    /** {@inheritDoc} */
    @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public void setDataExchange(DiscoverySpiDataExchange exchange) {

    }

    /** {@inheritDoc} */
    @Override public void setMetricsProvider(DiscoveryMetricsProvider metricsProvider) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void disconnect() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setAuthenticator(DiscoverySpiNodeAuthenticator auth) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long getGridStartTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
        exec.execute(() -> lsnr.onDiscovery(DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT,
            1,
            locNode,
            Collections.singleton(locNode),
            null,
            msg));
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isClientMode() throws IllegalStateException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        exec.execute(() -> lsnr.onDiscovery(
            EVT_NODE_JOINED,
            1,
            locNode,
            Collections.singleton(locNode),
            null,
            null
        ));
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(final IgniteSpiContext spiCtx) throws IgniteSpiException {
        // No-op.
    }
}
