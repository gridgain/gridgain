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

package org.apache.ignite.agent.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.agent.WebSocketManager;
import org.apache.ignite.agent.dto.cache.CacheInfo;
import org.apache.ignite.agent.dto.cache.CacheSqlMetadata;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;

import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterCachesInfoDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterCachesSqlMetaDest;
import static org.apache.ignite.agent.utils.QueryUtils.queryTypesToMetadataList;
import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Cache service.
 */
public class CacheService implements AutoCloseable {
    /** Cache events. */
    private static final int[] EVTS_CACHE = new int[] {EVT_CACHE_STARTED, EVT_CACHE_STOPPED};

    /** Context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Events. */
    private final IgniteEvents events;

    /** Websocket manager. */
    private final WebSocketManager mgr;

    /**
     * @param ctx Context.
     * @param mgr Websocket manager.
     */
    public CacheService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;
        this.log = ctx.log(CacheService.class);
        this.events = ctx.grid().events();

        // Listener for cache metadata change.
        events.localListen(this::onDiscoveryCustomEvent, EVT_DISCOVERY_CUSTOM_EVT);
        events.localListen(this::onCacheEvents, EVTS_CACHE);
    }

    /**
     * Send initial state of caches information.
     */
    public void sendInitialState() {
        sendCacheInfo();
    }

    /**
     * @param evt Event.
     */
    private boolean onCacheEvents(Event evt) {
        sendCacheInfo();

        return true;
    }

    /**
     * @param evt Event.
     */
    private boolean onDiscoveryCustomEvent(Event evt) {
        if (evt instanceof DiscoveryCustomEvent) {
            DiscoveryCustomMessage customMsg = ((DiscoveryCustomEvent) evt).customMessage();

            if (customMsg instanceof SchemaFinishDiscoveryMessage)
                sendCacheInfo();
        }

        return true;
    }

    /**
     * Send caches information to GMC.
     */
    private void sendCacheInfo() {
        if (!ctx.isStopping() && mgr.isConnected()) {
            Collection<CacheInfo> cachesInfo = getCachesInfo();
            Collection<CacheSqlMetadata> cacheSqlMetadata = getCacheSqlMetadata();

            UUID clusterId = ctx.cluster().get().id();
            mgr.send(buildClusterCachesInfoDest(clusterId), cachesInfo);
            mgr.send(buildClusterCachesSqlMetaDest(clusterId), cacheSqlMetadata);
        }
    }

    /**
     * @return Map of caches sql metadata.
     */
    private Collection<CacheSqlMetadata> getCacheSqlMetadata() {
        GridCacheProcessor cacheProc = ctx.cache();
        List<CacheSqlMetadata> cachesMetadata = new ArrayList<>();

        for (Map.Entry<String, DynamicCacheDescriptor> item : cacheProc.cacheDescriptors().entrySet()) {
            if (item.getValue().sql()) {
                String cacheName = item.getKey();
                Collection<GridQueryTypeDescriptor> types = ctx.query().types(cacheName);

                if (types != null)
                    cachesMetadata.addAll(queryTypesToMetadataList(cacheName, types));
            }
        }

        return cachesMetadata;
    }

    /**
     * @return List of caches info.
     */
    private List<CacheInfo> getCachesInfo() {
        GridCacheProcessor cacheProc = ctx.cache();
        Map<String, DynamicCacheDescriptor> cacheDescriptors = cacheProc.cacheDescriptors();
        List<CacheInfo> cachesInfo = new ArrayList<>(cacheDescriptors.size());

        for (Map.Entry<String, DynamicCacheDescriptor> item : cacheDescriptors.entrySet()) {
            DynamicCacheDescriptor cd = item.getValue();

            cachesInfo.add(
                new CacheInfo()
                    .setName(item.getKey())
                    .setDeploymentId(cd.deploymentId())
                    .setGroup(cd.groupDescriptor().groupName())
            );
        }

        return cachesInfo;
    }


    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        events.stopLocalListen(this::onDiscoveryCustomEvent, EVT_DISCOVERY_CUSTOM_EVT);
        events.stopLocalListen(this::onCacheEvents, EVTS_CACHE);
    }
}
