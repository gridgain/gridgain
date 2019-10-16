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

package org.gridgain.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.gridgain.agent.WebSocketManager;
import org.gridgain.dto.cache.CacheInfo;
import org.gridgain.dto.cache.CacheSqlIndexMetadata;
import org.gridgain.dto.cache.CacheSqlMetadata;

import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterCachesInfoDest;
import static org.gridgain.agent.StompDestinationsUtils.buildClusterCachesSqlMetaDest;

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

    /** Websocket manager. */
    private final WebSocketManager mgr;

    /**
     * @param ctx Context.
     * @param mgr Websocket manager.
     */
    public CacheService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;
        log = ctx.log(CacheService.class);

        GridEventStorageManager evtMgr = ctx.event();

        // Listener for cache metadata change.
        evtMgr.addLocalEventListener(this::onDiscoveryCustomEvent, EVT_DISCOVERY_CUSTOM_EVT);
        evtMgr.addLocalEventListener(this::onCacheEvents, EVTS_CACHE);
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
    private void onCacheEvents(Event evt) {
        sendCacheInfo();
    }

    /**
     * @param evt Event.
     */
    private void onDiscoveryCustomEvent(Event evt) {
        if (evt instanceof DiscoveryCustomEvent) {
            DiscoveryCustomMessage customMsg = ((DiscoveryCustomEvent) evt).customMessage();

            if (customMsg instanceof SchemaFinishDiscoveryMessage)
                sendCacheInfo();
        }
    }

    /**
     * Send caches information to GMC.
     */
    private void sendCacheInfo() {
        if (!ctx.isStopping()) {
            Collection<CacheInfo> cachesInfo = getCachesInfo();
            Map<String, CacheSqlMetadata> cacheSqlMetadata = getCacheSqlMetadata();

            UUID clusterId = ctx.cluster().get().id();
            mgr.send(buildClusterCachesInfoDest(clusterId), cachesInfo);
            mgr.send(buildClusterCachesSqlMetaDest(clusterId), cacheSqlMetadata);
        }
    }

    /**
     * @return Map of caches sql metadata.
     */
    private Map<String, CacheSqlMetadata> getCacheSqlMetadata() {
        GridCacheProcessor cacheProc = ctx.cache();
        Map<String, CacheSqlMetadata> cachesMetadata = new HashMap<>();

        for (Map.Entry<String, DynamicCacheDescriptor> item : cacheProc.cacheDescriptors().entrySet()) {
            if (item.getValue().sql()) {
                String cacheName = item.getKey();
                IgniteInternalCache<Object, Object> cache = ctx.grid().cachex(cacheName);

                if (cache != null) {
                    try {
                        GridCacheSqlMetadata meta = F.first(cache.context().queries().sqlMetadata());
                        Map<String, Collection<GridCacheSqlIndexMetadata>> src = meta.indexes();

                        CacheSqlMetadata metadata = new CacheSqlMetadata()
                            .setCacheName(meta.cacheName())
                            .setTypes(meta.types())
                            .setKeyClasses(meta.keyClasses())
                            .setValueClasses(meta.valClasses())
                            .setFields(meta.fields());

                        if (src != null) {
                            for (Map.Entry<String, Collection<GridCacheSqlIndexMetadata>> entry: src.entrySet()) {
                                Collection<GridCacheSqlIndexMetadata> idxs = entry.getValue();
                                List<CacheSqlIndexMetadata> res = new ArrayList<>(idxs.size());

                                for (GridCacheSqlIndexMetadata idx : idxs) {
                                    CacheSqlIndexMetadata idxMeta = new CacheSqlIndexMetadata()
                                        .setName(idx.name())
                                        .setFields(idx.fields())
                                        .setUnique(idx.unique())
                                        .setDescendings(idx.descendings());

                                    res.add(idxMeta);
                                }

                                metadata.getIndexes().put(entry.getKey(), res);
                            }
                        }

                        cachesMetadata.put(cacheName, metadata);
                    } catch (IgniteCheckedException e) {
                        e.printStackTrace();
                    }
                }
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
                    .setDeploymentId(cd.deploymentId().globalId())
                    .setGroup(cd.groupDescriptor().groupName())
            );
        }

        return cachesInfo;
    }


    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        ctx.event().removeLocalEventListener(this::onDiscoveryCustomEvent, EVT_DISCOVERY_CUSTOM_EVT);
        ctx.event().removeLocalEventListener(this::onCacheEvents, EVTS_CACHE);
    }
}
