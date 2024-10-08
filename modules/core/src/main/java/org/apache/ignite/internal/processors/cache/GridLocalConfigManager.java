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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isPersistentCache;
import static org.apache.ignite.internal.processors.query.QueryUtils.normalizeObjectName;
import static org.apache.ignite.internal.processors.query.QueryUtils.normalizeSchemaName;

/**
 * Responsible for restoring local cache configurations (both from static configuration and persistence).
 * Keep stop sequence of caches and caches which were presented on node before node join.
 */
public class GridLocalConfigManager {
    /** */
    private final boolean startClientCaches =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_START_CACHES_ON_JOIN, false);

    /** Caches stop sequence. */
    private final Deque<String> stopSeq = new LinkedList<>();

    /** Logger. */
    private final IgniteLogger log;

    /** Node's local caches on start (both from static configuration and from persistent caches). */
    private Set<String> localCachesOnStart;

    /** Cache processor. */
    private final GridCacheProcessor cacheProcessor;

    /** Context. */
    private final GridKernalContext ctx;

    /**
     * @param cacheProcessor Cache processor.
     * @param kernalCtx Kernal context.
     */
    public GridLocalConfigManager(
        GridCacheProcessor cacheProcessor,
        GridKernalContext kernalCtx
    ) {
        this.cacheProcessor = cacheProcessor;
        ctx = kernalCtx;
        log = ctx.log(getClass());
    }

    /**
     * Save cache configuration to persistent store if necessary.
     *
     * @param storedCacheData Stored cache data.
     * @param overwrite Overwrite existing.
     */
    public void saveCacheConfiguration(StoredCacheData storedCacheData, boolean overwrite) throws IgniteCheckedException {
        assert storedCacheData != null;

        GridCacheSharedContext<Object, Object> sharedContext = cacheProcessor.context();

        if (sharedContext.pageStore() != null
            && !sharedContext.kernalContext().clientNode()
            && isPersistentCache(storedCacheData.config(), sharedContext.gridConfig().getDataStorageConfiguration()))
            sharedContext.pageStore().storeCacheData(storedCacheData, overwrite);
    }

    /**
     *
     */
    public Collection<String> stopSequence() {
        return stopSeq;
    }

    /**
     * @return Caches to be started when this node starts.
     */
    public Set<String> localCachesOnStart() {
        return localCachesOnStart;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public CacheJoinNodeDiscoveryData restoreCacheConfigurations() throws IgniteCheckedException {
        if (ctx.isDaemon())
            return null;

        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches = new HashMap<>();

        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates = new HashMap<>();

        restoreCaches(caches, templates, ctx.config(), ctx.cache().context().pageStore());

        CacheJoinNodeDiscoveryData discoData = new CacheJoinNodeDiscoveryData(
            IgniteUuid.randomUuid(),
            caches,
            templates,
            startAllCachesOnClientStart()
        );

        localCachesOnStart = new HashSet<>(discoData.caches().keySet());

        return discoData;
    }

    /**
     * @return {@code True} if need locally start all existing caches on client node start.
     */
    private boolean startAllCachesOnClientStart() {
        return startClientCaches && ctx.clientNode();
    }

    /**
     * @param caches Caches accumulator.
     * @param templates Templates accumulator.
     * @param config Ignite configuration.
     * @param pageStoreManager Page store manager.
     */
    private void restoreCaches(
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates,
        IgniteConfiguration config,
        IgnitePageStoreManager pageStoreManager
    ) throws IgniteCheckedException {
        assert !config.isDaemon() : "Trying to restore cache configurations on daemon node.";

        CacheConfiguration[] cfgs = config.getCacheConfiguration();

        for (int i = 0; i < cfgs.length; i++) {
            CacheConfiguration<?, ?> cfg = new CacheConfiguration(cfgs[i]);

            // Replace original configuration value.
            cfgs[i] = cfg;

            addCacheFromConfiguration(cfg, false, caches, templates);
        }

        if (CU.isPersistenceEnabled(config) && pageStoreManager != null) {
            Map<String, StoredCacheData> storedCaches = pageStoreManager.readCacheConfigurations();

            if (!F.isEmpty(storedCaches)) {
                List<String> skippedConfigs = new ArrayList<>();

                for (StoredCacheData storedCacheData : storedCaches.values()) {
                    // Backward compatibility for old stored caches data.
                    if (storedCacheData.hasOldCacheConfigurationFormat()) {
                        storedCacheData = new StoredCacheData(storedCacheData);

                        T2<CacheConfiguration, CacheConfigurationEnrichment> splitCfg =
                            cacheProcessor.splitter().split(storedCacheData.config());

                        storedCacheData.config(splitCfg.get1());
                        storedCacheData.cacheConfigurationEnrichment(splitCfg.get2());

                        // Overwrite with new format.
                        saveCacheConfiguration(storedCacheData, true);
                    }

                    String cacheName = storedCacheData.config().getName();

                    CacheType type = CacheType.cacheType(cacheName);

                    if (!caches.containsKey(cacheName))
                        // No static cache - add the configuration.
                        addStoredCache(caches, storedCacheData, cacheName, type, true, false);
                    else {
                        // A static cache with the same name already exists.
                        CacheConfiguration cfg = caches.get(cacheName).cacheData().config();
                        CacheConfiguration cfgFromStore = storedCacheData.config();

                        validateCacheConfigurationOnRestore(cfg, cfgFromStore);

                        addStoredCache(caches, storedCacheData, cacheName, type, true,
                            cacheProcessor.keepStaticCacheConfiguration());

                        if (!cacheProcessor.keepStaticCacheConfiguration() && type == CacheType.USER)
                            skippedConfigs.add(cacheName);

                    }
                }

                if (!F.isEmpty(skippedConfigs)) {
                    U.warn(log, "Static configuration for the following caches will be ignored because a persistent " +
                        "cache with the same name already exist (see " +
                        "https://www.gridgain.com/docs/latest/developers-guide/configuring-caches/configuration-overview for more information): " +
                        skippedConfigs);
                }
            }
        }
    }

    /**
     * Add stored cache data to caches storage.
     *
     * @param caches Cache storage.
     * @param cacheData Cache data to add.
     * @param cacheName Cache name.
     * @param cacheType Cache type.
     * @param isStaticallyConfigured Statically configured flag.
     */
    private void addStoredCache(
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        StoredCacheData cacheData,
        String cacheName,
        CacheType cacheType,
        boolean persistedBefore,
        boolean isStaticallyConfigured
    ) {
        if (!caches.containsKey(cacheName)) {
            if (!cacheType.userCache())
                stopSeq.addLast(cacheName);
            else
                stopSeq.addFirst(cacheName);
        }

        caches.put(cacheName, new CacheJoinNodeDiscoveryData.CacheInfo(cacheData, cacheType, cacheData.sql(),
            persistedBefore ? 1 : 0, isStaticallyConfigured));
    }

    /**
     * @param cfg Cache configuration.
     * @param sql SQL flag.
     * @param caches Caches map.
     * @param templates Templates map.
     * @throws IgniteCheckedException If failed.
     */
    private void addCacheFromConfiguration(
        CacheConfiguration<?, ?> cfg,
        boolean sql,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates
    ) throws IgniteCheckedException {
        String cacheName = cfg.getName();

        CU.validateCacheName(cacheName);

        Collection<CacheConfiguration<?, ?>> ccfgs = new ArrayList<>(caches.size());

        for (CacheJoinNodeDiscoveryData.CacheInfo cacheInfo : caches.values())
            ccfgs.add(cacheInfo.cacheData().config());

        String err = validateIncomingConfiguration(ccfgs, cfg);

        if (err != null)
            throw new IgniteException(err);

        cacheProcessor.cloneCheckSerializable(cfg);

        // Initialize defaults.
        cacheProcessor.initialize(cfg, ctx.cacheObjects().contextForCache(cfg));

        StoredCacheData cacheData = new StoredCacheData(cfg);

        cacheData.sql(sql);

        T2<CacheConfiguration, CacheConfigurationEnrichment> splitCfg = cacheProcessor.splitter().split(cfg);

        cacheData.config(splitCfg.get1());
        cacheData.cacheConfigurationEnrichment(splitCfg.get2());

        cfg = splitCfg.get1();

        if (GridCacheUtils.isCacheTemplateName(cacheName))
            templates.put(cacheName, new CacheJoinNodeDiscoveryData.CacheInfo(cacheData, CacheType.USER, false, 0, true));
        else {
            if (caches.containsKey(cacheName)) {
                throw new IgniteCheckedException("Duplicate cache name found (check configuration and " +
                    "assign unique name to each cache): " + cacheName);
            }

            CacheType cacheType = CacheType.cacheType(cacheName);

            if (cacheType != CacheType.USER && cfg.getDataRegionName() == null)
                cfg.setDataRegionName(cacheProcessor.context().database().systemDateRegionName());

            addStoredCache(caches, cacheData, cacheName, cacheType, false, true);
        }
    }

    /**
     * Validates already processed cache configuration instead a newly defined.
     *
     * @param cacheConfigs Already processed caches.
     * @param cfg Currently processed cache config.
     * @return Error message, if supplied configuration is incorrect.
     * @throws IgniteException If misconfigured.
     */
    @Nullable public static String validateIncomingConfiguration(
        Collection<CacheConfiguration<?, ?>> cacheConfigs,
        CacheConfiguration<?, ?> cfg
    ) {
        Map<String, String> idxNamesPerCache = new HashMap<>();

        String schemaName = normalizeSchemaName(cfg.getName(), cfg.getSqlSchema());

        for (CacheConfiguration<?, ?> conf0 : cacheConfigs) {
            Collection<QueryEntity> entrs = conf0.getQueryEntities();
            String cacheName = conf0.getName();
            String cacheSchemaName = normalizeSchemaName(conf0.getName(), conf0.getSqlSchema());

            if (!Objects.equals(cacheSchemaName, schemaName) || CU.isSystemCache(cacheName) ||
                (Objects.equals(cacheSchemaName, schemaName) && Objects.equals(cfg.getName(), cacheName)))
                continue;

            for (QueryEntity ent : entrs) {
                Collection<QueryIndex> idxs = ent.getIndexes();

                for (QueryIndex idx : idxs)
                    idxNamesPerCache.put(idx.getName(), cacheName);
            }
        }

        if (idxNamesPerCache.isEmpty())
            return null;

        Collection<QueryEntity> entrs = cfg.getQueryEntities();

        for (QueryEntity ent : entrs) {
            Collection<QueryIndex> idxs = ent.getIndexes();

            for (QueryIndex idx : idxs) {
                String normalizedIdxName = normalizeObjectName(idx.getName(), false);

                String cacheName = idxNamesPerCache.get(normalizedIdxName);

                if (cacheName != null) {
                    return "Duplicate index name for [cache=" + cfg.getName() +
                        ", idxName=" + idx.getName() + "], an equal index name is already configured for [cache=" +
                        cacheName + ']';
                }
            }
        }

        return null;
    }

    /**
     * Validates cache configuration against stored cache configuration when persistence is enabled.
     *
     * @param cfg Configured cache configuration.
     * @param cfgFromStore Stored cache configuration
     * @throws IgniteCheckedException If validation failed.
     */
    private void validateCacheConfigurationOnRestore(CacheConfiguration cfg, CacheConfiguration cfgFromStore)
        throws IgniteCheckedException {
        assert cfg != null && cfgFromStore != null;

        if ((cfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT ||
            cfgFromStore.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT)
            && cfg.getAtomicityMode() != cfgFromStore.getAtomicityMode()) {
            throw new IgniteCheckedException("Cannot start cache. Statically configured atomicity mode differs from " +
                "previously stored configuration. Please check your configuration: [cacheName=" + cfg.getName() +
                ", configuredAtomicityMode=" + cfg.getAtomicityMode() +
                ", storedAtomicityMode=" + cfgFromStore.getAtomicityMode() + "]");
        }

        boolean staticCfgVal = cfg.isEncryptionEnabled();

        boolean storedVal = cfgFromStore.isEncryptionEnabled();

        if (storedVal != staticCfgVal) {
            throw new IgniteCheckedException("Encrypted flag value differs. Static config value is '" + staticCfgVal +
                "' and value stored on the disk is '" + storedVal + "'");
        }
    }
}
