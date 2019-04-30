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

package org.apache.ignite.console.repositories;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Cache;
import org.apache.ignite.console.dto.Cluster;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.dto.Igfs;
import org.apache.ignite.console.dto.Model;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;
import org.springframework.stereotype.Repository;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.console.common.Utils.diff;
import static org.apache.ignite.console.common.Utils.idsFromJson;
import static org.apache.ignite.console.common.Utils.toJsonArray;
import static org.apache.ignite.console.json.JsonUtils.asJson;
import static org.apache.ignite.console.json.JsonUtils.fromJson;
import static org.apache.ignite.console.json.JsonUtils.toJson;

/**
 * Repository to work with configurations.
 */
@Repository
public class ConfigurationsRepository {
    /** */
    protected final TransactionManager txMgr;

    /** */
    private final Table<Cluster> clustersTbl;

    /** */
    private final Table<Cache> cachesTbl;

    /** */
    private final Table<Model> modelsTbl;

    /** */
    private final Table<Igfs> igfssTbl;

    /** */
    protected final OneToManyIndex clustersIdx;

    /** */
    private final OneToManyIndex cachesIdx;

    /** */
    private final OneToManyIndex modelsIdx;

    /** */
    private final OneToManyIndex igfssIdx;

    /** */
    private final OneToManyIndex configIdx;

    /**
     * @param ignite Ignite.
     * @param txMgr Transactions manager.
     */
    public ConfigurationsRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

        clustersTbl = new Table<Cluster>(ignite, "wc_account_clusters")
            .addUniqueIndex(Cluster::name, (cluster) -> "Cluster '" + cluster + "' already exits");

        cachesTbl = new Table<>(ignite, "wc_cluster_caches");
        modelsTbl = new Table<>(ignite, "wc_cluster_models");
        igfssTbl = new Table<>(ignite, "wc_cluster_igfss");

        clustersIdx = new OneToManyIndex(ignite, "wc_account_clusters_idx");
        cachesIdx = new OneToManyIndex(ignite, "wc_cluster_caches_idx");
        modelsIdx = new OneToManyIndex(ignite, "wc_cluster_models_idx");
        igfssIdx = new OneToManyIndex(ignite, "wc_cluster_igfss_idx");

        configIdx = new OneToManyIndex(ignite, "wc_account_configs_idx");
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Configuration in JSON format.
     */
    public JsonObject loadConfiguration(UUID accId, UUID clusterId) {
        try (Transaction ignored = txMgr.txStart()) {
            Cluster cluster = clustersTbl.load(clusterId);

            if (cluster == null)
                throw new IllegalStateException("Cluster not found for ID: " + clusterId);

            clustersIdx.validate(accId, cluster.getId());

            Collection<Cache> caches = cachesTbl.loadAll(cachesIdx.load(clusterId));
            Collection<Model> models = modelsTbl.loadAll(modelsIdx.load(clusterId));
            Collection<Igfs> igfss = igfssTbl.loadAll(igfssIdx.load(clusterId));

            return new JsonObject()
                .add("cluster", fromJson(cluster.json()))
                .add("caches", toJsonArray(caches))
                .add("models", toJsonArray(models))
                .add("igfss", toJsonArray(igfss));
        }
    }

    /**
     * @param cluster Cluster DTO.
     * @return Short view of cluster DTO as JSON object.
     */
    protected JsonObject shortCluster(Cluster cluster) {
        UUID clusterId = cluster.getId();

        int cachesCnt = cachesIdx.load(clusterId).size();
        int modelsCnt = modelsIdx.load(clusterId).size();
        int igfsCnt = igfssIdx.load(clusterId).size();

        return new JsonObject()
            .add("id", cluster.getId())
            .add("name", cluster.name())
            .add("discovery", cluster.discovery())
            .add("cachesCount", cachesCnt)
            .add("modelsCount", modelsCnt)
            .add("igfsCount", igfsCnt);
    }

    /**
     * @param accId Account ID.
     * @return List of user clusters.
     */
    public JsonArray loadClusters(UUID accId) {
        try (Transaction ignored = txMgr.txStart()) {
            TreeSet<UUID> clusterIds = clustersIdx.load(accId);

            Collection<Cluster> clusters = clustersTbl.loadAll(clusterIds);

            JsonArray shortList = new JsonArray();

            clusters.forEach(cluster -> shortList.add(shortCluster(cluster)));

            return shortList;
        }
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Cluster.
     */
    public Cluster loadCluster(UUID accId, UUID clusterId) {
        try (Transaction ignored = txMgr.txStart()) {
            clustersIdx.validate(accId, clusterId);

            Cluster cluster = clustersTbl.load(clusterId);

            if (cluster == null)
                throw new IllegalStateException("Cluster not found for ID: " + clusterId);

            return cluster;
        }
    }

    /**
     * @param accId Account ID.
     * @param cacheId Cache ID.
     * @return Cache.
     */
    public Cache loadCache(UUID accId, UUID cacheId) {
        try (Transaction ignored = txMgr.txStart()) {
            configIdx.validate(accId, cacheId);

            Cache cache = cachesTbl.load(cacheId);

            if (cache == null)
                throw new IllegalStateException("Cache not found for ID: " + cacheId);

            return cache;
        }
    }

    /**
     * @param accId Account ID.
     * @param mdlId Model ID.
     * @return Model.
     */
    public Model loadModel(UUID accId, UUID mdlId) {
        try (Transaction ignored = txMgr.txStart()) {
            configIdx.validate(accId, mdlId);

            Model mdl = modelsTbl.load(mdlId);

            if (mdl == null)
                throw new IllegalStateException("Model not found for ID: " + mdlId);

            return mdl;
        }
    }

    /**
     * @param accId Account ID.
     * @param igfsId IGFS ID.
     * @return IGFS.
     */
    public Igfs loadIgfs(UUID accId, UUID igfsId) {
        try (Transaction ignored = txMgr.txStart()) {
            configIdx.validate(accId, igfsId);

            Igfs igfs = igfssTbl.load(igfsId);

            if (igfs == null)
                throw new IllegalStateException("IGFS not found for ID: " + igfsId);

            return igfs;
        }
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Collection of cluster caches.
     */
    public Collection<Cache> loadCaches(UUID accId, UUID clusterId) {
        try (Transaction ignored = txMgr.txStart()) {
            TreeSet<UUID> cachesIds = cachesIdx.load(clusterId);

            configIdx.validateAll(accId, cachesIds);

            return cachesTbl.loadAll(cachesIds);
        }
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Collection of cluster models.
     */
    public Collection<Model> loadModels(UUID accId, UUID clusterId) {
        try (Transaction ignored = txMgr.txStart()) {
            TreeSet<UUID> modelsIds = modelsIdx.load(clusterId);

            configIdx.validateAll(accId, modelsIds);

            return modelsTbl.loadAll(modelsIds);
        }
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Collection of cluster IGFSs.
     */
    public Collection<Igfs> loadIgfss(UUID accId, UUID clusterId) {
        try (Transaction ignored = txMgr.txStart()) {
            configIdx.validate(accId, clusterId);

            TreeSet<UUID> igfsIds = igfssIdx.load(clusterId);

            configIdx.validateAll(accId, igfsIds);

            return igfssTbl.loadAll(igfsIds);
        }
    }

    /**
     * Handle objects that was deleted from cluster.
     *
     * @param accId Account ID.
     * @param tbl Table with DTOs.
     * @param idx Foreign key.
     * @param clusterId Cluster ID.
     * @param oldCluster Old cluster JSON.
     * @param newCluster New cluster JSON.
     * @param fld Field name that holds IDs to check for deletion.
     */
    private void removedInCluster(
        UUID accId,
        Table<? extends DataObject> tbl,
        OneToManyIndex idx,
        UUID clusterId,
        JsonObject oldCluster,
        JsonObject newCluster,
        String fld
    ) {
        TreeSet<UUID> oldIds = idsFromJson(oldCluster, fld);
        TreeSet<UUID> newIds = idsFromJson(newCluster, fld);

        TreeSet<UUID> deletedIds = diff(oldIds, newIds);

        if (!F.isEmpty(deletedIds)) {
            configIdx.validateAll(accId, deletedIds);

            tbl.deleteAll(deletedIds);
            idx.removeAll(clusterId, deletedIds);
        }
    }

    /**
     * @param accId Account ID.
     * @param changedItems Items to save.
     * @return Saved cluster.
     */
    private Cluster saveCluster(UUID accId, JsonObject changedItems) {
        JsonObject jsonCluster = changedItems.getJsonObject("cluster");

        Cluster newCluster = Cluster.fromJson(jsonCluster);

        UUID clusterId = newCluster.getId();

        clustersIdx.validateSave(accId, clusterId, clustersTbl);

        Cluster oldCluster = clustersTbl.load(clusterId);

        if (oldCluster != null) {
            JsonObject oldClusterJson = fromJson(oldCluster.json());

            removedInCluster(accId, cachesTbl, cachesIdx, clusterId, oldClusterJson, jsonCluster, "caches");
            removedInCluster(accId, modelsTbl, modelsIdx, clusterId, oldClusterJson, jsonCluster, "models");
            removedInCluster(accId, igfssTbl, igfssIdx, clusterId, oldClusterJson, jsonCluster, "igfss");
        }

        clustersIdx.add(accId, clusterId);

        clustersTbl.save(newCluster);

        return newCluster;
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     * @param basic {@code true} in case of saving basic cluster.
     */
    private void saveCaches(UUID accId, Cluster cluster, JsonObject json, boolean basic) {
        JsonArray jsonCaches = json.getJsonArray("caches");

        if (F.isEmpty(jsonCaches))
            return;

        Map<UUID, Cache> caches = jsonCaches
            .stream()
            .map(item -> Cache.fromJson(asJson(item)))
            .collect(toMap(Cache::getId, c -> c));

        Set<UUID> cacheIds = caches.keySet();

        configIdx.validateSaveAll(accId, cacheIds, cachesTbl);

        if (basic) {
            Collection<Cache> oldCaches = cachesTbl.loadAll(new TreeSet<>(cacheIds));

            oldCaches.forEach(oldCache -> {
                Cache newCache = caches.get(oldCache.getId());

                if (newCache != null) {
                    JsonObject oldJson = fromJson(oldCache.json());
                    JsonObject newJson = fromJson(newCache.json());

                    newCache.json(toJson(oldJson.mergeIn(newJson)));
                }
            });
        }

        configIdx.addAll(accId, cacheIds);

        cachesIdx.addAll(cluster.getId(), cacheIds);

        cachesTbl.saveAll(caches);
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     */
    private void saveModels(UUID accId, Cluster cluster, JsonObject json) {
        JsonArray jsonModels = json.getJsonArray("models");

        if (F.isEmpty(jsonModels))
            return;

        Map<UUID, Model> mdls = jsonModels
            .stream()
            .map(item -> Model.fromJson(asJson(item)))
            .collect(toMap(Model::getId, m -> m));

        Set<UUID> mdlIds = mdls.keySet();

        configIdx.validateSaveAll(accId, mdlIds, modelsTbl);

        configIdx.addAll(accId, mdlIds);

        modelsIdx.addAll(cluster.getId(), mdlIds);

        modelsTbl.saveAll(mdls);
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     */
    private void saveIgfss(UUID accId, Cluster cluster, JsonObject json) {
        JsonArray jsonIgfss = json.getJsonArray("igfss");

        if (F.isEmpty(jsonIgfss))
            return;

        Map<UUID, Igfs> igfss = jsonIgfss
            .stream()
            .map(item -> Igfs.fromJson(asJson(item)))
            .collect(toMap(Igfs::getId, i -> i));

        Set<UUID> igfsIds = igfss.keySet();

        configIdx.validateSaveAll(accId, igfsIds, igfssTbl);

        configIdx.addAll(accId, igfsIds);

        igfssIdx.addAll(cluster.getId(), igfsIds);

        igfssTbl.saveAll(igfss);
    }

    /**
     * Save full cluster.
     *
     * @param accId Account ID.
     * @param json Configuration in JSON format.
     */
    public void saveAdvancedCluster(UUID accId, JsonObject json) {
        try (Transaction tx = txMgr.txStart()) {
            Cluster cluster = saveCluster(accId, json);

            saveCaches(accId, cluster, json, false);
            saveModels(accId, cluster, json);
            saveIgfss(accId, cluster, json);

            tx.commit();
        }
    }

    /**
     * Save basic cluster.
     *
     * @param accId Account ID.
     * @param json Configuration in JSON format.
     */
    public void saveBasicCluster(UUID accId, JsonObject json) {
        try (Transaction tx = txMgr.txStart()) {
            Cluster cluster = saveCluster(accId, json);

            saveCaches(accId, cluster, json, true);

            tx.commit();
        }
    }

    /**
     * Delete objects that relates to cluster.
     *
     * @param clusterId Cluster ID.
     * @param fkIdx Foreign key.
     * @param tbl Table with children.
     */
    private void deleteClusterObjects(UUID accId, UUID clusterId, OneToManyIndex fkIdx, Table<? extends DataObject> tbl) {
        TreeSet<UUID> ids = fkIdx.delete(clusterId);

        configIdx.validateAll(accId, ids);
        configIdx.removeAll(accId, ids);

        tbl.deleteAll(ids);
    }

    /**
     * Delete all objects that relates to cluster.
     *
     * @param clusterId Cluster ID.
     */
    protected void deleteAllClusterObjects(UUID accId, UUID clusterId) {
        deleteClusterObjects(accId, clusterId, cachesIdx, cachesTbl);
        deleteClusterObjects(accId, clusterId, modelsIdx, modelsTbl);
        deleteClusterObjects(accId, clusterId, igfssIdx, igfssTbl);
    }

    /**
     * Delete clusters.
     *
     * @param accId Account ID.
     * @param clusterIds Cluster IDs to delete.
     */
    public void deleteClusters(UUID accId, TreeSet<UUID> clusterIds) {
        try (Transaction tx = txMgr.txStart()) {
            configIdx.validateAll(accId, clusterIds);

            clusterIds.forEach(clusterId -> deleteAllClusterObjects(accId, clusterId));

            clustersTbl.deleteAll(clusterIds);
            clustersIdx.removeAll(accId, clusterIds);

            tx.commit();
        }
    }

    /**
     * Delete all configurations for specified account.
     *
     * @param accId Account ID.
     */
    public void deleteByAccountId(UUID accId) {
        try (Transaction tx = txMgr.txStart()) {
            TreeSet<UUID> clusterIds = clustersIdx.delete(accId);

            clusterIds.forEach(clusterId -> deleteAllClusterObjects(accId, clusterId));

            clustersTbl.deleteAll(clusterIds);

            tx.commit();
        }
    }
}
