/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
package org.apache.ignite.internal.processors.query.stat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Statistics repository implementation.
 */
public class IgniteStatisticsRepository {
    /** Logger. */
    private final IgniteLogger log;

    /** Statistics store. */
    private final IgniteStatisticsStore store;

    /** Local (for current node) object statistics. */
    private final Map<StatisticsKey, ObjectStatisticsImpl> locStats = new ConcurrentHashMap<>();

    /** Obsolescence for each partition. */
    private final Map<StatisticsKey, Map<Integer, ObjectPartitionStatisticsObsolescence>> statObs =
        new ConcurrentHashMap<>();

    /** Statistics gathering. */
    private final IgniteStatisticsHelper helper;

    /**
     * Constructor.
     *
     * @param store Ignite statistics store to use.
     * @param helper IgniteStatisticsHelper.
     * @param logSupplier Ignite logger supplier to get logger from.
     */
    public IgniteStatisticsRepository(
            IgniteStatisticsStore store,
            IgniteStatisticsHelper helper,
            Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.store = store;
        this.helper = helper;
        this.log = logSupplier.apply(IgniteStatisticsRepository.class);
    }

    /**
     * Get local partition statistics by specified object.
     *
     * @param key Object to get statistics by.
     * @return Collection of partitions statistics.
     */
    public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatisticsKey key) {
        return store.getLocalPartitionsStatistics(key);
    }

    /**
     * Clear partition statistics for specified object.
     *
     * @param key Object to clear statistics by.
     * @param colNames if specified - only statistics by specified columns will be cleared.
     */
    public void clearLocalPartitionsStatistics(StatisticsKey key, Set<String> colNames) {
        if (F.isEmpty(colNames)) {
            store.clearLocalPartitionsStatistics(key);

            return;
        }

        Collection<ObjectPartitionStatisticsImpl> oldStatistics = store.getLocalPartitionsStatistics(key);

        if (oldStatistics.isEmpty())
            return;

        Collection<ObjectPartitionStatisticsImpl> newStatistics = new ArrayList<>(oldStatistics.size());
        Collection<Integer> partitionsToRmv = new ArrayList<>();

        for (ObjectPartitionStatisticsImpl oldStat : oldStatistics) {
            ObjectPartitionStatisticsImpl newStat = subtract(oldStat, colNames);

            if (!newStat.columnsStatistics().isEmpty())
                newStatistics.add(newStat);
            else
                partitionsToRmv.add(oldStat.partId());
        }

        if (newStatistics.isEmpty()) {
            store.clearLocalPartitionsStatistics(key);

            return;
        }

        if (!partitionsToRmv.isEmpty())
            store.clearLocalPartitionsStatistics(key, partitionsToRmv);

        store.replaceLocalPartitionsStatistics(key, newStatistics);
    }

    /**
     * Save specified local partition statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void saveLocalPartitionStatistics(
        StatisticsKey key,
        ObjectPartitionStatisticsImpl statistics
    ) {
        ObjectPartitionStatisticsImpl oldPartStat = store.getLocalPartitionStatistics(key, statistics.partId());
        if (oldPartStat == null)
            store.saveLocalPartitionStatistics(key, statistics);
        else {
            ObjectPartitionStatisticsImpl combinedStats = add(oldPartStat, statistics);
            store.saveLocalPartitionStatistics(key, combinedStats);
        }
    }

    /**
     * Save specified local partition statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void replaceLocalPartitionStatistics(
        StatisticsKey key,
        ObjectPartitionStatisticsImpl statistics
    ) {
        store.saveLocalPartitionStatistics(key, statistics);
    }

    /**
     * Replace all object statistics with specified ones.
     *
     * @param key Object key.
     * @param statistics Collection of tables partition statistics.
     */
    public void saveLocalPartitionsStatistics(
        StatisticsKey key,
        Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        store.replaceLocalPartitionsStatistics(key, statistics);
    }

    /**
     * Get partition statistics.
     *
     * @param key Object key.
     * @param partId Partition id.
     * @return Object partition statistics or {@code null} if there are no statistics collected for such partition.
     */
    public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatisticsKey key, int partId) {
        return store.getLocalPartitionStatistics(key, partId);
    }

    /**
     * Clear partition statistics.
     *
     * @param key Object key.
     * @param partId Partition id.
     */
    public void clearLocalPartitionStatistics(StatisticsKey key, int partId) {
        store.clearLocalPartitionStatistics(key, partId);
    }

    /**
     * Save local object statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void saveLocalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics) {
        locStats.put(key, statistics);
    }

    /**
     * Get local statistics.
     *
     * @param key Object key to load statistics by.
     * @return Object local statistics or {@code null} if there are no statistics collected for such object.
     */
    public ObjectStatisticsImpl getLocalStatistics(StatisticsKey key) {
        return locStats.get(key);
    }

    /**
     * Clear local object statistics.
     *
     * @param key Object key to clear local statistics by.
     */
    public void clearLocalStatistics(StatisticsKey key) {
        locStats.remove(key);
    }

    /**
     * Clear local object statistics.
     *
     * @param key Object key to clear local statistics by.
     * @param colNames Only statistics by specified columns will be cleared.
     */
    public void clearLocalStatistics(StatisticsKey key, Set<String> colNames) {
        if (log.isDebugEnabled()) {
            log.debug("Clear local statistics [key=" + key +
                ", columns=" + colNames + ']');
        }

        if (locStats == null) {
            log.warning("Unable to clear local statistics for " + key + " on non server node.");

            return;
        }

        locStats.computeIfPresent(key, (k, v) -> {
            ObjectStatisticsImpl locStatNew = subtract(v, colNames);
            return locStatNew.columnsStatistics().isEmpty() ? null : locStatNew;
        });
    }

    /**
     * @return Ignite statistics store.
     */
    public IgniteStatisticsStore statisticsStore() {
        return store;
    }

    /**
     * Add new statistics into base one (with overlapping of existing data).
     *
     * @param base Old statistics.
     * @param add Updated statistics.
     * @param <T> Statistics type (partition or object one).
     * @return Combined statistics.
     */
    public static <T extends ObjectStatisticsImpl> T add(T base, T add) {
        T res = (T)add.clone();
        for (Map.Entry<String, ColumnStatistics> entry : base.columnsStatistics().entrySet())
            res.columnsStatistics().putIfAbsent(entry.getKey(), entry.getValue());

        return res;
    }

    /**
     * Remove specified columns from clone of base ObjectStatistics object.
     *
     * @param base ObjectStatistics to remove columns from.
     * @param cols Columns to remove.
     * @return Cloned object without specified columns statistics.
     */
    public static <T extends ObjectStatisticsImpl> T subtract(T base, Set<String> cols) {
        T res = (T)base.clone();

        for (String col : cols)
            res.columnsStatistics().remove(col);

        return res;
    }

    /**
     * Scan local partitioned statistic and aggregate local statistic for specified statistic object.
     * @param parts Partitions numbers to aggregate,
     * @param cfg Statistic configuration to specify statistic object to aggregate.
     * @return aggregated local statistic.
     */
    public ObjectStatisticsImpl aggregatedLocalStatistics(
        Set<Integer> parts,
        StatisticsObjectConfiguration cfg
    ) {
        if (log.isDebugEnabled()) {
            log.debug("Refresh local aggregated statistic [cfg=" + cfg +
                ", part=" + parts + ']');
        }

        Collection<ObjectPartitionStatisticsImpl> stats = store.getLocalPartitionsStatistics(cfg.key());

        Collection<ObjectPartitionStatisticsImpl> statsToAgg = stats.stream()
            .filter(s -> parts.contains(s.partId()))
            .collect(Collectors.toList());

        assert statsToAgg.size() == parts.size() : "Cannot aggregate local statistics: not enough partitioned statistics";

        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(
            cfg.key().schema(),
            cfg.key().obj(),
            new ArrayList<>(cfg.columns().keySet())
        );

        ObjectStatisticsImpl locStat = helper.aggregateLocalStatistics(
            keyMsg,
            statsToAgg
        );

        saveLocalStatistics(cfg.key(), locStat);

        return locStat;
    }

    /**
     * Stop repository.
     */
    public void stop() {
        locStats.clear();

        if (log.isDebugEnabled())
            log.debug("Statistics repository started.");
    }

    /**
     * Start repository.
     */
    public void start() {
        if (log.isDebugEnabled())
            log.debug("Statistics repository started.");
    }

    /**
     * Load obsolescence info from local metastorage and cache it. Remove parts, that doesn't match configuration.
     * Create missing partitions info.
     *
     * @param cfg Partitions configuration.
     */
    public void loadObsolescenceInfo(Map<StatisticsObjectConfiguration, Set<Integer>> cfg) {
        Map<StatisticsKey, Map<Integer, ObjectPartitionStatisticsObsolescence>> obsolescence = store.loadAllObsolescence();

        obsolescence.forEach((k,v) -> statObs.put(k, new ConcurrentHashMap<>(v)));

        updateObsolescenceInfo(cfg);
    }

    /**
     * Update obsolescence info cache to fit specified cfg.
     *
     * @param cfg Obsolescence configuration.
     */
    public void updateObsolescenceInfo(Map<StatisticsObjectConfiguration, Set<Integer>> cfg) {
        Map<StatisticsKey, Set<Integer>> deleted = updateObsolescenceInfo(statObs, cfg);

        for (Map.Entry<StatisticsKey, Set<Integer>> objDeleted : deleted.entrySet())
            store.removeObsolescenceInfo(objDeleted.getKey(), objDeleted.getValue());
    }

    /**
     * Load or update obsolescence info cache to fit specified cfg.
     *
     * @param cfg Map object statistics configuration to primary partitions set.
     */
    public void checkObsolescenceInfo(Map<StatisticsObjectConfiguration, Set<Integer>> cfg) {

    }

    /**
     * Make obsolescence map correlated with configuration and return removed elements map.
     *
     * @param obsolescence Obsolescence info map.
     * @param cfg Obsolescence configuration.
     * @return Removed from obsolescence info map partitions.
     */
    private static Map<StatisticsKey, Set<Integer>> updateObsolescenceInfo(
        Map<StatisticsKey, Map<Integer, ObjectPartitionStatisticsObsolescence>> obsolescence,
        Map<StatisticsObjectConfiguration, Set<Integer>> cfg
    ) {
        Map<StatisticsKey, Set<Integer>> res = new HashMap<>();

        Set<StatisticsKey> keys = cfg.keySet().stream().map(StatisticsObjectConfiguration::key).collect(Collectors.toSet());

        for (Map.Entry<StatisticsKey, Map<Integer, ObjectPartitionStatisticsObsolescence>> objObs :
            obsolescence.entrySet()) {
            StatisticsKey key = objObs.getKey();
            if (!keys.contains(key)) {
                Map<Integer, ObjectPartitionStatisticsObsolescence> rmv = obsolescence.remove(key);
                res.put(key, rmv.keySet());
            }
        }

        for (Map.Entry<StatisticsObjectConfiguration, Set<Integer>> objObsCfg : cfg.entrySet()) {
            StatisticsKey key = objObsCfg.getKey().key();
            Map<Integer, ObjectPartitionStatisticsObsolescence> objObs = obsolescence.get(objObsCfg.getKey().key());
            if (objObs == null) {
                objObs = new ConcurrentHashMap<>();
                obsolescence.put(key, objObs);
            }
            Map<Integer, ObjectPartitionStatisticsObsolescence> objObsFinal = objObs;

            // TODO:
            long cfgVer = 1;//objObsCfg.getKey().version();
            Set<Integer> partIds = objObsCfg.getValue();

            for (Map.Entry<Integer, ObjectPartitionStatisticsObsolescence> objPartObs : objObs.entrySet()) {
                Integer partId = objPartObs.getKey();
                if (partIds.contains(partId)) {
                    if (cfgVer != objPartObs.getValue().ver())
                        objObs.put(partId, new ObjectPartitionStatisticsObsolescence(cfgVer));
                }
                else {
                    objObs.remove(partId);
                    res.computeIfAbsent(key, k -> new HashSet<>()).add(partId);
                }
            }
        }

        return res;
    }

    /**
     * Try to count modified to specified object and partition.
     *
     * @param key Statistics key.
     * @param partId Partition id.
     * @param changedKey Changed key bytes.
     */
    public void addRowsModified(StatisticsKey key, int partId, byte[] changedKey) {
        Map<Integer, ObjectPartitionStatisticsObsolescence> objObs = statObs.get(key);
        if (objObs == null)
            return;

        ObjectPartitionStatisticsObsolescence objPartObs = objObs.get(partId);
        if (objPartObs == null)
            return;

        objPartObs.modify(changedKey);
    }

    /**
     * Save all modified obsolescence info to local metastorage.
     *
     * @return Map of modified partitions.
     */
    public Map<StatisticsKey, Map<Integer, ObjectPartitionStatisticsObsolescence>> saveObsolescenceInfo() {
        Map<StatisticsKey, Map<Integer, ObjectPartitionStatisticsObsolescence>> dirtyObs = new HashMap<>();
        for (Map.Entry<StatisticsKey, Map<Integer, ObjectPartitionStatisticsObsolescence>> objObs :
            statObs.entrySet()) {
            Map<Integer, ObjectPartitionStatisticsObsolescence> objDirtyObs = new HashMap<>();

            for (Map.Entry<Integer, ObjectPartitionStatisticsObsolescence> objPartObs : objObs.getValue().entrySet()) {
                if (objPartObs.getValue().dirty()) {
                    ObjectPartitionStatisticsObsolescence obs = objPartObs.getValue();
                    obs.dirty(false);
                    objDirtyObs.put(objPartObs.getKey(), obs);
                }
            }

            if (!objDirtyObs.isEmpty())
                dirtyObs.put(objObs.getKey(), objDirtyObs);
        }

        store.saveObsolescenceInfo(dirtyObs);

        return dirtyObs;
    }

    /**
     * Remove statistics obsolescence info by the given key.
     *
     * @param key Statistics key to remove obsolescence info by.
     */
    public void removeObsolescenceInfo(StatisticsKey key) {
        statObs.remove(key);
    }
}
