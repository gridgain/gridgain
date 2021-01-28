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
import java.util.List;
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
    private final Map<StatisticsKey, ObjectStatisticsImpl> locStats;

    /** Statistics gathering. */
    private final IgniteStatisticsHelper helper;

    /** Global (for whole cluster) object statistics. */
    private final Map<StatisticsKey, ObjectStatisticsImpl> globalStats = new ConcurrentHashMap<>();

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
        this.locStats = new ConcurrentHashMap<>();
        this.helper = helper;
        this.log = logSupplier.apply(IgniteStatisticsRepository.class);
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

    /** {@inheritDoc} */
    public Collection<ObjectPartitionStatisticsImpl> mergeLocalPartitionsStatistics(
            StatisticsKey key,
            Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        List<ObjectPartitionStatisticsImpl> res = new ArrayList<>();
        for (ObjectPartitionStatisticsImpl newStat : statistics) {
            ObjectPartitionStatisticsImpl oldStat = store.getLocalPartitionStatistics(key, newStat.partId());
            if (oldStat != null)
                newStat = add(oldStat, newStat);

            res.add(newStat);
            store.saveLocalPartitionStatistics(key, newStat);
        }

        return res;
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
    public void clearLocalPartitionsStatistics(StatisticsKey key, String... colNames) {
        if (F.isEmpty(colNames))
            store.clearLocalPartitionsStatistics(key);
        else {
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

            if (newStatistics.isEmpty())
                store.clearLocalPartitionsStatistics(key);
            else {
                if (!partitionsToRmv.isEmpty())
                    store.clearLocalPartitionsStatistics(key, partitionsToRmv);

                store.replaceLocalPartitionsStatistics(key, newStatistics);
            }
        }
    }

    /**
     * Save specified local partition statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void saveLocalPartitionStatistics(StatisticsKey key, ObjectPartitionStatisticsImpl statistics) {
        ObjectPartitionStatisticsImpl oldPartStat = store.getLocalPartitionStatistics(key, statistics.partId());
        if (oldPartStat == null)
            store.saveLocalPartitionStatistics(key, statistics);
        else {
            ObjectPartitionStatisticsImpl combinedStats = add(oldPartStat, statistics);
            store.saveLocalPartitionStatistics(key, combinedStats);
        }
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
        if (locStats == null) {
            log.warning("Unable to save local statistics for " + key + " on non server node.");

            return;
        }

        locStats.put(key, statistics);
    }

    /**
     * Merge local object statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to merge.
     * @return Merged statistics.
     */
    public ObjectStatisticsImpl mergeLocalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics) {
        if (locStats == null) {
            log.warning("Unable to merge local statistics for " + key + " on non server node.");

            return null;
        }
        return locStats.compute(key, (k, v) -> (v == null) ? statistics : add(v, statistics));
    }

    /**
     * Calculate and cache saved local statistics.
     *
     * @param key Object key.
     * @param statistics Collection of partitions statistics.
     */
    public void cacheLocalStatistics(StatisticsKey key, Collection<ObjectPartitionStatisticsImpl> statistics) {
        if (locStats == null) {
            log.warning("Unable to cache local statistics for " + key + " on non server node.");

            return;
        }
        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(key.schema(), key.obj(), null);

        locStats.put(key, helper.aggregateLocalStatistics(keyMsg, statistics));
    }

    /**
     * Get local statistics.
     *
     * @param key Object key to load statistics by.
     * @return Object local statistics or {@code null} if there are no statistics collected for such object.
     */
    public ObjectStatisticsImpl getLocalStatistics(StatisticsKey key) {
        if (locStats == null)
            return null;

        return locStats.get(key);
    }

    /**
     * Clear local object statistics.
     *
     * @param key Object key to clear local statistics by.
     * @param colNames If specified - only statistics by specified columns will be cleared.
     */
    public void clearLocalStatistics(StatisticsKey key, String... colNames) {
        if (locStats == null) {
            log.warning("Unable to clear local statistics for " + key + " on non server node.");

            return;
        }

        if (F.isEmpty(colNames))
            locStats.remove(key);
        else
            locStats.computeIfPresent(key, (k, v) -> {
                ObjectStatisticsImpl locStatNew = subtract(v, colNames);
                return locStatNew.columnsStatistics().isEmpty() ? null : locStatNew;
            });
    }

    /**
     * Save global statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to save.
     */
    public void saveGlobalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics) {
        globalStats.put(key, statistics);
    }

    /**
     * Merge global statistics.
     *
     * @param key Object key.
     * @param statistics Statistics to merge.
     * @return Merged statistics.
     */
    public ObjectStatisticsImpl mergeGlobalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics) {
        return globalStats.compute(key, (k, v) -> (v == null) ? statistics : add(v, statistics));
    }

    /**
     * Get global statistics by object.
     *
     * @param key To get global statistics by.
     * @return Object statistics of {@code null} if there are no global statistics for specified object.
     */
    public ObjectStatisticsImpl getGlobalStatistics(StatisticsKey key) {
        return globalStats.get(key);
    }

    /**
     * Clear global statistics by object.
     *
     * @param key Object key.
     * @param colNames If specified - only statistics by specified columns will be cleared.
     */
    public void clearGlobalStatistics(StatisticsKey key, String... colNames) {
        if (F.isEmpty(colNames))
            globalStats.remove(key);
        else {
            globalStats.computeIfPresent(key, (k, v) -> {
                ObjectStatisticsImpl newStat = subtract(v, colNames);
                return newStat.columnsStatistics().isEmpty() ? null : newStat;
            });
        }
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
    public static <T extends ObjectStatisticsImpl> T subtract(T base, String[] cols) {
        T res = (T)base.clone();

        for (String col : cols)
            res.columnsStatistics().remove(col);

        return res;
    }

    /** */
    public void refreshAggregatedLocalStatistics(Set<Integer> parts, Set<StatisticsObjectConfiguration> cfgs) {
        log.info("+++ REFRESH");

        for (StatisticsObjectConfiguration objStatCfg : cfgs) {
            Collection<ObjectPartitionStatisticsImpl> stats = store.getLocalPartitionsStatistics(objStatCfg.key());

            Collection<ObjectPartitionStatisticsImpl> statsToAgg = stats.stream()
                .filter(s -> parts.contains(s.partId()))
                .collect(Collectors.toList());

            assert statsToAgg.size() == parts.size() : "Cannot aggregate local statistics: not enough partitioned statistics";

            ObjectStatisticsImpl locStat = helper.aggregateLocalStatistics(
                StatisticsUtils.statisticsObjectConfiguration2Key(objStatCfg),
                statsToAgg
            );

            saveLocalStatistics(objStatCfg.key(), locStat);
        }
    }

    /** */
    public ObjectStatisticsImpl aggregatePartitionedStatistics(Set<Integer> parts, StatisticsObjectConfiguration cfg) {
        Collection<ObjectPartitionStatisticsImpl> stats = store.getLocalPartitionsStatistics(cfg.key());

        Collection<ObjectPartitionStatisticsImpl> statsToAgg = stats.stream()
            .filter(s -> parts.contains(s.partId()))
            .collect(Collectors.toList());

        assert statsToAgg.size() == parts.size() : "Cannot aggregate local statistics: not enough partitioned statistics";

        ObjectStatisticsImpl locStat = helper.aggregateLocalStatistics(
            StatisticsUtils.statisticsObjectConfiguration2Key(cfg),
            statsToAgg
        );

        return locStat;
    }
}
