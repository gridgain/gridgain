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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
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
        if (locStats == null) {
            log.warning("Unable to save local statistics for " + key + " on non server node.");

            return;
        }

        locStats.put(key, statistics);
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

    /** */
    public ObjectStatisticsImpl refreshAggregatedLocalStatistics(
        Set<Integer> parts,
        StatisticsObjectConfiguration objStatCfg
    ) {
        if (log.isDebugEnabled()) {
            log.debug("Refresh local aggregated statistic [cfg=" + objStatCfg +
                ", part=" + parts + ']');
        }

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

        return locStat;
    }
}
