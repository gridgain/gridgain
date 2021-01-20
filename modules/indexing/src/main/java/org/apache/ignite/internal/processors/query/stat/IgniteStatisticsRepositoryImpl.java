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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.util.typedef.F;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Statistics repository implementation.
 */
public class IgniteStatisticsRepositoryImpl implements IgniteStatisticsRepository {
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
    public IgniteStatisticsRepositoryImpl(
            IgniteStatisticsStore store,
            IgniteStatisticsHelper helper,
            Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.store = store;
        this.locStats = new ConcurrentHashMap<>();
        this.helper = helper;
        this.log = logSupplier.apply(IgniteStatisticsRepositoryImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void saveLocalPartitionsStatistics(
            StatisticsKey key,
            Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        store.replaceLocalPartitionsStatistics(key, statistics);
    }

    /** {@inheritDoc} */
    @Override public Collection<ObjectPartitionStatisticsImpl> mergeLocalPartitionsStatistics(
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

    /** {@inheritDoc} */
    @Override public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatisticsKey key) {
        return store.getLocalPartitionsStatistics(key);
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key, String... colNames) {
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

    /** {@inheritDoc} */
    @Override public void saveLocalPartitionStatistics(StatisticsKey key, ObjectPartitionStatisticsImpl statistics) {
        ObjectPartitionStatisticsImpl oldPartStat = store.getLocalPartitionStatistics(key, statistics.partId());
        if (oldPartStat == null)
            store.saveLocalPartitionStatistics(key, statistics);
        else {
            ObjectPartitionStatisticsImpl combinedStats = add(oldPartStat, statistics);
            store.saveLocalPartitionStatistics(key, combinedStats);
        }
    }

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatisticsKey key, int partId) {
        return store.getLocalPartitionStatistics(key, partId);
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionStatistics(StatisticsKey key, int partId) {
        store.clearLocalPartitionStatistics(key, partId);
    }

    /** {@inheritDoc} */
    @Override public void saveLocalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics) {
        if (locStats == null) {
            log.warning("Unable to save local statistics for " + key + " on non server node.");

            return;
        }

        locStats.put(key, statistics);
    }

    /** {@inheritDoc} */
    @Override public ObjectStatisticsImpl mergeLocalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics) {
        if (locStats == null) {
            log.warning("Unable to merge local statistics for " + key + " on non server node.");

            return null;
        }
        return locStats.compute(key, (k, v) -> (v == null) ? statistics : add(v, statistics));
    }

    /** {@inheritDoc} */
    @Override public void cacheLocalStatistics(StatisticsKey key, Collection<ObjectPartitionStatisticsImpl> statistics) {
        if (locStats == null) {
            log.warning("Unable to cache local statistics for " + key + " on non server node.");

            return;
        }
        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(key.schema(), key.obj(), null);

        locStats.put(key, helper.aggregateLocalStatistics(keyMsg, statistics));
    }

    /** {@inheritDoc} */
    @Override public ObjectStatisticsImpl getLocalStatistics(StatisticsKey key) {
        if (locStats == null)
            return null;

        return locStats.get(key);
    }

    /** {@inheritDoc} */
    @Override public void clearLocalStatistics(StatisticsKey key, String... colNames) {
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

    /** {@inheritDoc} */
    @Override public void saveGlobalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics) {
        globalStats.put(key, statistics);
    }

    /** {@inheritDoc} */
    @Override public ObjectStatisticsImpl mergeGlobalStatistics(StatisticsKey key, ObjectStatisticsImpl statistics) {
        return globalStats.compute(key, (k, v) -> (v == null) ? statistics : add(v, statistics));
    }

    /** {@inheritDoc} */
    @Override public ObjectStatisticsImpl getGlobalStatistics(StatisticsKey key) {
        return globalStats.get(key);
    }

    /** {@inheritDoc} */
    @Override public void clearGlobalStatistics(StatisticsKey key, String... colNames) {
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
}
