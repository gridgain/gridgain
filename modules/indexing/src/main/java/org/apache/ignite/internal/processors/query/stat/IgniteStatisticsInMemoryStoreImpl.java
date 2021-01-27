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
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Sql statistics storage in metastore.
 * Will store all statistics related objects with prefix "stats."
 * Store only partition level statistics.
 */
public class IgniteStatisticsInMemoryStoreImpl implements IgniteStatisticsStore {
    /** Table -> Partition -> Partition Statistics map, populated only on server nodes without persistence enabled. */
    private final Map<StatisticsKey, IntMap<ObjectPartitionStatisticsImpl>> partsStats = new ConcurrentHashMap<>();

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param logSupplier Logger getting function.
     */
    public IgniteStatisticsInMemoryStoreImpl(Function<Class<?>, IgniteLogger> logSupplier) {
        this.log = logSupplier.apply(IgniteStatisticsInMemoryStoreImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void clearAllStatistics() {
        partsStats.clear();
    }

    /** {@inheritDoc} */
    @Override public void replaceLocalPartitionsStatistics(
        StatisticsKey key,
        Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        partsStats.put(key, buildStatisticsMap(key, statistics));
    }

    /** {@inheritDoc} */
    @Override public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatisticsKey key) {
        Collection<ObjectPartitionStatisticsImpl>[] res = new Collection[1];
        partsStats.computeIfPresent(key, (k, v) -> {
            // Need to make a copy under the lock.
            res[0] = Arrays.asList(v.values());

            return v;
        });

        return (res[0] == null) ? Collections.emptyList() : res[0];
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key) {
        partsStats.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void saveLocalPartitionStatistics(StatisticsKey key, ObjectPartitionStatisticsImpl statistics) {
        partsStats.compute(key, (k, v) -> {
            // Need to change the map under the lock.
            if (v == null)
                v = new IntHashMap<>();

            v.put(statistics.partId(), statistics);

            return v;
        });
    }

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatisticsKey key, int partId) {
        ObjectPartitionStatisticsImpl[] res = new ObjectPartitionStatisticsImpl[1];
        partsStats.computeIfPresent(key, (k, v) -> {
            // Need to access the map under the lock.
            res[0] = v.get(partId);

            return v;
        });
        return res[0];
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionStatistics(StatisticsKey key, int partId) {
        partsStats.computeIfPresent(key, (k,v) -> {
            v.remove(partId);

            return v;
        });
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key, Collection<Integer> partIds) {
        partsStats.computeIfPresent(key, (k,v) -> {
            for (Integer partId : partIds)
                v.remove(partId);

            return v;
        });
    }

    /**
     * Convert collection of partition level statistics into map(partId->partStatistics).
     *
     * @param key Object key.
     * @param statistics Collection of tables partition statistics.
     * @return Partition id to statistics map.
     */
    private IntMap<ObjectPartitionStatisticsImpl> buildStatisticsMap(
        StatisticsKey key,
        Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        IntMap<ObjectPartitionStatisticsImpl> statisticsMap = new IntHashMap<ObjectPartitionStatisticsImpl>();
        for (ObjectPartitionStatisticsImpl s : statistics) {
            if (statisticsMap.put(s.partId(), s) != null)
                log.warning(String.format("Trying to save more than one %s.%s partition statistics for partition %d",
                        key.schema(), key.obj(), s.partId()));
        }
        return statisticsMap;
    }
}
