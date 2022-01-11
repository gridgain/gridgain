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
import org.apache.ignite.internal.util.collection.IntMap;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * Statistics store implementation to log unexpected calls. Returns nulls to any request.
 */
public class IgniteStatisticsDummyStoreImpl implements IgniteStatisticsStore {
    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param logSupplier Log supplier.
     */
    public IgniteStatisticsDummyStoreImpl(Function<Class<?>, IgniteLogger> logSupplier) {
        this.log = logSupplier.apply(IgniteStatisticsDummyStoreImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void clearAllStatistics() {
        if (log.isInfoEnabled())
            log.info("Unable to clear all partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public Map<StatisticsKey, Collection<ObjectPartitionStatisticsImpl>> getAllLocalPartitionsStatistics(
        String schema
    ) {
        if (log.isInfoEnabled())
            log.info("Unable to get all partition level statistics on non server node.");

        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void replaceLocalPartitionsStatistics(
        StatisticsKey key,
        Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        if (log.isInfoEnabled())
            log.info("Unable to replace partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatisticsKey key) {
        if (log.isInfoEnabled())
            log.info("Unable to get partition level statistics on non server node.");

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key) {
        if (log.isInfoEnabled())
            log.info("Unable to clear partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatisticsKey key, int partId) {
        if (log.isInfoEnabled())
            log.info("Unable to get partition level statistics on non server node.");

        return null;
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionStatistics(StatisticsKey key, int partId) {
        if (log.isInfoEnabled())
            log.info("Unable to clear partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key, Collection<Integer> partIds) {
        if (log.isInfoEnabled())
            log.info("Unable to clear partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public void saveLocalPartitionStatistics(StatisticsKey key, ObjectPartitionStatisticsImpl statistics) {
        if (log.isInfoEnabled())
            log.info("Unable to save partition level statistics on non server node.");
    }

    /** {@inheritDoc} */
    @Override public void saveObsolescenceInfo(
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> obsolescence
    ) {
        if (log.isInfoEnabled())
            log.info("Unable to save statistics obsolescence info on non server node.");
    }

    /** {@inheritDoc} */
    @Override public void saveObsolescenceInfo(
        StatisticsKey key,
        int partId,
        ObjectPartitionStatisticsObsolescence partObs
    ) {
        if (log.isInfoEnabled())
            log.info("Unable to save statistics obsolescence info on non server node.");
    }

    /** {@inheritDoc} */
    @Override public void clearObsolescenceInfo(StatisticsKey key, Collection<Integer> partIds) {
        if (log.isInfoEnabled())
            log.info("Unable to clear statistics obsolescence info on non server node.");
    }

    /** {@inheritDoc} */
    @Override public Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> loadAllObsolescence() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> loadLocalPartitionMap(StatisticsKey key) {
        return Collections.emptySet();
    }
}
