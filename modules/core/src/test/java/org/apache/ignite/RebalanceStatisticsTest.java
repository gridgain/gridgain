/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
package org.apache.ignite;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.stream.IntStream.range;
import static java.util.stream.Stream.of;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_STATISTICS;

public class RebalanceStatisticsTest extends GridCommonAbstractTest {

    /** Cache names */
    private static final String[] CACHE_NAMES = {"ch0", "ch1", "ch2", "ch3"};

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration[] cacheConfigurations = of(CACHE_NAMES)
            .map(this::cacheConfiguration)
            .toArray(CacheConfiguration[]::new);

        cfg.setCacheConfiguration(cacheConfigurations);
        cfg.setRebalanceThreadPoolSize(5);
        return cfg;
    }

    /**
     * Create {@link CacheConfiguration} by name.
     *
     * @param cacheName cache name
     * @return cache group configuration
     */
    private CacheConfiguration cacheConfiguration(final String cacheName) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 100));
        ccfg.setBackups(3);
        return ccfg;
    }

    /** TODO: javadoc later */
    @Test
    public void testRebalanceStatistics() throws Exception {
        IgniteEx crd = startGrids(3);

        for (String cacheName : CACHE_NAMES) {
            IgniteCache<Object, Object> cache = crd.cache(cacheName);

            range(0, 10_000)
                .forEach(value -> cache.put(value, cacheName + value));
        }

        System.setProperty(IGNITE_QUIET, Boolean.FALSE.toString());
        System.setProperty(IGNITE_WRITE_REBALANCE_STATISTICS, Boolean.TRUE.toString());
        System.setProperty(IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, Boolean.TRUE.toString());

        startGrid(3);

        awaitPartitionMapExchange();
    }
}
