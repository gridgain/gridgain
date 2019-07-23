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
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import static java.util.stream.IntStream.range;
import static java.util.stream.Stream.of;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_STATISTICS;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

public class RebalanceStatisticsTest extends GridCommonAbstractTest {

    /** Method rule */
    @Rule public final TestRule methodRule = new SystemPropertiesRule();

    /** Logger for listen messages */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** For remember messages from {@link #log} */
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream(32 * 1024);

    /** For write messages from {@link #log} */
    private final PrintWriter pw = new PrintWriter(baos);

    /** Cache names */
    private static final String[] CACHE_NAMES = {"ch0", "ch1", "ch2", "ch3"};

    /** Node count */
    private static final int NODE_CNT = 4;

    /** Coordinator */
    private IgniteEx crd;

    @Override protected void beforeTest() throws Exception {
        crd = startGrids(NODE_CNT);

        super.beforeTest();
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        baos.reset();
        log.clearListeners();

        super.afterTest();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration[] cacheConfigurations = of(CACHE_NAMES)
            .map(this::cacheConfiguration)
            .toArray(CacheConfiguration[]::new);

        cfg.setCacheConfiguration(cacheConfigurations);
        cfg.setRebalanceThreadPoolSize(5);
        cfg.setGridLogger(log);
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
        ccfg.setBackups(1);
        return ccfg;
    }

    /**
     * Should not write statistics when {@code IGNITE_QUIET} == true.
     *
     * @throws Exception not expected
     * @see IgniteSystemProperties#IGNITE_QUIET
     * */
    @Test
    @WithSystemProperty(key = IGNITE_QUIET, value = "true")
    public void testNotPrintStatWhenIgniteQuite() throws Exception {
        fillCaches(100);

        log.registerListener(pw::write);

        startGrid(NODE_CNT);

        awaitPartitionMapExchange();

        assertNotContains(super.log, baos.toString(), "Total information:");
    }

    /**
     * Should not write statistics when {@code IGNITE_QUIET} == false &&
     * {@code IGNITE_WRITE_REBALANCE_STATISTICS} == false
     *
     * @throws Exception not expected
     * @see IgniteSystemProperties#IGNITE_QUIET
     * @see IgniteSystemProperties#IGNITE_WRITE_REBALANCE_STATISTICS
     * */
    @Test
    @WithSystemProperty(key = IGNITE_QUIET, value = "false")
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_STATISTICS, value = "false")
    public void testNotPrintStatWhenNotIgniteWriteRebalanceStatistics() throws Exception {
        fillCaches(100);

        log.registerListener(pw::write);

        startGrid(NODE_CNT);

        awaitPartitionMapExchange();

        assertNotContains(super.log, baos.toString(), "Total information:");
    }

    /**
     * Should not write statistics when {@code IGNITE_QUIET} == false &&
     * {@code IGNITE_WRITE_REBALANCE_STATISTICS} == false
     *
     * @throws Exception not expected
     * @see IgniteSystemProperties#IGNITE_QUIET
     * @see IgniteSystemProperties#IGNITE_WRITE_REBALANCE_STATISTICS
     * */
    @Test
    @WithSystemProperty(key = IGNITE_QUIET, value = "false")
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_STATISTICS, value = "true")
    public void testPrintStatisticsWithOutPartitionDistribution() throws Exception {
        fillCaches(100);

        log.registerListener(pw::write);

        startGrid(NODE_CNT);

        awaitPartitionMapExchange();

        assertContains(super.log, baos.toString(), "Total information:");
        assertNotContains(super.log, baos.toString(), "Partitions distribution per cache group:");
    }

    /**
     * Should not write statistics when {@code IGNITE_QUIET} == false &&
     * {@code IGNITE_WRITE_REBALANCE_STATISTICS} == false
     *
     * @throws Exception not expected
     * @see IgniteSystemProperties#IGNITE_QUIET
     * @see IgniteSystemProperties#IGNITE_WRITE_REBALANCE_STATISTICS
     * */
    @Test
    @WithSystemProperty(key = IGNITE_QUIET, value = "false")
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_STATISTICS, value = "true")
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, value = "true")
    public void testPrintFullStatistics() throws Exception {
        fillCaches(100);

        log.registerListener(pw::write);

        startGrid(NODE_CNT);

        awaitPartitionMapExchange();

        assertContains(super.log, baos.toString(), "Total information:");
        assertContains(super.log, baos.toString(), "Partitions distribution per cache group:");
    }

    /**
     * Fill all {@link #CACHE_NAMES}.
     *
     * @param cnt - count of additions
     */
    private void fillCaches(final int cnt) {
        for (String cacheName : CACHE_NAMES) {
            IgniteCache<Object, Object> cache = crd.cache(cacheName);

            range(0, cnt).forEach(value -> cache.put(value, cacheName + value));
        }
    }
}
