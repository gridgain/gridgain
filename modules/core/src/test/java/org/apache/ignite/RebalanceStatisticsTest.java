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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import static java.lang.Integer.parseInt;
import static java.util.Objects.nonNull;
import static java.util.regex.Pattern.compile;
import static java.util.stream.IntStream.range;
import static java.util.stream.Stream.of;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_STATISTICS;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

@WithSystemProperty(key = IGNITE_QUIET, value = "false")
@WithSystemProperty(key = IGNITE_WRITE_REBALANCE_STATISTICS, value = "true")
@WithSystemProperty(key = IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, value = "true")
public class RebalanceStatisticsTest extends GridCommonAbstractTest {

    /** Class rule */
    @ClassRule public static final TestRule classRule = new SystemPropertiesRule();

    /** Cache names */
    private static final String[] DEFAULT_CACHE_NAMES = {"ch0", "ch1", "ch2", "ch3"};

    /** Total information text */
    private static final String TOTAL_INFORMATION_TEXT = "Total information:";

    /** Partitions distribution text */
    private static final String PARTITIONS_DISTRIBUTION_TEXT = "Partitions distribution per cache group:";

    /** Topic statistics text */
    public static final String TOPIC_STATISTICS_TEXT = "Topic statistics:";

    /** Supplier statistics text */
    public static final String SUPPLIER_STATISTICS_TEXT = "Supplier statistics:";

    /** Information per cache group text */
    public static final String INFORMATION_PER_CACHE_GROUP_TEXT = "Information per cache group:";

    /** Name attribute */
    public static final String NAME_ATTRIBUTE = "name";

    /** Node count */
    private static final int DEFAULT_NODE_CNT = 4;

    /** Logger for listen messages */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** For remember messages from {@link #log} */
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream(32 * 1024);

    /** For write messages from {@link #log} */
    private final PrintWriter pw = new PrintWriter(baos);

    /** Caches configuration */
    private CacheConfiguration[] cacheCfgs;

    /** Coordinator */
    private IgniteEx crd;

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        baos.reset();
        log.clearListeners();

        super.afterTest();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setCacheConfiguration(cacheCfgs);
        cfg.setRebalanceThreadPoolSize(5);
        cfg.setGridLogger(log);
        return cfg;
    }

    /**
     * Create {@link CacheConfiguration}.
     *
     * @param cacheName cache name
     * @param parts count of partitions
     * @param backups count backup
     * @return cache group configuration
     */
    private CacheConfiguration cacheConfiguration(final String cacheName, final int parts, final int backups) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, parts));
        ccfg.setBackups(backups);
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
        cacheCfgs = defaultCacheConfigurations();

        crd = startGrids(DEFAULT_NODE_CNT);

        fillCaches(100);

        log.registerListener(pw::write);

        startGrid(DEFAULT_NODE_CNT);

        awaitPartitionMapExchange();

        assertNotContains(super.log, baos.toString(), TOTAL_INFORMATION_TEXT);
    }

    /**
     * Should not write statistics when {@code IGNITE_WRITE_REBALANCE_STATISTICS} == false
     *
     * @throws Exception not expected
     * @see IgniteSystemProperties#IGNITE_WRITE_REBALANCE_STATISTICS
     * */
    @Test
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_STATISTICS, value = "false")
    public void testNotPrintStatWhenNotIgniteWriteRebalanceStatistics() throws Exception {
        cacheCfgs = defaultCacheConfigurations();

        crd = startGrids(DEFAULT_NODE_CNT);

        fillCaches(100);

        log.registerListener(pw::write);

        startGrid(DEFAULT_NODE_CNT);

        awaitPartitionMapExchange();

        assertNotContains(super.log, baos.toString(), TOTAL_INFORMATION_TEXT);
    }

    /**
     * Should print total statistics without partition distribution when
     * {@code IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS} == false
     *
     * @throws Exception not expected
     * @see IgniteSystemProperties#IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS
     * */
    @Test
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, value = "false")
    public void testPrintStatisticsWithOutPartitionDistribution() throws Exception {
        cacheCfgs = defaultCacheConfigurations();

        crd = startGrids(DEFAULT_NODE_CNT);

        fillCaches(100);

        log.registerListener(pw::write);

        startGrid(DEFAULT_NODE_CNT);

        awaitPartitionMapExchange();

        String logOutput = baos.toString();

        assertContains(super.log, logOutput, TOTAL_INFORMATION_TEXT);
        assertNotContains(super.log, logOutput, PARTITIONS_DISTRIBUTION_TEXT);
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
    public void testPrintFullStatistics() throws Exception {
        cacheCfgs = defaultCacheConfigurations();

        crd = startGrids(DEFAULT_NODE_CNT);

        fillCaches(100);

        log.registerListener(pw::write);

        startGrid(DEFAULT_NODE_CNT);

        awaitPartitionMapExchange();

        String logOutput = baos.toString();

        assertContains(super.log, logOutput, TOTAL_INFORMATION_TEXT);
        assertContains(super.log, logOutput, PARTITIONS_DISTRIBUTION_TEXT);
    }

    /**
     * Check that we get only affected partitions.
     *
     * @throws Exception not expected
     * */
    @Test
    public void testCorrectPartitionsDistribution() throws Exception {
        String cacheName = "c1";
        int partCnt = 65_000;

        cacheCfgs = new CacheConfiguration[]{cacheConfiguration(cacheName, partCnt, 0)};

        crd = startGrids(3);

        fillCaches(65_000);

        log.registerListener(pw::write);

        startGrid(DEFAULT_NODE_CNT);

        awaitPartitionMapExchange();

        String logOutput = baos.toString();

        assertContains(super.log, logOutput, TOTAL_INFORMATION_TEXT);
        assertContains(super.log, logOutput, PARTITIONS_DISTRIBUTION_TEXT);

        Map<String, String> perCacheGrpTopicStat = perCacheGroupTopicStatistics(logOutput);

        String topicStat = perCacheGrpTopicStat.get(cacheName);

        assertNotNull(topicStat);

        Matcher matcher = compile("p=([0-9]+)").matcher(topicStat);

        int topicPartCnt = 0;
        while (matcher.find())
            topicPartCnt += parseInt(matcher.group(1));

        assertEquals(4, partCnt / topicPartCnt);
    }

    /**
     * Extract topic statistics for each caches.
     *
     * @param s text
     * @return key - name cache, value topic statistics
     * */
    private Map<String, String> perCacheGroupTopicStatistics(final String s) {
        assert nonNull(s);

        Map<String, String> perCacheGroupTopicStatistics = new HashMap<>();

        int startI = s.indexOf(INFORMATION_PER_CACHE_GROUP_TEXT);

        for (; ; ) {
            int tsti = s.indexOf(TOPIC_STATISTICS_TEXT, startI);
            if (tsti == -1)
                break;

            int ssti = s.indexOf(SUPPLIER_STATISTICS_TEXT, tsti);
            if (ssti == -1)
                break;

            int nai = s.indexOf(NAME_ATTRIBUTE, startI);
            if (nai == -1)
                break;

            int ci = s.indexOf(",", nai);
            if (ci == -1)
                break;

            String cacheName = s.substring(nai + NAME_ATTRIBUTE.length() + 1, ci);
            String topicStat = s.substring(tsti + TOPIC_STATISTICS_TEXT.length(), ssti);

            perCacheGroupTopicStatistics.put(cacheName, topicStat);
            startI = ssti;
        }

        return perCacheGroupTopicStatistics;
    }

    /** Create default {@link CacheConfiguration}'s  */
    private CacheConfiguration[] defaultCacheConfigurations() {
        return of(DEFAULT_CACHE_NAMES)
            .map(cacheName -> cacheConfiguration(cacheName, 100, 3))
            .toArray(CacheConfiguration[]::new);
    }

    /**
     * Fill all {@link #DEFAULT_CACHE_NAMES}.
     *
     * @param cnt - count of additions
     */
    private void fillCaches(final int cnt) {
        for (CacheConfiguration cacheCfg : cacheCfgs) {
            String name = cacheCfg.getName();

            IgniteCache<Object, Object> cache = crd.cache(name);

            range(0, cnt).forEach(value -> cache.put(value, name + value));
        }
    }
}
