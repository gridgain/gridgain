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
package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander.RebalanceFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static java.util.Objects.nonNull;
import static java.util.regex.Pattern.compile;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/**
 * Class checks the presence of evicted partitions in log.
 */
@WithSystemProperty(key = "SHOW_EVICTION_PROGRESS_FREQ", value = "10")
public class EvictPartitionInLogTest extends GridCommonAbstractTest {
    /** Class rule. */
    @ClassRule public static final TestRule classRule = new SystemPropertiesRule();

    /** Listener log messages. */
    private static ListeningTestLogger testLog;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ((AtomicReference<IgniteLogger>)getFieldValue(GridDhtLocalPartition.class, "logRef")).set(null);
        setFieldValue(GridDhtLocalPartition.class, "log", null);

        testLog = new ListeningTestLogger(false, log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        testLog.clearListeners();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog)
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setBackups(0)
                    .setAffinity(new RendezvousAffinityFunction(false, 12))
                    .setIndexedTypes(Integer.class, Integer.class)
            );
    }

    /**
     * Test checks the presence of evicted partitions (RENTING state) in log.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEvictPartByRentingState() throws Exception {
        startGrid(0);
        awaitPartitionMapExchange();

        LogListener logLsnr = LogListener.matches(pattern(DEFAULT_CACHE_NAME, "eviction")).build();
        testLog.registerListener(logLsnr);

        internalCache(0).context().topology().localPartitions().stream()
            .peek(p -> p.setState(RENTING))
            .forEach(GridDhtLocalPartition::clearAsync);

        assertTrue(logLsnr.check());
    }

    /**
     * Test checks the presence of evicted partitions (MOVING state) in log.
     *
     * @throws Exception If failed.
     * */
    @Test
    public void testEvictPartByMovingState() throws Exception {
        startGrid(0);
        awaitPartitionMapExchange();

        LogListener logLsnr = LogListener.matches(pattern(DEFAULT_CACHE_NAME, "clearing")).build();
        testLog.registerListener(logLsnr);

        GridCacheAdapter<Object, Object> internalCache = internalCache(0);

        RebalanceFuture rebFut = (RebalanceFuture)internalCache.context().preloader().rebalanceFuture();
        rebFut.reset();

        internalCache.context().topology().localPartitions().stream()
            .peek(p -> p.setState(MOVING))
            .forEach(GridDhtLocalPartition::clearAsync);

        rebFut.onDone(Boolean.TRUE);

        assertTrue(logLsnr.check());
    }

    /**
     * Creating a pattern to search for message "Evicted partitions..." in log.
     *
     * @param cacheName Cache name.
     * @param reason Reason for eviction.
     * @return Pattern.
     */
    private Pattern pattern(String cacheName, String reason) {
        assert nonNull(cacheName);
        assert nonNull(reason);

        return compile("Evicted partitions \\[grpId=" + CU.cacheId(cacheName) + ", grpName=" + cacheName + ", " +
            reason + "=\\[[0-9\\-]*]]");
    }
}
