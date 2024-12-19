/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.GridCacheSharedTtlCleanupManager.DEFAULT_TOMBSTONE_TTL_PROP;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

@RunWith(Parameterized.class)
public class PartitionReconciliationTombstonesWithIndicesTest extends PartitionReconciliationAbstractTest{
    /** Nodes. */
    protected static final int NODES_CNT = 3;

    /** Custom key class. */
    private static final String CUSTOM_KEY_CLS = "org.apache.ignite.tests.p2p.ReconciliationCustomKey";

    /** Custom value class. */
    private static final String CUSTOM_VAL_CLS = "org.apache.ignite.tests.p2p.ReconciliationCustomValue";

    /** Cache atomicity mode. */
    @Parameterized.Parameter(0)
    public CacheAtomicityMode cacheAtomicityMode;

    /** Fix mode. */
    @Parameterized.Parameter(1)
    public boolean fixMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(cacheAtomicityMode);
        ccfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));
        ccfg.setBackups(NODES_CNT - 1);
        ccfg.setReadFromBackup(true);

        ccfg.setIndexedTypes(
            getExternalClassLoader().loadClass(CUSTOM_KEY_CLS),
            getExternalClassLoader().loadClass(CUSTOM_VAL_CLS));

        cfg.setCacheConfiguration(ccfg);

        cfg.setConsistentId(name);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setClassLoader(getExternalClassLoader());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     *
     */
    @Parameterized.Parameters(name = "atomicity = {0}, repair = {1}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        CacheAtomicityMode[] atomicityModes = new CacheAtomicityMode[] {
            ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

        for (CacheAtomicityMode atomicityMode : atomicityModes) {
            params.add(new Object[] {atomicityMode, false});
            params.add(new Object[] {atomicityMode, true});
        }

        return params;
    }

    @Test
    @WithSystemProperty(key = DEFAULT_TOMBSTONE_TTL_PROP, value = "300000000")
    public void testTombstones() throws Exception {
        IgniteEx ig = startGrids(NODES_CNT);

        IgniteEx client = startClientGrid(NODES_CNT);

        ig.cluster().state(ACTIVE);

        Class<?> customKeyCls = ig.configuration().getClassLoader().loadClass(CUSTOM_KEY_CLS);
        Class<?> customValCls = ig.configuration().getClassLoader().loadClass(CUSTOM_VAL_CLS);

        Constructor<?> keyCtor = customKeyCls.getDeclaredConstructor(int.class);

        IgniteCache<Object, Object> clientCache = client.cache(DEFAULT_CACHE_NAME);

        Object primaryKey = keyCtor.newInstance(42);
        Object primaryVal = customValCls.newInstance();

        int primaryNodeIdx =  -1;
        for (int i = 0; i < NODES_CNT; ++i) {
            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimary(grid(i).localNode(), primaryKey)) {
                primaryNodeIdx = i;

                break;
            }
        }

        assertTrue("Failed to find primary node for key [key=" + primaryKey + ']', primaryNodeIdx >= 0);

        clientCache.put(primaryKey, primaryVal);

        // Block first NODES_CNT - 1 messages from primary node,
        // to emulate the case when primary contains a tombstone for the key
        // and backup nodes store the original value.
        AtomicInteger blocked = new AtomicInteger(NODES_CNT - 1);
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(primaryNodeIdx));
        spi.blockMessages((node, msg) -> {
            boolean blockedMsg = cacheAtomicityMode == ATOMIC ?
                msg instanceof GridDhtAtomicSingleUpdateRequest :
                msg instanceof GridDhtTxFinishRequest;

            return blockedMsg && blocked.decrementAndGet() >= 0;
        });

        final int pNodeIdx = primaryNodeIdx;
        IgniteInternalFuture<?> removeFut = runAsync(() -> {
            grid(pNodeIdx).cache(DEFAULT_CACHE_NAME).remove(primaryKey);
        });

        assertTrue(
            "Failed to wait all update messages that are sent to backup nodes.",
            waitForCondition(() -> blocked.get() == 0, 10_000));

        ReconciliationResult res = partitionReconciliation(ig, fixMode, RepairAlgorithm.PRIMARY, 4, DEFAULT_CACHE_NAME);

        assertTrue("unexpected error [errs=" + res.errors() + ']', res.errors().isEmpty());
        assertEquals(
            "Unexpected number of inconsistent keys.",
            1,
            res.partitionReconciliationResult().inconsistentKeysCount());

        if (fixMode) {
            // Check values after reconciliation.
            for (int i = 0; i < NODES_CNT; ++i) {
                Object val = grid(i).cache(DEFAULT_CACHE_NAME).get(primaryKey);

                assertNull("Unexpected value on node [actualVal=" + val + ", nodeId=" + i + ']', val);
            }
        }

        spi.stopBlock();
        removeFut.get(5, SECONDS);

        // Wait for processing of blocked messages.
        doSleep(1000);

        // Check that stale messages are ignored.
        for (int i = 0; i < NODES_CNT; ++i) {
            Object val = grid(i).cache(DEFAULT_CACHE_NAME).get(primaryKey);

            assertNull("Unexpected value on node [actualVal=" + val + ", nodeId=" + i + ']', val);
        }
    }
}
