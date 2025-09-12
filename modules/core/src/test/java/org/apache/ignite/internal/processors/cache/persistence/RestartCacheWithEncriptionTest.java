/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.stream.Stream;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test class for verifying cache restart functionality with encryption enabled.
 * Extends {@link GridCommonAbstractTest} to leverage Ignite's test framework.
 */
public class RestartCacheWithEncriptionTest extends GridCommonAbstractTest {

    /** Number of partitions for the cache. */
    public static final int PARTS = 10;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(AbstractEncryptionTest.KEYSTORE_PATH);
        encSpi.setKeyStorePassword(AbstractEncryptionTest.KEYSTORE_PASSWORD.toCharArray());

        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setEncryptionSpi(encSpi)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    // 10 MB in bytes.
                    .setMaxSize(100l * 1024 * 1024)
                    .setPersistenceEnabled(true)))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setEncryptionEnabled(true)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS))
            );
    }

    /**
     * Tests the restart of encrypted caches.
     *
     * @throws Exception If an error occurs during the test.
     */
    @Test
    public void testRestartCaches() throws Exception {
        IgniteEx ignite0 = startGrids(3);

        ignite0.cluster().state(ClusterState.ACTIVE);

        load(ignite0, DEFAULT_CACHE_NAME, Stream.iterate(0, n -> n + 1).limit(1000));

        TestRecordingCommunicationSpi.spi(ignite0).blockMessages((node, msg) -> {
            if (getTestIgniteInstanceName(2).equals(node.consistentId())) {
                info("Message sending to nodeId=" + node.consistentId() + " msg=" + msg.getClass().getSimpleName());

                return msg instanceof GridDhtPartitionsFullMessage;
            }

            return false;
        });

        destroyCache(DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi.spi(ignite0).waitForBlocked();

        IgniteInternalFuture startCacheFut = GridTestUtils.runAsync(() -> {
            startCache(DEFAULT_CACHE_NAME);
        });

        assertTrue(GridTestUtils.waitForCondition(() -> ignite(2).context().cache().cacheGroupDescriptors()
            .get(GridCacheUtils.cacheId(DEFAULT_CACHE_NAME)) != null, 10_000));

        TestRecordingCommunicationSpi.spi(ignite0).stopBlock();

        startCacheFut.get();

        forceCheckpoint();
    }

    /**
     * Starts a cache with the given name and encryption enabled.
     *
     * @param name Name of the cache to start.
     */
    private void startCache(String name) {
        ignite(1).getOrCreateCache(new CacheConfiguration<>(name)
            .setEncryptionEnabled(true)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS)));

        info("Cache restarted: " + name);
    }

    /**
     * Destroys the cache with the given name.
     *
     * @param name Name of the cache to destroy.
     */
    private void destroyCache(String name) {
        ignite(1).cache(name).destroy();

        info("Cache destroyed: " + name);
    }
}
