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

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CheckpointFailBeforeWriteMarkTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    /** Cache name. */
    private static final String CACHE_NAME = "cacheOne";
    /** Cache size */
    private InterceptorIOFactory interceptorIOFactory = new InterceptorIOFactory();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setCheckpointThreads(2);
        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)

            .setMaxSize(30L * 1024 * 1024);

        storageCfg.setFileIOFactory(interceptorIOFactory);

        cfg.setDataStorageConfiguration(storageCfg)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER))
            .setCacheConfiguration(cacheConfiguration(CACHE_NAME, CacheAtomicityMode.TRANSACTIONAL))
        ;

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    private static class InterceptorIOFactory extends AsyncFileIOFactory {
        volatile boolean interrupt = false;

        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            System.out.println(file.getName());

            if (interrupt && file.getName().endsWith("START.bin.tmp")) {
//                try {
//                    Thread.sleep(10);
//                }
//                catch (InterruptedException e) {
//                    e.printStackTrace(); // TODO implement.
//                }
                throw new RuntimeException("Some error");
            }

            return super.create(file, modes); // TODO: CODE: implement.
        }
    }

    /**
     * @param cacheName Cache name.
     * @param mode Cache atomicity mode.
     * @return Configured cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(String cacheName, CacheAtomicityMode mode) {
        return new CacheConfiguration<>(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(mode)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setIndexedTypes(String.class, String.class);
    }

    /**
     * @throws Exception if fail.
     */
    @Test
    public void testCheckpointFailBeforeMarkEntityWrite() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        GridTestUtils.runAsync(() -> {

            Ignite ignite = ignite(0);

            IgniteCache<Integer, Object> cache2 = ignite.cache(CACHE_NAME);

            for (int i = 0; i < 200_000; i++) {
                cache2.put(i, new byte[i]);

                if(i % 1000 == 0)
                    log.info("WRITE : " + i);
            }
        });

        doSleep(3000);

        interceptorIOFactory.interrupt = true;

//        stopGrid(0 );

        doSleep(5000);

        interceptorIOFactory.interrupt = false;
        startGrid(0);

    }
}
