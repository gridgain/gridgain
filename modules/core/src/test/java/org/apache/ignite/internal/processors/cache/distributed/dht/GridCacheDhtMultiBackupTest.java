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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class GridCacheDhtMultiBackupTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridCacheDhtMultiBackupTest() {
        super(false /* don't start grid. */);
    }

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration()
            .setName("partitioned")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        return cfg;
    }

    /**
     * @throws Exception If failed
     */
    @Test
    public void testPut() throws Exception {
        try {
            Ignite g = startGrid(0);

            if (g.cluster().nodes().size() < 5)
                U.warn(log, "Topology is too small for this test. " +
                    "Run with 4 remote nodes or more having large number of backup nodes.");

            g.compute().run(new CAX() {
                    @IgniteInstanceResource
                    private Ignite g;

                    @Override public void applyx() {
                        X.println("Checking whether cache is empty.");

                        IgniteCache<SampleKey, SampleValue> cache = g.cache("partitioned");

                        assert cache.localSize() == 0;
                    }
                }
            );

            IgniteCache<SampleKey, SampleValue> cache = g.cache("partitioned");

            int cnt = 0;

            for (int key = 0; key < 1000; key++) {
                SampleKey key1 = new SampleKey(key);

                if (!g.cluster().localNode().id().equals(g.affinity("partitioned").mapKeyToNode(key1).id())) {
                    cache.put(key1, new SampleValue(key));

                    cnt++;
                }
            }

            X.println(">>> Put count: " + cnt);
        }
        finally {
            G.stopAll(false);
        }
    }

    /**
     *
     */
    private static class SampleKey implements Serializable {
        /** */
        private int key;

        /**
         * @param key
         */
        private SampleKey(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof SampleKey && ((SampleKey)obj).key == key;
        }
    }

    /**
     *
     */
    private static class SampleValue implements Serializable {
        /** */
        private int val;

        /**
         * @param val
         */
        private SampleValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof SampleValue && ((SampleValue)obj).val == val;
        }
    }
}
