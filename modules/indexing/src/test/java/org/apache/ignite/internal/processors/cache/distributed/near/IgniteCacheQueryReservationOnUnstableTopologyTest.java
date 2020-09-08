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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests partition reservation releasing for queries over unstable topology.
 */
public class IgniteCacheQueryReservationOnUnstableTopologyTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS = 32;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setIndexedTypes(Integer.class, Integer.class)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS)).setBackups(1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testQueryReservationReleaseNormal() throws Exception {
        doTestQueryReservationRelease(false, false);
    }

    /** */
    @Test
    public void testQueryReservationReleaseLazy() throws Exception {
        doTestQueryReservationRelease(true, false);
    }

    /** */
    @Test
    public void testQueryReservationReleaseNormal_SmallPage() throws Exception {
        doTestQueryReservationRelease(false, true);
    }

    /** */
    @Test
    public void testQueryReservationReleaseLazy_SmallPage() throws Exception {
        doTestQueryReservationRelease(true, true);
    }

    /**
     * @param lazy Lazy flag for query.
     * @param smallPage {@code True} to set small page size.
     *
     * @throws Exception If failed.
     */
    private void doTestQueryReservationRelease(boolean lazy, boolean smallPage) throws Exception {
        IgniteEx crd = startGrids(2);

        awaitPartitionMapExchange();

        final int keys = PARTS * 5;

        for (int i = 0; i < keys; i++)
            crd.cache(DEFAULT_CACHE_NAME).put(i, i);

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> qryFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while (!stop.get()) {
                    SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM Integer");

                    qry.setLazy(lazy);

                    if (smallPage)
                        qry.setPageSize(1);

                    assertEquals(keys, grid(0).cache(DEFAULT_CACHE_NAME).query(qry).getAll().size());
                }
            }
        }, 3, "qry-thread");

        doSleep(100);

        startGrid(2);
        startGrid(3);

        stop.set(true);

        awaitPartitionMapExchange();

        qryFut.get();
    }
}
