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

package org.apache.ignite.cache.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class IndexQueryPartitionTest extends GridCommonAbstractTest {
    private static final String CACHE_PARTITIONED = "CACHE-PARTIIONED";
    private static final String CACHE_REPLICATED = "CACHE-REPLICATED";

    /** */
    @Parameterized.Parameter
    public boolean client;

    /** */
    private static Map<Integer, Person> data;

    /** */
    @Parameterized.Parameters(name = "client={0}")
    public static List<Object[]> params() {
        return F.asList(
            new Object[] {false},
            new Object[] {true}
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Person> ccfg1 = new CacheConfiguration<Integer, Person>()
            .setName(CACHE_PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setIndexedTypes(Integer.class, Person.class)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(100));

        CacheConfiguration<Integer, Person> ccfg2 = new CacheConfiguration<Integer, Person>()
            .setName(CACHE_REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
            .setIndexedTypes(Integer.class, Person.class)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(100));

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(3);

        startClientGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testSinglePartition() {
        load();

        for (int part = 0; part < 100; part++) {
            Map<Integer, Person> expRes = expect(part);

            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setPartition(part);

            QueryCursor<Cache.Entry<Integer, Person>> cursor = grid().cache(CACHE_PARTITIONED).query(qry);

            for (Cache.Entry<Integer, Person> e : cursor) {
                Person p = expRes.remove(e.getKey());

                assertEquals(e.getKey().toString(), p, e.getValue());
            }

            assertTrue(expRes.isEmpty());
        }
    }

    /** */
    @Test
    public void testSetNullNotAffect() {
        try (IgniteDataStreamer<Integer, Person> dataStreamer = grid().dataStreamer(CACHE_PARTITIONED)) {
            Random rnd = new Random();

            for (int i = 0; i < 10_000; i++)
                dataStreamer.addData(i, new Person(rnd.nextInt()));
        }

        IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
            .setPartition(0);

        assertTrue(grid().cache(CACHE_PARTITIONED).query(qry).getAll().size() < 10_000);

        qry = new IndexQuery<Integer, Person>(Person.class)
            .setPartition(null);

        assertTrue(grid().cache(CACHE_PARTITIONED).query(qry).getAll().size() == 10_000);
    }

    /** */
    @Test
    public void testLocalWithPartition() {
        load();

        for (int part = 0; part < 100; part++) {
            IndexQuery<Integer, Person> qry = new IndexQuery<Integer, Person>(Person.class)
                .setPartition(part);

            qry.setLocal(true);

            IgniteCache<Integer, Person> cache = grid().cache(CACHE_PARTITIONED);

            if (client) {
                GridTestUtils.assertThrows(null, () -> cache.query(qry).getAll(),
                    CacheException.class,
                    "Execution of local IndexQuery on client node disallowed.");
            }
            else {
                boolean owner = grid().affinity(CACHE_PARTITIONED).mapPartitionToNode(part).equals(grid().localNode());

                assertEquals(!owner, cache.query(qry).getAll().isEmpty());
            }
        }
    }

    /** */
    @Test
    public void testNegativePartitionFails() {
        GridTestUtils.assertThrows(null, () -> new IndexQuery<Integer, Person>(Person.class).setPartition(-1),
            IllegalArgumentException.class,
            "Specified partition must be in the range [0, N) where N is partition number in the cache.");

        GridTestUtils.assertThrows(null, () -> new IndexQuery<Integer, Person>(Person.class).setPartition(-23),
            IllegalArgumentException.class,
            "Specified partition must be in the range [0, N) where N is partition number in the cache.");

        GridTestUtils.assertThrows(null, () -> {
                IndexQuery qry = new IndexQuery<Integer, Person>(Person.class).setPartition(1000);

                grid().cache(CACHE_PARTITIONED).query(qry);
            },
            CacheException.class,
            "Specified partition must be in the range [0, N) where N is partition number in the cache.");
    }

    @Test
    public void testSetPartitionOnReplicatedCacheFails() {
        IgniteCache<Integer, Person> cache = grid().cache(CACHE_REPLICATED);

        cache.put(1, new Person(1));

        IndexQuery<Integer, Person> idxQuery =
            new IndexQuery<Integer, Person>(Person.class).setPartition(1);

        GridTestUtils.assertThrows(null, () -> cache.query(idxQuery).getAll(),
            CacheException.class, "Partitions are not supported for replicated caches");
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx grid() {
        IgniteEx grid = client ? grid(3) : grid(0);

        assert (client && grid(0).localNode().isClient()) || !grid(0).localNode().isClient();

        return grid;
    }

    /** */
    private void load() {
        data = new HashMap<>();

        try (IgniteDataStreamer<Integer, Person> dataStreamer = grid(0).dataStreamer(CACHE_PARTITIONED)) {
            Random rnd = new Random();

            for (int i = 0; i < 10_000; i++) {
                Person p = new Person(rnd.nextInt());

                data.put(i, p);
                dataStreamer.addData(i, p);
            }
        }
    }

    /** */
    private Map<Integer, Person> expect(int part) {
        Map<Integer, Person> exp = new HashMap<>();

        for (Integer key: data.keySet()) {
            int p = grid(0).affinity(CACHE_PARTITIONED).partition(key);

            if (p == part)
                exp.put(key, data.get(key));
        }

        return exp;
    }

    /** */
    private static class Person {
        /** */
        @GridToStringInclude
        @QuerySqlField(index = true)
        private final int fld;

        /** */
        Person(int fld) {
            this.fld = fld;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return fld == ((Person)o).fld;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return fld;
        }
    }
}
