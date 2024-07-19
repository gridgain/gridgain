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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ignite.internal.processors.platform.client.ClientStatus.TOO_MANY_CURSORS;

/**
 * TooManyOpenCursorsTest
 */
public class TooManyOpenCursorsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setMaxSize(100L * 1024 * 1024)
                                        .setPersistenceEnabled(true)
                        )
        );

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTooManyOpenCursors() throws Exception {
        Ignite srv1 = startGrid(0);
        Ignite srv2 = startGrid(1);

        srv1.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>("Person");
        ccfg.setQueryEntities(Collections.singletonList(createPersonQueryEntity()));

        IgniteCache<Integer, Person> personCache = srv1.getOrCreateCache(ccfg);

        for (int i = 0; i < 100; i++)
            personCache.put(i, new Person("Name" + i));


        ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        IgniteClient client = Ignition.startClient(cfg);

        CountDownLatch latch = new CountDownLatch(1);

        GridTestUtils.runAsync(new ExecuteQueryTask(client, latch));

        srv2.close();

        latch.await();

        fail("Too many open cursors!");
    }

    private static QueryEntity createPersonQueryEntity() {
        return new QueryEntity()
                .setValueType(Person.class.getName())
                .setKeyType(Integer.class.getName())
                .addQueryField("id", Integer.class.getName(), null)
                .addQueryField("name", String.class.getName(), null)
                .setKeyFieldName("id")
                .setTableName("PERSON");
    }

    private static class ExecuteQueryTask implements Runnable {
        private final IgniteClient client;

        private final CountDownLatch latch;

        public ExecuteQueryTask(IgniteClient client, CountDownLatch latch) {
            this.client = client;
            this.latch = latch;
        }

        @Override
        public void run() {
            while (true) {
                String sql = "select id, name from \"Person\".PERSON where id = ?";

                SqlFieldsQuery query = new SqlFieldsQuery(sql);

                query.setArgs(ThreadLocalRandom.current().nextLong(100));

                try (FieldsQueryCursor<List<?>> cursor = client.query(query)) {
                    Iterator<List<?>> itr = cursor.iterator();

                    while (itr.hasNext()) {
                        System.out.println(itr.next().get(0));
                    }
                } catch (ClientException e) {
                    if (e.getCause() != null && e.getCause() instanceof ClientServerError) {
                        ClientServerError cse = (ClientServerError) e.getCause();
                        if (cse.getCode() == TOO_MANY_CURSORS) {
                            e.printStackTrace();

                            latch.countDown();

                            throw e;
                        }
                    }

                    e.printStackTrace();
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static class Person implements Serializable {
        public String name;

        public Person() {
        }

        public Person(String name) {
            this.name = name;
        }
    }
}