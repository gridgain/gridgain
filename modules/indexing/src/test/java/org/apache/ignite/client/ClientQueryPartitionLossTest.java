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

package org.apache.ignite.client;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Tests client queries in the event of partition loss.
 */
public class ClientQueryPartitionLossTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
                .setDataStorageConfiguration(
                        new DataStorageConfiguration()
                                .setDefaultDataRegionConfiguration(
                                        new DataRegionConfiguration()
                                                .setMaxSize(100L * 1024 * 1024)
                                                .setPersistenceEnabled(true)
                                )
                )
                .setClientConnectorConfiguration(
                        new ClientConnectorConfiguration()
                                .setMaxOpenCursorsPerConnection(3));
    }

    @Test
    public void testPartitionLossSqlFieldsQuery() throws Exception {
        testPartitionLoss(cache -> {
            SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery("select id, name from \"Person\".PERSON where id = ?");
            sqlFieldsQuery.setArgs(ThreadLocalRandom.current().nextLong(100));

            return cache.query(sqlFieldsQuery);
        });
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testPartitionLossSqlQuery() throws Exception {
        testPartitionLoss(cache -> {
            SqlQuery<Integer, Person> sqlQuery = new SqlQuery<>(
                    Person.class, "select * from PERSON where id = ?");

            sqlQuery.setArgs(ThreadLocalRandom.current().nextLong(100));

            return cache.query(sqlQuery);
        });
    }

    @Test
    public void testPartitionLossScanQuery() throws Exception {
        testPartitionLoss(cache -> cache.query(new ScanQuery<Integer, Person>()));
    }

    private void testPartitionLoss(Function<ClientCache<Integer, Person>, QueryCursor<?>> queryFunc) throws Exception {
        Ignite srv1 = startGrid(0);
        Ignite srv2 = startGrid(1);

        srv1.cluster().state(ClusterState.ACTIVE);

        ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        IgniteClient client = Ignition.startClient(cfg);

        ClientCacheConfiguration ccfg = new ClientCacheConfiguration()
                .setName("Person")
                .setQueryEntities(createPersonQueryEntity());

        ClientCache<Integer, Person> personCache = client.getOrCreateCache(ccfg);

        for (int i = 0; i < 100; i++)
            personCache.put(i, new Person("Name" + i));

        boolean partitionsLost = false;

        for (int i = 0; i < 100; i++) {
            if (i == 10) {
                // Stop node to cause partition loss.
                srv2.close();
            }

            try (QueryCursor<?> cursor = queryFunc.apply(personCache)) {
                cursor.getAll();
            } catch (ClientException e) {
                if (e.getMessage().contains(
                        "Failed to execute query because cache partition has been lost [cacheName=Person")) {
                    partitionsLost = true;

                    // Ignore expected exception.
                    //noinspection CallToPrintStackTrace
                    e.printStackTrace();

                    continue;
                }

                throw e;
            }
        }

        assertTrue(partitionsLost);
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

    private static class Person implements Serializable {
        public String name;

        public Person() {
        }

        public Person(String name) {
            this.name = name;
        }
    }
}