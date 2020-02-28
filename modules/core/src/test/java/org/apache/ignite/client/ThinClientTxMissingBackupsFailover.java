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

package org.apache.ignite.client;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Thin client backup copy failover tests.
 */
@RunWith(Parameterized.class)
public class ThinClientTxMissingBackupsFailover extends GridCommonAbstractTest {
    /**
     * @return List of test parameters.
     */
    @Parameterized.Parameters(name = "{0} - {1}")
    public static List<Object[]> testData() {
        List<Object[]> data = new ArrayList<>(6);
        data.add(new Object[] {TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED});
        data.add(new Object[] {TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ});
        data.add(new Object[] {TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE});
        data.add(new Object[] {TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED});
        data.add(new Object[] {TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ});
        data.add(new Object[] {TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE});
        return data;
    }

    /** */
    @Parameterized.Parameter(0)
    public TransactionConcurrency concurrency;

    /** */
    @Parameterized.Parameter(1)
    public TransactionIsolation isolation;

    /**
     * Check that removing backup copy for tx doesn't pervent it from commit.
     */
    @Test
    public void testMissingBackupTxFailover() throws Exception {
        try (Ignite ignite = Ignition.start(Config.getServerConfiguration());
             Ignite ignite2 = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            ClientCache<Integer, String> cache = client.createCache(new ClientCacheConfiguration()
                    .setName("cache")
                    .setBackups(1)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            );
            cache.put(0, "value0");

            final CountDownLatch latch = new CountDownLatch(1);

            try (ClientTransaction tx = client.transactions().txStart(concurrency, isolation)) {
                Thread t = new Thread(() -> {
                    try {
                        Collection<ClusterNode> mapping = ignite.affinity("cache").mapKeyToPrimaryAndBackups(0);
                        mapping.stream().skip(1).forEach(node -> {
                            stopGrid(node.id().toString());
                        });
                    }
                    catch (Exception ex) {
                        fail();
                    }
                    finally {
                        latch.countDown();
                    }
                });

                assertEquals("value0", cache.get(0));

                t.start();

                latch.await();

                String s = tx.toString();

                cache.put(0, "value1");

                t.join();

                tx.commit();
            }

            assertEquals("value1", cache.get(0));
        }
    }

    /** */
    private static ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration()
                .setAddresses(Config.SERVER)
                .setSendBufferSize(0)
                .setReceiveBufferSize(0);
    }
}
