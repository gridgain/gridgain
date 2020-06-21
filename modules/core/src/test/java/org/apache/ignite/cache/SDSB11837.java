package org.apache.ignite.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

public class SDSB11837 extends GridCommonAbstractTest {
    Person[] persons = createTestData();

    Random random = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * What's the plan:
     * 1. start grids
     * 2. In loop do the following until bug is reproduced:
     * 2.1 create cache
     * 2.2 apply load on this cache
     * 2.3. delete cache/deactivate grid of somehow call stopCache method
     */
    @Test
    public void test() throws Exception {
        IgniteEx igniteEx = startGrids(3);

        igniteEx.cluster().active(true);

        IgniteEx client = startGrid("client");

        NearCacheConfiguration<Integer, Person> nearCfg = new NearCacheConfiguration<>();

        CacheConfiguration<Integer, Person> cfg =
            new CacheConfiguration<Integer, Person>("testCache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(1);

        AtomicBoolean interrupted = new AtomicBoolean(false);

        IgniteTransactions txs = client.transactions();

        while (true) {
            interrupted.set(false);

            IgniteCache cache = client.getOrCreateCache(cfg, nearCfg);

            Map<Integer, Person> values = new HashMap<>();

            for (int i = 0; i < 100; i++)
                values.put(random.nextInt(1000), persons[random.nextInt(100)]);

            GridTestUtils.runAsync(() -> {
                while (!interrupted.get()) {
                    try (Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                        cache.putAll(values);

                        tx.commit();
                    }
                    catch (Exception e) {
                        e.printStackTrace();

                        interrupted.set(true);
                    }
                }
            });

            U.sleep(300);

            cache.destroy();

            U.sleep(300);
        }
    }

    /** */
    private Person[] createTestData() {
        final int num = 100;

        Person[] res = new Person[num];

        for (int i = 0; i < num; i++)
            res[i] = new Person("John", "Dow", i);

        return res;
    }

    private class Person {
        String firstName;
        String lastName;
        int age;

        public Person(String firstName, String lastName, int age) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }
    }
}
