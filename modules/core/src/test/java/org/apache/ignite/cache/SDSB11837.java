package org.apache.ignite.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

public class SDSB11837 extends GridCommonAbstractTest {
    /** Cache1 name. */
    private static final String CACHE_1_NAME = "cache1";

    /** Cache2 name. */
    private static final String CACHE_2_NAME = "cache2";

    /** rnd instance. */
    private static final GridRandom rnd = new GridRandom();

    /** */
    private CacheConfiguration<Integer, Person> ccfg1;

    /** */
    private CacheConfiguration<Integer, Person> ccfg2;

    Person[] persons = createTestData();

    Random random = new Random();

    /** */
    private ListeningTestLogger testLogger = new ListeningTestLogger(log);

    /** */
    private LogListener lsnr = LogListener.matches("NullPointerException").build();

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setGridLogger(testLogger);

        ccfg1 = new CacheConfiguration<Integer, Person>(CACHE_1_NAME)
                .setCacheMode(PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(FULL_SYNC)
                .setBackups(1);
        ccfg2 = new CacheConfiguration<Integer, Person>(CACHE_2_NAME)
                .setCacheMode(PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(FULL_SYNC)
                .setBackups(1);

        return cfg.setCacheConfiguration(ccfg1,ccfg2)
                .setDataStorageConfiguration(new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                                .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        testLogger.registerListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        testLogger.unregisterListener(lsnr);
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

    @Test
    public void NPEReproducer() throws Exception {

        startGridsMultiThreaded(2);

        Ignition.setClientMode(true);

        IgniteEx client = startGrid("client");

        client.cluster().active(true);

        CountDownLatch destroyLatch = new CountDownLatch(1);

        final IgniteCache<Integer, Person> cache = client.getOrCreateCache(ccfg1);

        final IgniteCache<Integer, Person> cache2 = client.getOrCreateCache(ccfg2);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(client);

        IgniteInternalFuture f0 = GridTestUtils.runAsync(() -> {
            try {
                destroyLatch.await();

                IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
                    doSleep(rnd.nextInt(500));

                    spi.stopBlock();
                });

                cache.destroy();

                f.get();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });

        spi.blockMessages((node, msg) -> {
            if (msg instanceof GridNearTxPrepareRequest)
                destroyLatch.countDown();

            return false;
        });

        Map<Integer, Person> values = new HashMap<>();

        for (int i = 0; i < 100; i++)
            values.put(random.nextInt(1000), persons[random.nextInt(100)]);

        IgniteTransactions txs = client.transactions();

        IgniteInternalFuture f1 = GridTestUtils.runAsync(() -> {
                try (Transaction tx = txs.txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED, 5000, 200)) {

                    cache.putAll(values);

                    cache2.putAll(values);

                    tx.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                }
        });

        f0.get();
        f1.get();

        assertFalse(lsnr.check());
    }

    /**
     *
     */
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
