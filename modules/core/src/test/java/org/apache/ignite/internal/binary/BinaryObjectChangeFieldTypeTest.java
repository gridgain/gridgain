package org.apache.ignite.internal.binary;

import java.util.concurrent.Callable;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 *
 */
public class BinaryObjectChangeFieldTypeTest extends GridCommonAbstractTest {

    /** */
    private static final int NODE_COUNT = 2;

    /** Cache name */
    private static final String CACHE_NAME = "CACHE";

    /** */
    private IgniteTransactions transactions;
    /** */
    private IgniteEx ignite;
    /** */
    private IgniteBinary binary;

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        startGrids(NODE_COUNT);

        super.beforeTest();

        ignite = grid(0);

        binary = ignite.binary();

        transactions = ignite.transactions();
    }

    /**
     * {@inheritDoc}
     * */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * In progress
     * @throws Exception exc
     */
    @Test
    public void testTxNoBackups() throws Exception {

        CacheConfiguration<Integer, Foo> cc = new CacheConfiguration<Integer, Foo>(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setIndexedTypes(Integer.class, Foo.class);

        IgniteCache<Integer, Foo> fooCache = ignite.getOrCreateCache(cc);

        IgniteCache<Integer, BinaryObject> binCache = fooCache.withKeepBinary();

        try (Transaction tx = transactions.txStart()) {
            fooCache.put(1, new Foo("dummy", 123));
        }

        assertEquals(123, fooCache.get(1).intField);

        try (Transaction tx = transactions.txStart()) {
            fooCache.put(1, new Foo("dummy", 5));

            BinaryObject bo = binary.builder(Foo.class.getName())
                .build()
                .toBuilder()
                .setField("intField", "String")
                .build();

            binCache.put(1001, bo);

            tx.commit();
        }
        catch (TransactionHeuristicException e) {
            log().warning("expected exception", e);

        }

        // actually genereates NPE
        assertEquals(123, fooCache.get(1).intField);
    }

    /**
     * In progress
     * @throws Exception exc
     */
    @Test
    public void testTxReplicated() throws Exception {

        CacheConfiguration<Integer, Foo> cc = new CacheConfiguration<Integer, Foo>(CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Integer.class, Foo.class);

        IgniteCache<Integer, Foo> fooCache = ignite.getOrCreateCache(cc);

        IgniteCache<Integer, BinaryObject> binCache = fooCache.withKeepBinary();

        try (Transaction tx = transactions.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            fooCache.put(1, new Foo("1", 1));

            fooCache.put(2, new Foo("2", 2));

            tx.commit();
        }

        assertEquals(1, fooCache.get(1).intField);

        assertEquals(2, fooCache.get(2).intField);

        try (Transaction tx = transactions.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            fooCache.put(1, new Foo("11", 11));

            BinaryObject bo = binary.builder(Foo.class.getName())
                .build()
                .toBuilder()
                .setField("intField", "String")
                .build();

            binCache.put(1001, bo);

            fooCache.put(2, new Foo("22", 22));

            assertEquals(22, fooCache.get(2).intField);

            tx.commit();
            fail("exception expected!!!");
        }
        catch (TransactionHeuristicException e) {
            log().warning("log expected exception", e);
        }

        assertEquals(2, fooCache.get(2).intField);

        assertEquals(1, fooCache.get(1).intField);
    }

    /**
     * Checking change data type of a field on an already created BinaryObject
     */
    @Test
    public void testBuild1() {
        CacheConfiguration<Integer, Foo> cc = new CacheConfiguration<Integer, Foo>(CACHE_NAME);

        IgniteCache<Integer, Foo> clientCache = ignite.getOrCreateCache(cc);

        clientCache.put(1, new Foo("11", 11));

        GridTestUtils.assertThrows(log, new Callable<BinaryObject>() {
            @Override public BinaryObject call() throws Exception {
                return binary.builder(Foo.class.getName())
                    .removeField("intField")
                    .build()
                    .toBuilder()
                    .setField("intField", "String")
                    .build();
            }
        }, BinaryObjectException.class, null);

    }

    /**
     * Checking change data type of a field on an already created BinaryObject
     */
    @Test
    public void testBuild2() {
        CacheConfiguration<Integer, Foo> cc = new CacheConfiguration<>(CACHE_NAME);

        IgniteCache<Integer, Foo> fooCache = ignite.getOrCreateCache(cc);

        fooCache.put(1, new Foo("11", 11));

        GridTestUtils.assertThrows(log, new Callable<BinaryObject>() {
            @Override public BinaryObject call() throws Exception {
                return fooCache.<Integer, BinaryObject>withKeepBinary().get(1)
                    .toBuilder()
                    .removeField("intField")
                    .removeField("strField")
                    .build()
                    .toBuilder()
                    .setField("intField", "String")
                    .build();
            }
        }, BinaryObjectException.class, null);

    }

    /**
     *
     */
    static class Foo {

        /**
         *
         */
        @QuerySqlField
        private String strField;

        /**
         *
         */
        @QuerySqlField(index = true)
        private int intField;

        /**
         * @param strField String field.
         * @param intField Int field.
         */
        public Foo(String strField, int intField) {
            this.intField = intField;
            this.strField = strField;
        }
    }

}