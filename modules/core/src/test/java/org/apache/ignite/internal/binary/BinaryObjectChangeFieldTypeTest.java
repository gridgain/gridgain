/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 * Checking change data type of a field on an already created BinaryObject
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
     * Check commit changed BinaryObject to non-backup cache
     */
    @Test
    public void testTxNoBackups() {
        CacheConfiguration<Integer, TestClass> cc = new CacheConfiguration<Integer, TestClass>(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setIndexedTypes(Integer.class, TestClass.class);

        IgniteCache<Integer, TestClass> fooCache = ignite.getOrCreateCache(cc);

        IgniteCache<Integer, BinaryObject> binCache = fooCache.withKeepBinary();

        try (Transaction tx = transactions.txStart()) {
            fooCache.put(1, new TestClass("dummy", 123));
            tx.commit();
        }

        assertEquals(123, fooCache.get(1).intField);

        try (Transaction tx = transactions.txStart()) {
            fooCache.put(1, new TestClass("dummy", 5));

            BinaryObject bo = binary.builder(TestClass.class.getName())
                .build()
                .toBuilder()
                .setField("intField", "String")
                .build();

            binCache.put(1001, bo);

            tx.commit();
            fail("exception expected!!!");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Wrong value has been set"));
        }

        assertEquals(123, fooCache.get(1).intField);
    }

    /**
     * Check commit changed BinaryObject to replicated cache
     */
    @Test
    public void testTxReplicated(){
        CacheConfiguration<Integer, TestClass> cc = new CacheConfiguration<Integer, TestClass>(CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Integer.class, TestClass.class);

        IgniteCache<Integer, TestClass> fooCache = ignite.getOrCreateCache(cc);

        IgniteCache<Integer, BinaryObject> binCache = fooCache.withKeepBinary();

        try (Transaction tx = transactions.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            fooCache.put(1, new TestClass("1", 1));

            fooCache.put(2, new TestClass("2", 2));

            tx.commit();
        }

        assertEquals(1, fooCache.get(1).intField);

        assertEquals(2, fooCache.get(2).intField);

        try (Transaction tx = transactions.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            fooCache.put(1, new TestClass("11", 11));

            BinaryObject bo = binary.builder(TestClass.class.getName())
                .build()
                .toBuilder()
                .setField("intField", "String")
                .build();

            binCache.put(1001, bo);

            fooCache.put(2, new TestClass("22", 22));

            assertEquals(22, fooCache.get(2).intField);

            tx.commit();
            fail("exception expected!!!");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Wrong value has been set"));
        }

        assertEquals(1, fooCache.get(1).intField);

        assertEquals(2, fooCache.get(2).intField);
    }

    /**
     * Checking change data type of a field on an already created BinaryObject
     */
    @Test
    public void testBuildWithChangeOneField() {
        CacheConfiguration<Integer, TestClass> cc = new CacheConfiguration<>(CACHE_NAME);

        IgniteCache<Integer, TestClass> clientCache = ignite.getOrCreateCache(cc);

        clientCache.put(1, new TestClass("11", 11));

        GridTestUtils.assertThrows(log, new Callable<BinaryObject>() {
            @Override public BinaryObject call() throws Exception {
                return binary.builder(TestClass.class.getName())
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
    public void testBuildWithFullyRemovedFields() {
        CacheConfiguration<Integer, TestClass> cc = new CacheConfiguration<>(CACHE_NAME);

        IgniteCache<Integer, TestClass> clientCache = ignite.getOrCreateCache(cc);

        clientCache.put(1, new TestClass("11", 11));

        GridTestUtils.assertThrows(log, new Callable<BinaryObject>() {
            @Override public BinaryObject call() throws Exception {
                return clientCache.<Integer, BinaryObject>withKeepBinary().get(1)
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
    static class TestClass {

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
        public TestClass(String strField, int intField) {
            this.intField = intField;
            this.strField = strField;
        }
    }

}