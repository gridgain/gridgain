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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SpiQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Indexing Spi query only test
 */
public class IndexingSpiQuerySelfTest extends GridCommonAbstractTest {
    private IndexingSpi indexingSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIndexingSpi(indexingSpi);
        cfg.setCommunicationSpi(new TestTcpCommunicationSpi());

        return cfg;
    }

    /** */
    protected <K,V> CacheConfiguration<K, V> cacheConfiguration(String cacheName) {
        return new CacheConfiguration<>(cacheName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleIndexingSpi() throws Exception {
        indexingSpi = new MyIndexingSpi();

        Ignite ignite = startGrid(0);

        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(DEFAULT_CACHE_NAME);

        IgniteCache<Integer, Integer> cache = ignite.createCache(ccfg);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        QueryCursor<Cache.Entry<Integer, Integer>> cursor = cache.query(new SpiQuery<Integer, Integer>().setArgs(2, 5));

        for (Cache.Entry<Integer, Integer> entry : cursor)
            System.out.println(entry);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIndexingSpiWithDisabledQueryProcessor() throws Exception {
        indexingSpi = new MyIndexingSpi();

        Ignite ignite = startGrid(0);

        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(DEFAULT_CACHE_NAME);

        IgniteCache<Integer, Integer> cache = ignite.createCache(ccfg);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        QueryCursor<Cache.Entry<Integer, Integer>> cursor = cache.query(new SpiQuery<Integer, Integer>().setArgs(2, 5));

        for (Cache.Entry<Integer, Integer> entry : cursor)
            System.out.println(entry);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryIndexingSpi() throws Exception {
        indexingSpi = new MyBinaryIndexingSpi();

        Ignite ignite = startGrid(0);

        CacheConfiguration<PersonKey, Person> ccfg = cacheConfiguration(DEFAULT_CACHE_NAME);

        IgniteCache<PersonKey, Person> cache = ignite.createCache(ccfg);

        for (int i = 0; i < 10; i++) {
            PersonKey key = new PersonKey(i);

            cache.put(key, new Person("John Doe " + i));
        }

        QueryCursor<Cache.Entry<PersonKey, Person>> cursor = cache.query(
            new SpiQuery<PersonKey, Person>().setArgs(new PersonKey(2), new PersonKey(5)));

        for (Cache.Entry<PersonKey, Person> entry : cursor)
            System.out.println(entry);

        cache.remove(new PersonKey(9));
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNonBinaryIndexingSpi() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_UNWRAP_BINARY_FOR_INDEXING_SPI, "true");

        try {
            indexingSpi = new MyIndexingSpi();

            Ignite ignite = startGrid(0);

            CacheConfiguration<PersonKey, Person> ccfg = cacheConfiguration(DEFAULT_CACHE_NAME);

            IgniteCache<PersonKey, Person> cache = ignite.createCache(ccfg);

            for (int i = 0; i < 10; i++) {
                PersonKey key = new PersonKey(i);

                cache.put(key, new Person("John Doe " + i));
            }

            QueryCursor<Cache.Entry<PersonKey, Person>> cursor = cache.query(
                new SpiQuery<PersonKey, Person>().setArgs(new PersonKey(2), new PersonKey(5)));

            for (Cache.Entry<PersonKey, Person> entry : cursor)
                System.out.println(entry);

            cache.remove(new PersonKey(9));
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_UNWRAP_BINARY_FOR_INDEXING_SPI);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIndexingSpiFailure() throws Exception {
        indexingSpi = new MyBrokenIndexingSpi();

        Ignite ignite = startGrid(0);

        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        final IgniteCache<Integer, Integer> cache = ignite.createCache(ccfg);

        final IgniteTransactions txs = ignite.transactions();

        for (final TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (final TransactionIsolation isolation : TransactionIsolation.values()) {
                System.out.println("Run in transaction: " + concurrency + " " + isolation);

                GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Transaction tx;

                        try (Transaction tx0 = tx = txs.txStart(concurrency, isolation)) {
                            cache.put(1, 1);

                            tx0.commit();
                        }

                        assertEquals(TransactionState.ROLLED_BACK, tx.state());
                        return null;
                    }
                }, IgniteTxHeuristicCheckedException.class);
            }
        }
    }

    @Test
    public void testSpiQueryPagination() throws Exception {
        final int pageSize = 5;

        final AtomicInteger queryPagesCount = new AtomicInteger(0);

        indexingSpi = new MyIndexingSpi();

        startGrid(0);

        indexingSpi = new MyIndexingSpi();

        Ignite ignite = startGrid(1);


        CacheConfiguration<Integer, Integer> ccfg = cacheConfiguration(DEFAULT_CACHE_NAME);

        IgniteCache<Integer, Integer> cache = ignite.createCache(ccfg);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        CommunicationSpi spi = ignite.configuration().getCommunicationSpi();

        assert spi instanceof TestTcpCommunicationSpi;

        TestTcpCommunicationSpi commSpi = (TestTcpCommunicationSpi)spi;

        commSpi.messageFilter = new IgniteInClosure<Message>() {
            @Override public void apply(Message msg) {
                if (!(msg instanceof GridIoMessage))
                    return;

                Message msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridCacheQueryRequest) {
                    assertEquals(pageSize, ((GridCacheQueryRequest)msg0).pageSize());

                    queryPagesCount.incrementAndGet();
                }
                else if (msg0 instanceof GridCacheQueryResponse)
                    assertTrue(((GridCacheQueryResponse)msg0).data().size() <= pageSize);
            }
        };

        QueryCursor<Cache.Entry<Integer, Integer>> cursor = cache.query(
                new SpiQuery<Integer, Integer>().setArgs(0, 101).setPageSize(pageSize));

        for (Cache.Entry<Integer, Integer> entry : cursor)
            System.out.println(entry);

        int remoteSize = cache.size() - cache.localSize(CachePeekMode.PRIMARY);;

        assert (pageSize * (queryPagesCount.get() - 1) <= remoteSize) &&
                (pageSize * queryPagesCount.get() >= remoteSize);
    }

    /**
     * Indexing Spi implementation for test
     */
    public static class MyIndexingSpi extends IgniteSpiAdapter implements IndexingSpi {
        /** Index. */
        private final SortedMap<Object, Object> idx = new TreeMap<>();

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Iterator<Cache.Entry<?, ?>> query(@Nullable String cacheName, Collection<Object> params,
            @Nullable IndexingQueryFilter filters) throws IgniteSpiException {
            if (params.size() < 2)
                throw new IgniteSpiException("Range parameters required.");

            Iterator<Object> paramsIt = params.iterator();

            Object from = paramsIt.next();
            Object to = paramsIt.next();

            from = from instanceof BinaryObject ? ((BinaryObject)from).deserialize() : from;
            to = to instanceof BinaryObject ? ((BinaryObject)to).deserialize() : to;

            SortedMap<Object, Object> map = idx.subMap(from, to);

            Collection<Cache.Entry<?, ?>> res = new ArrayList<>(map.size());

            for (Map.Entry<Object, Object> entry : map.entrySet())
                res.add(new CacheEntryImpl<>(entry.getKey(), entry.getValue()));

            return res.iterator();
        }

        /** {@inheritDoc} */
        @Override public void store(@Nullable String cacheName, Object key, Object val, long expirationTime)
            throws IgniteSpiException {
            assertFalse(key instanceof BinaryObject);
            assertFalse(val instanceof BinaryObject);

            idx.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable String cacheName, Object key) throws IgniteSpiException {
            // No-op.
        }
    }

    /**
     * Indexing Spi implementation for test. Accepts binary objects only
     */
    private static class MyBinaryIndexingSpi extends MyIndexingSpi {

        /** {@inheritDoc} */
        @Override public void store(@Nullable String cacheName, Object key, Object val,
            long expirationTime) throws IgniteSpiException {
            assertTrue(key instanceof BinaryObject);

            assertTrue(val instanceof BinaryObject);

            super.store(cacheName, ((BinaryObject)key).deserialize(), ((BinaryObject)val).deserialize(), expirationTime);
        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable String cacheName, Object key) throws IgniteSpiException {
            assertTrue(key instanceof BinaryObject);
        }
    }

    /**
     * Broken Indexing Spi implementation for test
     */
    private static class MyBrokenIndexingSpi extends MyIndexingSpi {
        /** {@inheritDoc} */
        @Override public void store(@Nullable String cacheName, Object key, Object val,
            long expirationTime) throws IgniteSpiException {
            throw new IgniteSpiException("Test exception");
        }
    }

    /**
     *
     */
     static class PersonKey implements Serializable, Comparable<PersonKey> {
        /** */
        private int id;

        /** */
        public PersonKey(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull PersonKey o) {
            return Integer.compare(id, o.id);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PersonKey key = (PersonKey)o;

            return id == key.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class Person implements Serializable {
        /** */
        private String name;

        /** */
        Person(String name) {
            this.name = name;
        }
    }

    /**
     *
     */
    private static class TestTcpCommunicationSpi extends TcpCommunicationSpi {
        /** */
        volatile IgniteInClosure<Message> messageFilter;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            if (messageFilter != null)
                messageFilter.apply(msg);

            super.sendMessage(node, msg, ackC);
        }
    }
}
