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

package org.apache.ignite.internal.util.tostring;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static java.lang.Thread.currentThread;
import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Class for checking sensitive data when outputting transactions to the log.
 */
public class TransactionSensitiveDataTest extends GridCommonAbstractTest {
    /** Class rule. */
    @ClassRule public static final TestRule classRule = new SystemPropertiesRule();

    /** Listener log messages. */
    private final ListeningTestLogger testLog = new ListeningTestLogger(false, log);

    /** Network timeout. */
    private static final long NETWORK_TIMEOUT = 500;

    /** Node count. */
    private static final int NODE_COUNT = 2;

    /** Create a client node. */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        testLog.clearListeners();

        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setGridLogger(testLog)
            .setClientMode(client)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(TRANSACTIONAL)
                    .setBackups(3)
                    .setAffinity(new RendezvousAffinityFunction(false, 10))
            )
            .setNetworkTimeout(NETWORK_TIMEOUT);
    }

    /**
     * Test for checking the absence of sensitive data in log during an
     * exchange while an active transaction is running.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
    @Test
    public void testHideSensitiveDataDuringExchange() throws Exception {
        checkSensitiveDataDuringExchange((s1, s2) -> assertNotContains(log, s1, Person.class.getSimpleName()));
    }

    /**
     * Test for checking the presence of sensitive data in log during an
     * exchange while an active transaction is running.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "true")
    @Test
    public void testShowSensitiveDataDuringExchange() throws Exception {
        checkSensitiveDataDuringExchange((s1, s2) -> assertContains(log, s1, s2));
    }

    /**
     * Test for checking the absence of sensitive data in log when node exits
     * during transaction preparation.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
    @Test
    public void testHideSensitiveDataDuringNodeLeft() throws Exception {
        checkSensitiveDataDuringNodeLeft((s1, s2) -> assertNotContains(log, s1, Person.class.getSimpleName()));
    }

    /**
     * Test for checking the presence of sensitive data in log when node exits
     * during transaction preparation.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "true")
    @Test
    public void testShowSensitiveDataDuringNodeLeft() throws Exception {
        checkSensitiveDataDuringNodeLeft((s1, s2) -> assertContains(log, s1, s2));
    }

    /**
     * Receiving a log message "Partition release future:" during the exchange
     * to check whether or not sensitive data is in printed transactions.
     *
     * @param check Check sensitive data in log message.
     * @throws Exception If failed.
     */
    private void checkSensitiveDataDuringExchange(BiConsumer<String, String> check) throws Exception {
        assert nonNull(check);

        IgniteEx crd = startGrids(NODE_COUNT);

        awaitPartitionMapExchange();

        AtomicReference<String> strToCheckRef = new AtomicReference<>();

        LogListener logLsnr = LogListener.matches(logStr -> {
            if (logStr.contains("Partition release future:") && currentThread().getName().contains(crd.name())) {
                strToCheckRef.set(logStr);

                return true;
            }

            return false;
        }).build();

        testLog.registerListener(logLsnr);

        IgniteCache<Object, Object> cache = crd.getOrCreateCache(DEFAULT_CACHE_NAME).withKeepBinary();

        BinaryObject binPerson = createPersonBinObj(crd);

        cache.put(0, binPerson);

        Transaction tx = crd.transactions().txStart();

        cache.put(0, binPerson);

        GridTestUtils.runAsync(() -> {
            logLsnr.check(4 * NETWORK_TIMEOUT);

            tx.commit();

            return null;
        });

        startGrid(NODE_COUNT);

        check.accept(strToCheckRef.get(), toStr(binPerson));
    }

    /**
     * Receiving the “Failed to send message to remote node” and
     * “Received error when future is done” message logs during the node exit
     * when preparing the transaction to check whether or not sensitive data
     * is in the printed transactions.
     *
     * @param check Check sensitive data in log messages.
     * @throws Exception If failed.
     */
    private void checkSensitiveDataDuringNodeLeft(BiConsumer<String, String> check) throws Exception {
        assert nonNull(check);

        client = false;

        startGrids(NODE_COUNT);

        client = true;

        IgniteEx clientNode = startGrid(NODE_COUNT);

        awaitPartitionMapExchange();

        AtomicReference<String> strFailedSndRef = new AtomicReference<>();
        AtomicReference<String> strReceivedErrorRef = new AtomicReference<>();

        testLog.registerListener(logStr -> {
            if (logStr.contains("Failed to send message to remote node"))
                strFailedSndRef.set(logStr);
        });

        testLog.registerListener(logStr -> {
            if (logStr.contains("Received error when future is done"))
                strReceivedErrorRef.set(logStr);
        });

        int stopGridId = 0;

        TestRecordingCommunicationSpi.spi(clientNode).closure((clusterNode, message) -> {
            if (GridNearTxPrepareRequest.class.isInstance(message))
                stopGrid(stopGridId);
        });

        String cacheName = DEFAULT_CACHE_NAME;

        IgniteCache<Object, Object> cache = clientNode.getOrCreateCache(cacheName).withKeepBinary();

        BinaryObject binPerson = createPersonBinObj(clientNode);

        try (Transaction tx = clientNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(primaryKey(grid(stopGridId).cache(cacheName)), binPerson);

            tx.commit();
        }
        catch (Exception ignored) {
            //ignore
        }

        String binPersonStr = toStr(binPerson);

        check.accept(strFailedSndRef.get(), binPersonStr);
        check.accept(strReceivedErrorRef.get(), binPersonStr);
    }

    /**
     * Create a string to search for a Person’s BinaryObject in the log.
     *
     * @param binPerson BinaryObject.
     * @return String representation of BinaryObject.
     */
    private String toStr(BinaryObject binPerson) {
        assert nonNull(binPerson);

        return binPerson.toString().replace(Person.class.getName(), Person.class.getSimpleName());
    }

    /**
     * Create a new BinaryObject for Person class.
     *
     * @param node Node to create an BinaryObject.
     * @return BinaryObject.
     */
    private BinaryObject createPersonBinObj(Ignite node) {
        assert nonNull(node);

        ThreadLocalRandom random = ThreadLocalRandom.current();

        return node.binary().toBinary(new Person(random.nextInt(), "name_" + random.nextInt()));
    }

    /**
     * Person class for cache storage.
     */
    static class Person implements Serializable {
        /** Id organization. */
        int orgId;

        /** Person name. */
        String name;

        /**
         * Constructor.
         *
         * @param orgId Id organization.
         * @param name Person name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }
}
