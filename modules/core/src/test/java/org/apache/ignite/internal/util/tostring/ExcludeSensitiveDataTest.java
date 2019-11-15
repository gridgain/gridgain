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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
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
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/**
 * Test for checking the exclusion of sensitive data from log.
 */
@WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
public class ExcludeSensitiveDataTest extends GridCommonAbstractTest {
    /** Class rule. */
    @ClassRule public static final TestRule classRule = new SystemPropertiesRule();

    /** Listener log messages. */
    private final ListeningTestLogger testLog = new ListeningTestLogger(false, log);

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
            .setGridLogger(testLog);
    }

    /**
     * Test for checking the exclusion of sensitive data from log
     * "Partition release future:".
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWithHidingSensitiveDataInPartitionReleaseFuture() throws Exception {
        int nodeCnt = 2;

        IgniteEx crd = startGrids(nodeCnt);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = crd.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(3)
                .setAffinity(new RendezvousAffinityFunction(false, 10))
        ).withKeepBinary();

        Person person = new Person(0, "name_1");

        BinaryObject binPerson = crd.binary().toBinary(person);

        cache.put(0, binPerson);

        Transaction tx = crd.transactions().txStart();

        cache.put(0, binPerson);

        AtomicReference<String> strToCheckRef = new AtomicReference<>();

        LogListener logLsnr = LogListener.matches(logStr -> {
            if (logStr.contains("Partition release future:") && currentThread().getName().contains(crd.name())) {
                strToCheckRef.set(logStr);

                return true;
            }

            return false;
        }).build();

        testLog.registerListener(logLsnr);

        GridTestUtils.runAsync(() -> {
            while (!logLsnr.check())
                ;

            tx.commit();
        });

        startGrid(nodeCnt);

        String strToCheck = strToCheckRef.get();

        String binPersonStr = U.compact(binPerson.toString());

        assertNotContains(log, strToCheck, binPersonStr);
    }

    public void a(int nodeCnt, BiConsumer<String, String> check) throws Exception {
        assert nonNull(check);

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
