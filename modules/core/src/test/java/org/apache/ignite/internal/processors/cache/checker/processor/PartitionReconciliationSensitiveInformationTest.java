/*
 * Copyright 2025 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.checker.processor;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.SensitiveMode;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.TestStorageUtils.corruptDataEntry;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.RECONCILIATION_DIR;

/**
 * Tests sensitive mode reporting.
 */
@WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
public class PartitionReconciliationSensitiveInformationTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    private static final int NODES_CNT = 2;

    /** Keys count. */
    protected static final int KEYS_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 12));
        ccfg.setBackups(1);
        ccfg.setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), RECONCILIATION_DIR, false));
    }

    @Test
    public void testSensitiveModes() throws Exception {
        startGrids(NODES_CNT);

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEYS_CNT; i++)
            cache.put(new CustomKey(i), new CustomValue("custom value " + i));

        CustomKey key = new CustomKey(12);
        int partId = grid(0).affinity(DEFAULT_CACHE_NAME).partition(key);

        for (SensitiveMode mode : SensitiveMode.values()) {
            corruptDataEntry(
                grid(1).cachex(DEFAULT_CACHE_NAME).context(),
                key,
                false,
                true,
                new GridCacheVersion(0, 0, 0L),
                "_broken");

            ReconciliationResult res = partitionReconciliation(
                grid(0),
                new VisorPartitionReconciliationTaskArg.Builder()
                    .fastCheck(false)
                    .repair(false)
                    .recheckAttempts(1)
                    .recheckDelay(0)
                    .includeSensitive(true)
                    .sensitiveMode(mode));

            assertEquals(
                "Number of inconsistent keys should not be empty.",
                1,
                res.partitionReconciliationResult().inconsistentKeysCount()
            );

            PartitionReconciliationDataRowMeta meta = res
                .partitionReconciliationResult()
                .inconsistentKeys()
                .get(DEFAULT_CACHE_NAME)
                .get(partId)
                .get(0);

            String keyView = meta.keyMeta().stringView(true);
            String valView0 = meta.valueMeta().get(grid(0).localNode().id()).stringView(true);
            String valView1 = meta.valueMeta().get(grid(1).localNode().id()).stringView(true);

            // The broken value is just a string.
            // It is not 'obfuscated' in any way. See ConsistencyCheckUtils#objectStringView
            assertTrue(
                "Val1 view is incorrect [val1=" + valView1 + ']',
                valView1.contains("BinaryObject_broken hex=["));

            switch (mode) {
                case DEFAULT: {
                    assertTrue(
                        "Key view is incorrect [key=" + keyView + ']',
                        keyView.contains("BinaryObject hex=["));

                    assertTrue(
                        "Val0 view is incorrect [val0=" + valView0 + ']',
                        valView0.contains("BinaryObject hex=["));

                    break;
                }
                case HASH: {
                    assertTrue(
                        "Key view is incorrect [key=" + keyView + ']',
                        checkHashView(keyView.split(" ")[0]));

                    assertTrue(
                        "Val0 view is incorrect [val0=" + valView0 + ']',
                        checkHashView(valView0.split(" ")[0]));

                    break;
                }
                case PLAIN: {
                    assertTrue(
                        "Key view is incorrect [key=" + keyView + ']',
                        keyView.contains("CustomKey"));

                    assertTrue(
                        "Val0 view is incorrect [val0=" + valView0 + ']',
                        valView0.contains("CustomValue"));

                    break;
                }

                default:
                    throw new IllegalArgumentException("Unsupported sensitive mode [mode=" + mode + ']');
            }

            cache.put(key, new CustomValue("custom value " + 12));
        }
    }

    /**
     * @param val String to check.
     * @return {@code true} if the provided {@code val} represents an integer value.
     */
    private static boolean checkHashView(String val) {
        try {
            Integer.parseInt(val);
            return true;
        }
        catch (NumberFormatException e) {
            return false;
        }
    }

    /** User defined key class. */
    public static class CustomKey {
        final int key;

        public CustomKey(int key) {
            this.key = key;
        }
    }

    /** User defined value class. */
    public static class CustomValue {
        final String customVal;

        public CustomValue(String val) {
            this.customVal = val;
        }
    }
}
