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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.junit.Test;
import org.locationtech.jts.util.Assert;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;

/**
 * Tests check a result of commit when a node fail before
 * send {@link GridNearTxFinishResponse} to transaction coodinator
 */
public class IgniteTxExceptionNodeFailTest extends GridCommonAbstractTest {
    /** syncMode */
    private static CacheWriteSynchronizationMode syncMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsConfig = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true));

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg
            .setDataStorageConfiguration(dsConfig)
            .setCacheConfiguration(new CacheConfiguration("cache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(syncMode).setBackups(0));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        FileUtils.deleteDirectory(new File(U.defaultWorkDirectory()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test with {@link CacheWriteSynchronizationMode#PRIMARY_SYNC}
     * @throws Exception
     */
    @Test
    public void testNodeFailWithPrimarySync() throws Exception {
        testNodeFail(PRIMARY_SYNC);
    }

    /**
     * Test with {@link CacheWriteSynchronizationMode#FULL_SYNC}
     * @throws Exception
     */
    @Test
    public void testNodeFailWithFullSync() throws Exception {
        testNodeFail(FULL_SYNC);
    }

    /**
     * <ul>
     * <li>Start 2 nodes with transactional cache without backups
     * <li>Start transaction:
     *  <ul>
     *  <li>put a key to a partition on transaction coordinator
     *  <li>put a key to a partition on other node
     *  <li>try to commit the transaction
     *  </ul>
     * <li>Stop other node when it try to send GridNearTxFinishResponse
     * <li>Check that {@link Transaction#commit()} throw {@link TransactionHeuristicException}
     * </ul>
     *
     * @param testSyncMode
     * @throws Exception
     */
    private void testNodeFail(CacheWriteSynchronizationMode testSyncMode) throws Exception {
        syncMode = testSyncMode;

        startGrids(2);

        grid(0).cluster().active(true);

        IgniteEx grid0 = grid(0);
        IgniteEx grid1 = grid(1);

        int key0 = 0;
        int key1 = 0;

        Affinity<Object> aff = grid1.affinity("cache");

        for (int i = 1; i < 1000; i++) {
            if (grid0.equals(grid(aff.mapKeyToNode(i)))) {
                key0 = i;

                break;
            }
        }

        for (int i = key0; i < 1000; i++) {
            if (grid1.equals(grid(aff.mapKeyToNode(i))) && !aff.mapKeyToNode(key1).equals(aff.mapKeyToNode(i))) {
                key1 = i;

                break;
            }
        }

        assert !aff.mapKeyToNode(key0).equals(aff.mapKeyToNode(key1));

        try (Transaction tx = grid1.transactions().txStart()) {
            grid1.cache("cache").put(key0, 100);
            grid1.cache("cache").put(key1, 200);

            spi(grid0).blockMessages((node, msg) -> {
                    if (msg instanceof GridNearTxFinishResponse) {
                        new Thread(
                            new Runnable() {
                                @Override public void run() {
                                    log().info("Stopping node: [" + grid0.name() + "]");

                                    IgnitionEx.stop(grid0.name(), true, null, true);
                                }
                            },
                            "node-stopper"
                        ).start();

                        return true;
                    }

                    return false;
                }
            );

            try {
                tx.commit();
            }
            catch (Throwable e) {
                String msg = e.getMessage();

                Assert.isTrue(e.getCause() instanceof CacheInvalidStateException);

                Pattern msgPattern = Pattern.compile("Failed to commit a transaction \\(all partition owners have left the grid, " +
                    "partition data has been lost\\) \\[cacheName=cache, partition=\\d+, " +
                    "key=KeyCacheObjectImpl \\[part=\\d+, val=" + key0 + ", hasValBytes=true\\]\\]");

                Matcher matcher = msgPattern.matcher(msg);

                Assert.isTrue(matcher.find());
            }
        }
    }
}