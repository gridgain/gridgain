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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
public class CacheMvccPartitionedSqlTxQueriesWithReducerTest extends CacheMvccSqlTxQueriesWithReducerAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueryUpdateOnUnstableTopologyDoesNotCauseDeadlock() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesAbstractTest.MvccTestSqlIndexValue.class);

        testSpi = true;

        Ignite updateNode = startGrids(3);

        CountDownLatch latch = new CountDownLatch(1);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        spi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage) {
                latch.countDown();

                return true;
            }

            return false;
        });

        CompletableFuture.runAsync(() -> stopGrid(2));

        assertTrue(latch.await(TX_TIMEOUT, TimeUnit.MILLISECONDS));

        CompletableFuture<Void> queryFut = CompletableFuture.runAsync(() -> updateNode
            .cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQuery("INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) VALUES (1,1),(2,2),(3,3)"))
            .getAll());

        Thread.sleep(300);

        spi.stopBlock();

        try {
            queryFut.get(TX_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            assertTrue(X.hasCause(e, ClusterTopologyException.class));
        }
    }
}
