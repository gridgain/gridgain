/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridCacheVersionGenerationWithCacheStorageTest extends GridCommonAbstractTest {
//
//    /** {@inheritDoc} */
//    @SuppressWarnings("unchecked")
//    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
//        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
//        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
//        ccfg.setAtomicityMode(ATOMIC);
//        ccfg.setCacheMode(PARTITIONED);
//        ccfg.setCacheStoreFactory(singletonFactory(new TestStore()));
//        ccfg.setReadThrough(true);
//        ccfg.setBackups(0);
//        ccfg.setIndexedTypes(
//            Integer.class, Integer.class
//        );
//        cfg.setCacheConfiguration(ccfg);
//        return cfg;
//    }
//
//    @Test
//    public void testFooBar2() throws Exception {
//        startGrid(0);
//        startGrid(2);
//        assert ignite(0).context().discovery().topologyVersion() == 2;
//        System.out.println("!!!!!!!!!1: " + ignite(0).context().discovery().topologyVersion());
//
////        blockSendingFullMessage(grid(0));
////        GridDhtPartitionsExchangeFuture.latch = new CountDownLatch(1);
//
//        IgniteInternalFuture fut = GridTestUtils.runAsync(()-> {
//            try {
//                startGrid(1);
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//        Thread.sleep(3_000);
//        System.out.println("!!!!!!!!!2: " + ignite(0).context().discovery().topologyVersion());
//        assert ignite(0).context().discovery().topologyVersion() == 3;
//        ignite(0).cache(DEFAULT_CACHE_NAME).get(0);
//        Thread.sleep(1_000);
//        ignite(0).cache(DEFAULT_CACHE_NAME).put(0, 0);
//
////        GridDhtPartitionsExchangeFuture.latch.countDown();
//
//        fut.get();
//        GridCacheContext<Object, Object> cctx = ignite(0).context().cache().cache(DEFAULT_CACHE_NAME).context();
//        CacheGroupContext grpCtx = cctx.group();
//        for (int i = 0; i < 1024; i++) {
//            GridDhtLocalPartition part = grpCtx.topology().localPartition(i);
//            try {
//                GridCursor<? extends CacheDataRow> cursor = grpCtx.offheap().dataStore(part).cursor(cctx.cacheId());
//                while (cursor.next()) {
//                    CacheDataRow row = cursor.get();
//                    System.out.println("<<<<<<<<<<2: " + row.version() + " " + row.key() + " " + row.value());
//                }
//            }
//            catch (IgniteCheckedException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    /**
//     * Test store.
//     */
//    private static class TestStore extends CacheStoreAdapter<Integer, Integer> {
//        /** {@inheritDoc} */
//        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, Object... args) {
//            // No-op;
//        }
//        /** {@inheritDoc} */
//        @Override public Integer load(Integer key) {
//            assert key != null;
//            return 0;
//        }
//        /** {@inheritDoc} */
//        @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends Integer> e) {
//            // No-op;
//        }
//        /** {@inheritDoc} */
//        @Override public void delete(Object key) {
//            // No-op;
//        }
//    }
//
//    /**
//     * Blocks sending full message from coordinator to non-coordinator node.
//     *
//     * @param from Coordinator node.
//     * @param pred Non-coordinator node predicate.
//     *                  If predicate returns {@code true} a full message will not be send to that node.
//     */
//    private void blockSendingFullMessage(IgniteEx from) {
//        // Block FullMessage for newly joined nodes.
//        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(from);
//
//        // Delay sending full messages (without exchange id).
//        spi.blockMessages((node, msg) -> {
//            if (msg instanceof GridDhtPartitionsFullMessage) {
//                GridDhtPartitionsFullMessage fullMsg = (GridDhtPartitionsFullMessage) msg;
//
//                if (fullMsg.exchangeId() != null) {
//                    log.warning("Blocked sending " + msg + " to " + node);
//
//                    return true;
//                }
//            }
//
//            return false;
//        });
//    }
}
