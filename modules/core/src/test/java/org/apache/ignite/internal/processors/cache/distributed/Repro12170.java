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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

public class Repro12170 extends GridCacheAbstractSelfTest {

    private ClusterNode wierdNode;

    /** */
    protected static final int GRID_CNT = 3;

    /** Ignore node ID. */
    private volatile UUID ignoreMsgNodeId;

    /** Ignore message class. */
    private Class<?> ignoreMsgCls;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testManyKeysCommit() throws Exception {
        Collection<Integer> keys = new ArrayList<>(200);

        for (int i = 0; i < 200; i++)
            keys.add(i);

        testTxOriginatingNodeFails(keys, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testManyKeysRollback() throws Exception {
        Collection<Integer> keys = new ArrayList<>(200);

        for (int i = 0; i < 200; i++)
            keys.add(i);

        testTxOriginatingNodeFails(keys, true);
    }

    /**
     * @return Index of node starting transaction.
     */
    protected int originatingNode() {
        return 0;
    }

    /**
     * Ignores messages to given node of given type.
     *
     * @param dstNodeId Destination node ID.
     * @param msgCls Message type.
     */
    protected void ignoreMessages(UUID dstNodeId, Class<?> msgCls) {
        ignoreMsgNodeId = dstNodeId;
        ignoreMsgCls = msgCls;
    }

    /**
     * @param keys Keys to update.
     * @param partial Flag indicating whether to simulate partial prepared state.
     * @throws Exception If failed.
     */
    protected void testTxOriginatingNodeFails(Collection<Integer> keys, final boolean partial) throws Exception {
        assertFalse(keys.isEmpty());

        final Collection<IgniteKernal> grids = new ArrayList<>();

        ClusterNode txNode = grid(originatingNode()).localNode();

        wierdNode = txNode;

        for (int i = 1; i < gridCount(); i++)
            grids.add((IgniteKernal)grid(i));

        final Map<Integer, String> map = new HashMap<>();

        final String initVal = "val";

        for (Integer key : keys) {
            grid(originatingNode()).cache(DEFAULT_CACHE_NAME).put(key, initVal);

            map.put(key, String.valueOf(key));
        }

        Map<Integer, Collection<ClusterNode>> nodeMap = new HashMap<>();

        info("Node being checked: " + grid(1).localNode().id());

        for (Integer key : keys) {
            Collection<ClusterNode> nodes = new ArrayList<>();

            nodes.addAll(grid(1).affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key));
            //Remove primary
            nodes.remove(txNode);

            nodeMap.put(key, nodes);
        }

        info("Starting pessimistic tx " +
                "[values=" + map + ", topVer=" + (grid(1)).context().discovery().topologyVersion() + ']');

        if (partial)
            ignoreMessages(grid(1).localNode().id(), ignoreMessageClass());

        final Ignite txIgniteNode = G.ignite(txNode.id());

        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache<Integer, String> cache = txIgniteNode.cache(DEFAULT_CACHE_NAME);

                assertNotNull(cache);

                TransactionProxyImpl tx = (TransactionProxyImpl)txIgniteNode.transactions().txStart();

                GridNearTxLocal txEx = tx.tx();

                assertTrue(txEx.optimistic());

                cache.putAll(map);

                try {
                    txEx.prepareNearTxLocal().get(3, TimeUnit.SECONDS);
                }
                catch (IgniteFutureTimeoutCheckedException ignored) {
                    info("Failed to wait for prepare future completion: " + partial);
                }

                return null;
            }
        }).get();

        info("Stopping originating node " + txNode);

        G.stop(G.ignite(txNode.id()).name(), true);

        info("Stopped grid, waiting for transactions to complete.");

        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (IgniteKernal g : grids) {
                    GridCacheSharedContext<Object, Object> ctx = g.context().cache().context();

                    int txNum = ctx.tm().idMapSize();

                    if (txNum != 0)
                        return false;
                }

                return true;
            }
        }, 10000);

        assertTrue(txFinished);

        info("Transactions finished.");

        for (Map.Entry<Integer, Collection<ClusterNode>> e : nodeMap.entrySet()) {
            final Integer key = e.getKey();

            final String val = map.get(key);

            assertFalse(e.getValue().isEmpty());

            for (ClusterNode node : e.getValue()) {
                compute(G.ignite(node.id()).cluster().forNode(node)).call(new IgniteCallable<Void>() {
                    /** */
                    @IgniteInstanceResource
                    private Ignite ignite;

                    @Override public Void call() throws Exception {
                        IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

                        assertNotNull(cache);

                        assertEquals(partial ? initVal : val, cache.localPeek(key));

                        return null;
                    }
                });
            }
        }

        for (Map.Entry<Integer, String> e : map.entrySet()) {
            for (Ignite g : G.allGrids()) {
                UUID locNodeId = g.cluster().localNode().id();

                assertEquals("Check failed for node: " + locNodeId, partial ? initVal : e.getValue(),
                        g.cache(DEFAULT_CACHE_NAME).get(e.getKey()));
            }
        }
    }

    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName)
                .setCacheConfiguration(
                        new CacheConfiguration(DEFAULT_CACHE_NAME)
                                .setCacheMode(CacheMode.PARTITIONED)
                                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                                .setBackups(2)
                ).setCommunicationSpi(new TcpCommunicationSpi() {

                    @IgniteInstanceResource
                    IgniteEx igniteEx;

                    @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
                        if ((Objects.equals(wierdNode, igniteEx.localNode()) || Objects.equals(wierdNode.id(), node.id()))
                                && ignoredMessage((GridIoMessage)msg)) {
                            log.info("============================== block message");
//                        try {
//                            U.sleep(7_000);
//                        }
//                        catch (Exception ex) {
//                            throw new IgniteException(ex);
//                        }
                        } else {
                            super.sendMessage(node, msg);
                        }
                    }
                }.setConnectTimeout(5_000)
                        .setMaxConnectTimeout(30_000)
                        .setReconnectCount(0))
                .setFailureDetectionTimeout(30_000)
            /*.setTransactionConfiguration(
                new TransactionConfiguration().setDefaultTxTimeout(5_000)
            )*/;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setCacheStoreFactory(null);
        cfg.setReadThrough(false);
        cfg.setWriteThrough(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected abstract CacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        ignoreMsgCls = null;
        ignoreMsgNodeId = null;
    }

    /**
     * Checks if message should be ignored.
     *
     * @param msg Message.
     * @return {@code True} if message should be ignored.
     */
    private boolean ignoredMessage(GridIoMessage msg) {
        return ignoreMsgCls != null && ignoreMsgCls.isAssignableFrom(msg.message().getClass());
    }
}
