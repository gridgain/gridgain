package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

public class Sdsb12170 extends GridCommonAbstractTest {

    private ClusterNode wierdNode;

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
                .setReconnectCount(1))
            .setFailureDetectionTimeout(30_000)
            /*.setTransactionConfiguration(
                new TransactionConfiguration().setDefaultTxTimeout(5_000)
            )*/;
    }

    @Test
    public void test() throws Exception {
        final IgniteEx igniteEx = startGrids(5);

        final ClusterNode txNode = grid(2).localNode();
        wierdNode = txNode;

        final IgniteCache<Object, Object> cache = grid(2).cache(DEFAULT_CACHE_NAME);

        Collection<Integer> keys = new ArrayList<>(10_000);

        for (int i = 0; i < 10_000; i++)
            keys.add(i);

        final String initVal = "initialValue";

        final Map<Integer, String> map = new HashMap<>();

        for (Integer key : keys) {
            grid(2).cache(DEFAULT_CACHE_NAME).put(key, initVal);

            map.put(key, String.valueOf(key));
        }

//        for (int i = 0; i < 10_000; i++) {
//            try (final Transaction tx = igniteEx.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
//                cache.put(i, i);
//
//                tx.commit();
//            }
//        }

        final IgniteEx txIgniteNode = grid(2);

        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache<Integer, String> cache = txIgniteNode.cache(DEFAULT_CACHE_NAME);

                assertNotNull(cache);

                TransactionProxyImpl tx = (TransactionProxyImpl)txIgniteNode.transactions()
                    .txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);

                GridNearTxLocal txEx = tx.tx();

//                assertTrue(txEx.optimistic());

                cache.putAll(map);

//                try {
//                    txEx.prepareNearTxLocal().get(3, TimeUnit.SECONDS);
//                }
//                catch (IgniteFutureTimeoutCheckedException ignored) {
//                    info("Failed to wait for prepare future completion: ");
//                }

                tx.commit();

                return null;
            }
        }).get();

        final Collection<IgniteKernal> grids = new ArrayList<>();

        for (int i = 1; i < 5; i++)
            grids.add((IgniteKernal)grid(i));

//        info("Stopping originating node " + txNode);
//
//        G.stop(G.ignite(txNode.id()).name(), true);
//
//        info("Stopped grid, waiting for transactions to complete.");

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
        }, 20000);

//        assertTrue(txFinished);

        info("Transactions finished.");

        Map<Integer, Collection<ClusterNode>> nodeMap = new HashMap<>();

        for (Integer key : keys) {
            Collection<ClusterNode> nodes = new ArrayList<>();

            nodes.addAll(grid(1).affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key));

            nodes.remove(txNode);

            nodeMap.put(key, nodes);
        }

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

                        return null;
                    }
                });
            }
        }

        for (Map.Entry<Integer, String> e : map.entrySet()) {
            for (Ignite g : G.allGrids()) {
                UUID locNodeId = g.cluster().localNode().id();

                assertEquals("Check failed for node: " + locNodeId, initVal,
                    g.cache(DEFAULT_CACHE_NAME).get(e.getKey()));
            }
        }

//        Thread.currentThread().join();
    }

    private boolean ignoredMessage(GridIoMessage msg) {
        return GridDistributedTxPrepareRequest.class != null && GridDistributedTxPrepareRequest.class.isAssignableFrom(msg.message().getClass());
    }

    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    @Override public void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }
}
