package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

public class IgniteTxExceptionNodeFailTest extends GridCommonAbstractTest {

    private SpecialSpi spi0;

    private SpecialSpi spi1;

    protected int gridCount() {
        return 2;
    }

    private static CacheWriteSynchronizationMode syncMode;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsConfig = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true));

        SpecialSpi spi = new SpecialSpi();

        cfg.setCommunicationSpi(spi);

        if (igniteInstanceName.contains("0"))
            spi0 = spi;

        if (igniteInstanceName.contains("1"))
            spi1 = spi;

        return cfg
            .setDataStorageConfiguration(dsConfig)
            .setCacheConfiguration(new CacheConfiguration("cache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(syncMode).setBackups(0));
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        FileUtils.deleteDirectory(new File(U.defaultWorkDirectory()));
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    @Test
    public void testNodeFailWithPrimarySync() throws Exception {
        testNodeFail(PRIMARY_SYNC);
    }

    @Test
    public void testNodeFailWithFullSync() throws Exception {
        testNodeFail(FULL_SYNC);
    }

    public void testNodeFail(CacheWriteSynchronizationMode testSyncMode) throws Exception {
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

            spi0.needToFail(true);

            GridTestUtils.assertThrows(null,
                tx::commit,
                TransactionHeuristicException.class,
                "Primary node [" + grid0.localNode().id() + "] has left the grid and there are no backup nodes");
        }
    }

    static class SpecialSpi extends TestRecordingCommunicationSpi {

        private volatile boolean needToFail;

        /**
         * {@inheritDoc}
         */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (needToFail) {
                if (msg instanceof GridIoMessage) {
                    Message message = ((GridIoMessage)msg).message();
                    if (message instanceof GridNearTxFinishResponse) {
                        blockMessages((node1, msg1) -> true);
                        new Thread(
                            new Runnable() {
                                @Override public void run() {
                                    ignite.log().info("Stopping node: [" + ignite.name() + "]");

                                    IgnitionEx.stop(ignite.name(), true, null, true);
                                }
                            },
                            "node-stopper"
                        ).start();
                    }
                }
            }
            super.sendMessage(node, msg, ackC);
        }

        public void needToFail(boolean needToFail) {
            this.needToFail = needToFail;
        }
    }

}
