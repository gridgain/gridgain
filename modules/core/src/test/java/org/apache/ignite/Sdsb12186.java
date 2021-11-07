package org.apache.ignite;

import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Sdsb12186 extends GridCommonAbstractTest {

    @Override public IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName)
            .setCacheConfiguration(
                new CacheConfiguration(DEFAULT_CACHE_NAME)
                    .setBackups(1)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setCacheMode(CacheMode.PARTITIONED)
            ).setCommunicationSpi(
                new TcpCommunicationSpi() {
                    @Override public void sendMessage(ClusterNode node, Message msg,
                        IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
                        final Message message = ((GridIoMessage)msg).message();
                        System.out.println(String.format("====================== %s", message.getClass().getName()));
                        if (message != null && /*GridNearTxPrepareRequest*/GridDhtTxPrepareRequest.class.isAssignableFrom(message.getClass())) {
                            try {
                                Thread.sleep(30_000);
                            } catch (InterruptedException ex) {

                            }
                        }
                        super.sendMessage(node, msg, ackC);
                    }
                }
            );
    }

    @Test
    public void test() throws Exception {
        final IgniteEx ign = startGrids(3);

        Set<Integer> pkeys = new TreeSet<>();
        try (final IgniteDataStreamer<Object, Object> streamer = ign.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100; i++) {
                streamer.addData(i, i);
                if (grid(1).affinity(DEFAULT_CACHE_NAME).isPrimary(grid(1).localNode(), i))
                    pkeys.add(i);
            }
        }

        for (Integer pkey : pkeys)
            grid(1).cache(DEFAULT_CACHE_NAME).removeAsync(pkey);
//        grid(1).cache(DEFAULT_CACHE_NAME).removeAllAsync(pkeys);

        grid(1).destroyCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();
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
