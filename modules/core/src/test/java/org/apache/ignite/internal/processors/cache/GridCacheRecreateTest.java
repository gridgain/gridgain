package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 */
public class GridCacheRecreateTest extends GridCommonAbstractTest {
    private volatile boolean usePersistence;

    @Override
    protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(600_000);
        cfg.setClientFailureDetectionTimeout(600_000);
        cfg.setMetricsLogFrequency(15_000);

        if (usePersistence) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

            cfg.setCacheConfiguration(null);
        }

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    @Test
    public void testA() throws Exception {
        IgniteEx crd = startGrids(1);

        IgniteEx client = startClientGrid(1);

        IgniteCache clientCache = createCache(client);

        CountDownLatch exchangeLatch1 = new CountDownLatch(2);
        CountDownLatch exchangeLatch2 = new CountDownLatch(1);

//        crd.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
//            @Override
//            public void onInitBeforeTopologyLockBefore(GridDhtPartitionsExchangeFuture fut) {
//                PartitionsExchangeAware.super.onInitBeforeTopologyLock(fut);
//
//                exchangeLatch1.countDown();
//
//                try {
//                    exchangeLatch2.await();
//                }
//                catch (Exception e) {
//                    e.printStackTrace();
//                    throw new RuntimeException(e);
//                }
//            }
//        });
//        client.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
//            @Override
//            public void onInitBeforeTopologyLockBefore(GridDhtPartitionsExchangeFuture fut) {
//                PartitionsExchangeAware.super.onInitBeforeTopologyLock(fut);
//
//                exchangeLatch1.countDown();
//
//                try {
//                    exchangeLatch2.await();
//                }
//                catch (Exception e) {
//                    e.printStackTrace();
//                    throw new RuntimeException(e);
//                }
//            }
//        });

        IgniteInternalFuture destFut = GridTestUtils.runAsync(() -> {
            client.destroyCache(clientCache.getName());
        });

        exchangeLatch1.await();

        CountDownLatch atomicFutLatch1 = new CountDownLatch(1);
        CountDownLatch atomicFutLatch2 = new CountDownLatch(1);
//        crd.context().cache().context().mvcc().onAtomicFutAddedAction = (fut) -> {
//            atomicFutLatch1.countDown();
//            try {
//                atomicFutLatch2.await();
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
//        };


        IgniteInternalFuture updFut = GridTestUtils.runAsync(() -> {
            clientCache.put(12, 42);
            log.warning(">>>>> update successfully completed!");
        });

        long timeout = 10_000;

        log.warning(">>>>> suspending cache update");
        atomicFutLatch1.await();

        log.warning(">>>>> starting to sleep...");
        doSleep(timeout);

        log.warning(">>>>> unblocking exchange...");
        exchangeLatch2.countDown();
        doSleep(timeout);

        log.warning(">>>>> resuming cache update");
        atomicFutLatch2.countDown();

        destFut.get();
        updFut.get();
    }

    @Test
    public void testAA() throws Exception {
        IgniteEx crd = startGrids(1);

        IgniteEx client = startClientGrid(1);

        IgniteCache clientCache = createCache(client);

        CountDownLatch exchangeLatch1 = new CountDownLatch(2);
        CountDownLatch exchangeLatch2 = new CountDownLatch(1);

//        crd.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
//            @Override
//            public void onInitBeforeTopologyLockBefore(GridDhtPartitionsExchangeFuture fut) {
//                PartitionsExchangeAware.super.onInitBeforeTopologyLock(fut);
//
//                exchangeLatch1.countDown();
//
//                try {
//                    exchangeLatch2.await();
//                }
//                catch (Exception e) {
//                    e.printStackTrace();
//                    throw new RuntimeException(e);
//                }
//            }
//        });
//        client.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
//            @Override
//            public void onInitBeforeTopologyLockBefore(GridDhtPartitionsExchangeFuture fut) {
//                PartitionsExchangeAware.super.onInitBeforeTopologyLock(fut);
//
//                exchangeLatch1.countDown();
//
//                try {
//                    exchangeLatch2.await();
//                }
//                catch (Exception e) {
//                    e.printStackTrace();
//                    throw new RuntimeException(e);
//                }
//            }
//        });

        IgniteInternalFuture destFut = GridTestUtils.runAsync(() -> {
            client.destroyCache(clientCache.getName());
        });

        exchangeLatch1.await();

        CountDownLatch atomicFutLatch1 = new CountDownLatch(1);
        CountDownLatch atomicFutLatch2 = new CountDownLatch(1);
//        crd.context().cache().context().mvcc().onAtomicFutAddedAction = (fut) -> {
//            atomicFutLatch1.countDown();
//            try {
//                atomicFutLatch2.await();
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
//        };

        IgniteInternalFuture updFut = GridTestUtils.runAsync(() -> {
            clientCache.put(12, 42);
            log.warning(">>>>> update successfully completed!");
        });

        long timeout = 10_000;

        log.warning(">>>>> suspending cache update");
        atomicFutLatch1.await();

        log.warning(">>>>> starting to sleep...");
        doSleep(timeout);

        log.warning(">>>>> unblocking exchange...");
        exchangeLatch2.countDown();
        doSleep(timeout);

        log.warning(">>>>> resuming cache update");
        atomicFutLatch2.countDown();

        destFut.get();
        updFut.get();
    }

    @Test
    public void testC() throws Exception {
        IgniteEx crd = startGrids(1);

        IgniteEx client = startClientGrid(1);

        IgniteCache clientCache = createCache(client);

        CountDownLatch latch = new CountDownLatch(1);

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        clientSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridNearAtomicAbstractUpdateRequest) {
                latch.countDown();
                return true;
            }

            return false;
        });

        IgniteInternalFuture<?> ipdFut = GridTestUtils.runAsync(() -> {
            try {
                clientCache.put(12, 12);
            }
            catch (Throwable t) {
                log.warning(">>>>> exception t = " + t);
                t.printStackTrace();
            }
        });

        clientSpi.waitForBlocked();

        crd.destroyCache(clientCache.getName());
        IgniteCache crdCache = createCache(crd);

        clientSpi.stopBlock();

        try {
            ipdFut.get();
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Test
    public void testCServers() throws Exception {
        IgniteEx crd = startGrids(1);

        IgniteEx client = startGrid(1);

        IgniteCache clientCache = createCache(client);

        awaitPartitionMapExchange();

        log.warning("---------------------------- starting the test ------------------------------");

        CountDownLatch latch = new CountDownLatch(1);

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        clientSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridNearAtomicAbstractUpdateRequest) {
                GridNearAtomicAbstractUpdateRequest msg0 = (GridNearAtomicAbstractUpdateRequest)msg;
                if (msg0.cacheId() == CU.cacheId(clientCache.getName())) {
                    log.warning(">>>>> blocking cache update message! [msg=" + msg0 + ']');
                    latch.countDown();
                    return true;
                }
            }

            return false;
        });

        Integer primaryKey = primaryKey(crd.cache(clientCache.getName()));

        IgniteInternalFuture<?> ipdFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting update...");
                clientCache.put(primaryKey, 12);
            }
            catch (Throwable t) {
                log.warning(">>>>> exception t = " + t);
                t.printStackTrace();
            }
        });

        log.warning(">>>>> waiting for the update");
        clientSpi.waitForBlocked();
        log.warning(">>>>> the update is blocked!");

        log.warning(">>>>> start destroing the cache");
        crd.destroyCache(clientCache.getName());
        log.warning(">>>>> the cache was destroyed");

        IgniteInternalFuture<?> recreateFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting a new cache async!");
                IgniteCache crdCache = createCache(crd);
                log.warning(">>>>> a new cache successfully started!");
            }
            catch (Throwable t) {
                throw new RuntimeException("creating failed...", t);
            }
        });

        doSleep(12_000);

        log.warning(">>>>> unblock cache update");
        clientSpi.stopBlock();

        try {
            log.warning(">>>>> waiting for the update");
            ipdFut.get();
        }
        catch (Throwable t) {
            log.warning(">>>>> expected excheption!" + t);
            t.printStackTrace();
        }

        recreateFut.get();
    }

    @Test
    public void testCServersRead() throws Exception {
        IgniteEx crd = startGrids(1);

        IgniteEx client = startGrid(1);

        IgniteCache clientCache = createCache(client);

        Integer primaryKey = primaryKey(crd.cache(clientCache.getName()));
       // clientCache.put(primaryKey, 12);

        awaitPartitionMapExchange();

        log.warning("---------------------------- starting the test ------------------------------");

        CountDownLatch latch = new CountDownLatch(1);

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        clientSpi.blockMessages((node, msg) -> {
            log.warning(">>>>> msg all [msg=" + msg + ']');

            if (msg instanceof GridNearSingleGetRequest) {
                GridNearSingleGetRequest msg0 = (GridNearSingleGetRequest)msg;
                if (msg0.cacheId() == CU.cacheId(clientCache.getName())) {
                    log.warning(">>>>> blocking cache update message! [msg=" + msg0 + ']');
                    latch.countDown();
                    return true;
                }
            }

            return false;
        });

        IgniteInternalFuture<?> ipdFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting reading...");
                clientCache.get(primaryKey);
            }
            catch (Throwable t) {
                log.warning(">>>>> exception t = " + t);
                t.printStackTrace();
            }
        });

        log.warning(">>>>> waiting for the update");
        clientSpi.waitForBlocked();
        log.warning(">>>>> the update is blocked!");

        log.warning(">>>>> start destroing the cache");
        crd.destroyCache(clientCache.getName());
        log.warning(">>>>> the cache was destroyed");

        IgniteInternalFuture<?> recreateFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting a new cache async!");
                IgniteCache crdCache = createCache(crd);
                log.warning(">>>>> a new cache successfully started!");
            }
            catch (Throwable t) {
                throw new RuntimeException("creating failed...", t);
            }
        });

        doSleep(12_000);

        log.warning(">>>>> unblock cache update");
        clientSpi.stopBlock();

        try {
            log.warning(">>>>> waiting for the update");
            ipdFut.get();
        }
        catch (Throwable t) {
            log.warning(">>>>> expected excheption!" + t);
            t.printStackTrace();
        }

        recreateFut.get();
    }

    @Test
    public void testCServersInvoke() throws Exception {
        IgniteEx crd = startGrids(1);

        IgniteEx client = startGrid(1);

        IgniteCache clientCache = createCache(client);

        Integer primaryKey = primaryKey(crd.cache(clientCache.getName()));
        // clientCache.put(primaryKey, 12);

        awaitPartitionMapExchange();

        log.warning("---------------------------- starting the test ------------------------------");

        CountDownLatch latch = new CountDownLatch(1);

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        clientSpi.blockMessages((node, msg) -> {
            log.warning(">>>>> msg all [msg=" + msg + ']');

            if (msg instanceof GridNearSingleGetRequest) {
                GridNearSingleGetRequest msg0 = (GridNearSingleGetRequest)msg;
                if (msg0.cacheId() == CU.cacheId(clientCache.getName())) {
                    log.warning(">>>>> blocking cache update message! [msg=" + msg0 + ']');
                    latch.countDown();
                    return true;
                }
            }

            return false;
        });

        IgniteInternalFuture<?> ipdFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting reading...");
                clientCache.invoke(primaryKey, new CacheEntryProcessor() {
                    @Override
                    public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
                        entry.setValue(42);
                        return null;
                    }
                }, null);
            }
            catch (Throwable t) {
                log.warning(">>>>> exception t = " + t);
                t.printStackTrace();
            }
        });

        log.warning(">>>>> waiting for the update");
        clientSpi.waitForBlocked();
        log.warning(">>>>> the update is blocked!");

        log.warning(">>>>> start destroing the cache");
        crd.destroyCache(clientCache.getName());
        log.warning(">>>>> the cache was destroyed");

        IgniteInternalFuture<?> recreateFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting a new cache async!");
                IgniteCache crdCache = createCache(crd);
                log.warning(">>>>> a new cache successfully started!");
            }
            catch (Throwable t) {
                throw new RuntimeException("creating failed...", t);
            }
        });

        doSleep(12_000);

        log.warning(">>>>> unblock cache update");
        clientSpi.stopBlock();

        try {
            log.warning(">>>>> waiting for the update");
            ipdFut.get();
        }
        catch (Throwable t) {
            log.warning(">>>>> expected excheption!" + t);
            t.printStackTrace();
        }

        recreateFut.get();
    }

    @Test
    public void testCServersTx() throws Exception {
        IgniteEx crd = startGrids(1);

        IgniteEx client = startGrid(1);

        IgniteCache clientCache = createCacheTx(client);

        awaitPartitionMapExchange();

        log.warning("---------------------------- starting the test ------------------------------");

        CountDownLatch latch = new CountDownLatch(1);

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        clientSpi.blockMessages((node, msg) -> {
            log.warning(">>>>> msg = [" + msg + ']');
            if (msg instanceof GridNearTxPrepareRequest) {
//                GridNearAtomicAbstractUpdateRequest msg0 = (GridNearAtomicAbstractUpdateRequest)msg;
//                if (msg0.cacheId() == CU.cacheId(clientCache.getName())) {
//                    log.warning(">>>>> blocking cache update message! [msg=" + msg0 + ']');
//                    latch.countDown();
                    return true;
//                }
            }

            if (msg instanceof GridNearLockRequest) {
                return true;
            }

            return false;
        });

        Integer primaryKey = primaryKey(crd.cache(clientCache.getName()));

        IgniteInternalFuture<?> ipdFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting update...");

                // 1. implicit tx
                // clientCache.put(primaryKey, 12);

                // 2. explicit
                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
                    clientCache.put(primaryKey, 12);

                    tx.commit();
                }
            }
            catch (Throwable t) {
                log.warning(">>>>> exception t = " + t);
                t.printStackTrace();
            }
        });

        log.warning(">>>>> waiting for the update");
        clientSpi.waitForBlocked();
        log.warning(">>>>> the update is blocked!");

        log.warning(">>>>> start destroing the cache");
        crd.destroyCache(clientCache.getName());
        log.warning(">>>>> the cache was destroyed");

        IgniteInternalFuture<?> recreateFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting a new cache async!");
                IgniteCache crdCache = createCache(crd);
                log.warning(">>>>> a new cache successfully started!");
            }
            catch (Throwable t) {
                throw new RuntimeException("creating failed...", t);
            }
        });

        doSleep(12_000);

        log.warning(">>>>> unblock cache update");
        clientSpi.stopBlock();

        try {
            log.warning(">>>>> waiting for the update");
            ipdFut.get();
        }
        catch (Throwable t) {
            log.warning(">>>>> expected excheption!" + t);
            t.printStackTrace();
        }

        recreateFut.get();
    }

    @Test
    public void testCServersTxClient() throws Exception {
        IgniteEx crd = startGrids(1);

        IgniteEx client = startClientGrid(1);

        IgniteCache clientCache = createCacheTx(client);

        awaitPartitionMapExchange();

        log.warning("---------------------------- starting the test ------------------------------");

        CountDownLatch latch = new CountDownLatch(1);

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        clientSpi.blockMessages((node, msg) -> {
            log.warning(">>>>> msg = [" + msg + ']');
            if (msg instanceof GridNearTxPrepareRequest) {
//                GridNearAtomicAbstractUpdateRequest msg0 = (GridNearAtomicAbstractUpdateRequest)msg;
//                if (msg0.cacheId() == CU.cacheId(clientCache.getName())) {
//                    log.warning(">>>>> blocking cache update message! [msg=" + msg0 + ']');
//                    latch.countDown();
                return true;
//                }
            }

            if (msg instanceof GridNearLockRequest) {
                return true;
            }

            return false;
        });

        Integer primaryKey = primaryKey(crd.cache(clientCache.getName()));

        IgniteInternalFuture<?> ipdFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting update...");

                // 1. implicit tx
                 clientCache.put(primaryKey, 12);

                // 2. explicit
//                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
//                    clientCache.put(primaryKey, 12);
//
//                    tx.commit();
//                }
            }
            catch (Throwable t) {
                log.warning(">>>>> exception t = " + t);
                t.printStackTrace();
            }
        });

        log.warning(">>>>> waiting for the update");
        clientSpi.waitForBlocked();
        log.warning(">>>>> the update is blocked!");

        log.warning(">>>>> start destroing the cache");
        crd.destroyCache(clientCache.getName());
        log.warning(">>>>> the cache was destroyed");

        IgniteInternalFuture<?> recreateFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting a new cache async!");
                IgniteCache crdCache = createCacheTx(crd);
                log.warning(">>>>> a new cache successfully started!");
            }
            catch (Throwable t) {
                throw new RuntimeException("creating failed...", t);
            }
        });

        doSleep(12_000);

        log.warning(">>>>> unblock cache update");
        clientSpi.stopBlock();

        try {
            log.warning(">>>>> waiting for the update");
            ipdFut.get();
        }
        catch (Throwable t) {
            log.warning(">>>>> expected excheption!" + t);
            t.printStackTrace();
        }

        recreateFut.get();
    }

    @Test
    public void testCServers2() throws Exception {
        IgniteEx crd = startGrids(1);

        IgniteCache initialCache = createCache(crd, "test-atomic-cache-1", "atomic-group");

        IgniteEx client = startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache clientCache = createCache(client, "test-atomic-cache-2", "atomic-group");

        awaitPartitionMapExchange();

        log.warning("---------------------------- starting the test ------------------------------");

        CountDownLatch latch = new CountDownLatch(1);

        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        clientSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridNearAtomicAbstractUpdateRequest) {
                GridNearAtomicAbstractUpdateRequest msg0 = (GridNearAtomicAbstractUpdateRequest)msg;
                log.warning(">>>>> cacheMsg [id=" + msg0.cacheId() + ", cacheId=" + CU.cacheId(clientCache.getName()) + ", grpId=" + CU.cacheGroupId(clientCache.getName(), "atomic-group") + ']');
                if (msg0.cacheId() == CU.cacheId(clientCache.getName())) {
                    log.warning(">>>>> blocking cache update message! [msg=" + msg0 + ']');
                    latch.countDown();
                    return true;
                }
            }

            return false;
        });

        Integer primaryKey = primaryKey(crd.cache(clientCache.getName()));

        IgniteInternalFuture<?> ipdFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting update...");
                clientCache.put(primaryKey, 12);
            }
            catch (Throwable t) {
                log.warning(">>>>> exception t = " + t);
                t.printStackTrace();
            }
        });

        log.warning(">>>>> waiting for the update");
        clientSpi.waitForBlocked();
        log.warning(">>>>> the update is blocked!");

        log.warning(">>>>> start destroing the cache");
        crd.destroyCache(clientCache.getName());
        log.warning(">>>>> the cache was destroyed");

        IgniteInternalFuture<?> recreateFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting a new cache async!");
                IgniteCache crdCache = createCache(crd, "test-atomic-cache-2", "atomic-group");
                log.warning(">>>>> a new cache successfully started!");
            }
            catch (Throwable t) {
                throw new RuntimeException("creating failed...", t);
            }
        });

        doSleep(12_000);

        log.warning(">>>>> unblock cache update");
        clientSpi.stopBlock();

        try {
            log.warning(">>>>> waiting for the update");
            ipdFut.get();
        }
        catch (Throwable t) {
            log.warning(">>>>> expected excheption!" + t);
            t.printStackTrace();
        }

        recreateFut.get();
    }

    @Test
    public void testCServers3() throws Exception {
        usePersistence = true;

        IgniteEx crd = startGrids(1);

        IgniteEx client = startGrid(1);

        crd.cluster().state(ACTIVE);

        IgniteCache initialCache = createCache(crd, "test-atomic-cache-1", "atomic-group");

        awaitPartitionMapExchange();

        IgniteCache clientCache = createCache(client, "test-atomic-cache-2", "atomic-group");

        awaitPartitionMapExchange();

        log.warning("---------------------------- starting the test ------------------------------");

        CountDownLatch latch = new CountDownLatch(1);

        AtomicBoolean firstMsg = new AtomicBoolean();
        TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
        clientSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridNearAtomicAbstractUpdateRequest) {
                GridNearAtomicAbstractUpdateRequest msg0 = (GridNearAtomicAbstractUpdateRequest)msg;
                log.warning(">>>>> cacheMsg [id=" + msg0.cacheId() + ", cacheId=" + CU.cacheId(clientCache.getName()) + ", grpId=" + CU.cacheGroupId(clientCache.getName(), "atomic-group") + ']');
                if (msg0.cacheId() == CU.cacheId(clientCache.getName())) {
                    log.warning(">>>>> blocking cache update message! [msg=" + msg0 + ']');
                    latch.countDown();
                    return firstMsg.compareAndSet(false, true);
                }
            }

            if (msg instanceof GridDhtPartitionSupplyMessageV2) {
                GridDhtPartitionSupplyMessageV2 msg0 = (GridDhtPartitionSupplyMessageV2) msg;

                return msg0.groupId() == CU.cacheGroupId(clientCache.getName(), "atomic-group");
            }

            return false;
        });

        Integer primaryKey = primaryKey(crd.cache(clientCache.getName()));

        IgniteInternalFuture<?> ipdFut = GridTestUtils.runAsync(() -> {
            try {
                log.warning(">>>>> starting update...");
                clientCache.put(primaryKey, 42);
                log.warning(">>>>> reading update... value = " + clientCache.get(primaryKey));
            }
            catch (Throwable t) {
                log.warning(">>>>> exception t = " + t);
                t.printStackTrace();
            }
        });

        log.warning(">>>>> waiting for the update");
        clientSpi.waitForBlocked();
        log.warning(">>>>> the update is blocked!");

        {
            stopGrid(0);
            awaitPartitionMapExchange();
            for (int i = 0; i < 12; i++)
                clientCache.put(primaryKey, i);

            doSleep(5_000);
            crd = startGrid(0);
        }
//
//        log.warning(">>>>> start destroing the cache");
//        crd.destroyCache(clientCache.getName());
//        log.warning(">>>>> the cache was destroyed");
//
//        IgniteInternalFuture<?> recreateFut = GridTestUtils.runAsync(() -> {
//            try {
//                log.warning(">>>>> starting a new cache async!");
//                IgniteCache crdCache = createCache(crd, "test-atomic-cache-2", "atomic-group");
//                log.warning(">>>>> a new cache successfully started!");
//            }
//            catch (Throwable t) {
//                throw new RuntimeException("creating failed...", t);
//            }
//        });
//
//        doSleep(12_000);

        log.warning(">>>>> unblock cache update");
        clientSpi.stopBlock(true, desc -> {
            Message msg0 = desc.ioMessage().message();
            return msg0 instanceof GridNearAtomicAbstractUpdateRequest;
        }, false, true);

        try {
            log.warning(">>>>> waiting for the update");
            ipdFut.get();
        }
        catch (Throwable t) {
            log.warning(">>>>> expected excheption!" + t);
            t.printStackTrace();
        }

//        recreateFut.get();
    }

    @Test
    public void testServersCacheGroup() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteCache c1 = createCache(crd, "test-atomic-1", "atomic-group");

        IgniteEx g2 = startGrid(1);

        IgniteCache c2 = createCache(crd, "test-atomic-2", "atomic-group");

        awaitPartitionMapExchange();

        DynamicCacheDescriptor desc1 = crd.context().cache().cacheDescriptor("test-atomic-1");
        DynamicCacheDescriptor desc2 = crd.context().cache().cacheDescriptor("test-atomic-2");

        DynamicCacheDescriptor desc11 = g2.context().cache().cacheDescriptor("test-atomic-1");
        DynamicCacheDescriptor desc22 = g2.context().cache().cacheDescriptor("test-atomic-2");

        log.warning(">>>>> crd [stratTopVer1=" + desc1.startTopologyVersion() + ", startTopVer2=" + desc2.startTopologyVersion() + ']');
        log.warning(">>>>> g2  [stratTopVer1=" + desc11.startTopologyVersion() + ", startTopVer2=" + desc22.startTopologyVersion() + ']');

        GridCacheContext ctx1 = crd.context().cache().context().cacheContext(CU.cacheId("test-atomic-1"));
        GridCacheContext ctx2 = crd.context().cache().context().cacheContext(CU.cacheId("test-atomic-2"));

        GridCacheContext ctx11 = g2.context().cache().context().cacheContext(CU.cacheId("test-atomic-1"));
        GridCacheContext ctx22 = g2.context().cache().context().cacheContext(CU.cacheId("test-atomic-2"));

        log.warning(">>>>> crd [CTX stratTopVer1=" + ctx1.startTopologyVersion() + ", startTopVer2=" + ctx2.startTopologyVersion() + ']');
        log.warning(">>>>> crd [CTX stratTopVer1=" + ctx11.startTopologyVersion() + ", startTopVer2=" + ctx22.startTopologyVersion() + ']');
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRecreateWithPutAll() throws Exception {
        startGrids(2);

        //doSleep(120_000);

        IgniteEx client = startClientGrid(2);
//        IgniteEx client = grid(1);

        createCache(client);

        GridTestUtils.runAsync(() -> {
            while (true) {
                try {
//                    Thread.sleep(100);
//
                    putAll(client);
                } catch (Exception e) {
                    // do nothing
                }
            }
        });

        while (true) {
            try {
                Thread.sleep(500);

                destroyCache(client);

                Thread.sleep(500);

                createCache(client);
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    private static IgniteCache createCache(IgniteEx ignite) {
        System.out.println("Cache create started!");

        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>()
            .setName("PDCX_StaticData-93737")
            .setBackups(1)
            .setReadFromBackup(false)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        IgniteCache cache = ignite.getOrCreateCache(cfg);

        System.out.println("Cache create finished!");

        return cache;
    }

    private static IgniteCache createCacheTx(IgniteEx ignite) {
        System.out.println("Cache create started!");

        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>()
            .setName("PDCX_StaticData-93737")
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        IgniteCache cache = ignite.getOrCreateCache(cfg);

        System.out.println("Cache create finished!");

        return cache;
    }

    private static IgniteCache createCache(IgniteEx ignite, String cacheName, String cacheGroup) {
        System.out.println("Cache create started!");

        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>()
            .setName(cacheName)
            .setBackups(1)
            .setGroupName(cacheGroup)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        IgniteCache cache = ignite.getOrCreateCache(cfg);

        System.out.println("Cache create finished!");

        return cache;
    }

    private static void putAll(IgniteEx ignite) {
//        System.out.println("putAll started!");

        IgniteCache<Object, Object> cache = ignite.cache("PDCX_StaticData-93737");

//        if (cache == null)
//            System.out.println("Cache doesn't exist!");

        HashMap<Integer, Integer> map = new LinkedHashMap<>();

        for (int i = 0; i < 10_0000; i++) {
            map.put(ThreadLocalRandom.current().nextInt(1_000_000), ThreadLocalRandom.current().nextInt());
        }

        cache.putAll(map);

//        System.out.println("putAll done!");
    }

    private static void removeAll(IgniteEx ignite) {
        //System.out.println("removeAll started!");

        IgniteCache<Object, Object> cache = ignite.cache("PDCX_StaticData-93737");

        if (cache == null)
            System.out.println("Cache doesn't exist!");

        HashSet<Integer> set = new HashSet<>();

        for (int i = 0; i < 1_000; i++) {
            set.add(ThreadLocalRandom.current().nextInt(1_000_000));
        }

        cache.removeAll(set);

        //System.out.println("removeAll done!");
    }

    private static void destroyCache(IgniteEx ignite) {
        System.out.println("Cache destroy started!");

        ignite.destroyCache("PDCX_StaticData-93737");

        System.out.println("Cache destroy finished!");
    }
}
