package org.apache.ignite;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Sdsb12171 extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAffinity(new RendezvousAffinityFunction(false, 16)).
            setBackups(1).
            setCacheMode(CacheMode.REPLICATED).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setGroupName("test_group"));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * 1. Start Node
     * 2. Load data
     * 3. Checkpoint
     * 4. Change hwm
     * 5. Stop node in the midlle of checkpoint
     * 6. Start Node
     *
     * @throws Exception
     */
    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGrid(1);

        ignite.cluster().state(ClusterState.ACTIVE);

        System.out.println("============== load 1");

        for (int i = 0; i < 10_000; i++) {
            ignite.cache(DEFAULT_CACHE_NAME).put(i, i);
        }

//        ignite.cache(DEFAULT_CACHE_NAME).

//        CountDownLatch latch = new CountDownLatch(1);
//
//        ((GridCacheDatabaseSharedManager) ignite.context().cache().context().database())
//            .addCheckpointListener(new CheckpointListener() {
//                                       @Override
//                                       public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
////                                           try {
////                                               multithreadedAsync(new Runnable() {
////                                                   @Override public void run() {
////                                                       IgnitionEx.stopAll(true, ShutdownPolicy.IMMEDIATE);
////                                                       latch.countDown();
////                                                   }}, 1
////                                               );
////                                               Thread.sleep(300);
////                                           } catch (Exception ignore) {}
//                                       }
//
//                                       @Override
//                                       public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
//                                           try {
//                                               multithreadedAsync(new Runnable() {
//                                                   @Override public void run() {
//                                                       IgnitionEx.stopAll(true, ShutdownPolicy.IMMEDIATE);
//                                                       latch.countDown();
//                                                   }}, 1
//                                               );
//                                               Thread.sleep(300);
//                                           } catch (Exception ignore) {}
//                                       }
//
//                                       @Override
//                                       public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
//
//                                       }
//                                   }
//            );
//
//        try {
//            forceCheckpoint();
//        } catch (Exception ignore) {}
//
//        latch.await();

        forceCheckpoint();

//        IgniteInternalCache<Object, Object> cache = ignite.context()
//            .cache()
//            .cache(DEFAULT_CACHE_NAME);
//
        System.out.println("============== load 2");
//        try (Transaction tx = cache.txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE)) {
            for (int i = 0; i < 500; i++) {
                ignite.cache(DEFAULT_CACHE_NAME).put(i, i);
            }
//            tx.commit();
//        }catch (Exception ex) {
//
//        }

//        System.out.println("============== stop caches");
//        ignite.context().cache().stopCaches(true);

        System.out.println("============== update hwm");
        PartitionUpdateCounter counter = counter(1, DEFAULT_CACHE_NAME, ignite.name());
        counter.reserve(1);

//        cache.context().topology().localPartition(1).updateCounter();

//        cache
//            .context()
//            .group()
//            .onPartitionCounterUpdate(
//                cache.context().cacheId(),
//                1, 100, cache.context().affinity().affinityTopologyVersion(), true
//            );

//        ignite.context().cache().context().wal().log(
//            new MetaPageUpdatePartitionDataRecordV2(
//                cache.context().groupId(),
//                1,
//                10,
//                1,
//                (int)1, // TODO: Partition size may be long
//                1,
//                TouchPoint.State.MOVED == null ? -1 : (byte)TouchPoint.State.MOVED.ordinal(),
//                1,
//                1
//            ));

        CountDownLatch latch = new CountDownLatch(1);

        ((GridCacheDatabaseSharedManager) ignite.context().cache().context().database())
            .addCheckpointListener(new CheckpointListener() {
                                       @Override
                                       public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
//                                           try {
//                                               multithreadedAsync(new Runnable() {
//                                                   @Override public void run() {
//                                                       IgnitionEx.stopAll(true, ShutdownPolicy.IMMEDIATE);
//                                                       latch.countDown();
//                                                   }}, 1
//                                               );
//                                               Thread.sleep(300);
//                                           } catch (Exception ignore) {}
                                       }

                                       @Override
                                       public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                                           try {
                                               multithreadedAsync(new Runnable() {
                                                   @Override public void run() {
                                                       IgnitionEx.stopAll(true, ShutdownPolicy.IMMEDIATE);
                                                       latch.countDown();
                                                   }}, 1
                                               );
                                               Thread.sleep(300);
                                           } catch (Exception ignore) {}
                                       }

                                       @Override
                                       public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {

                                       }
                                   }
            );

        try {
            forceCheckpoint();
        } catch (Exception ignore) {}

        latch.await();

        ignite = startGrid(1);

        ignite.cluster().state(ClusterState.ACTIVE);

        forceCheckpoint();
    }
}
