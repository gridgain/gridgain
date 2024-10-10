/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.TX_RECORD;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests for transaction record logging.
 */
public class IgnitePdsTxRecordLoggingTest extends GridCommonAbstractTest {
    /** In-memory data region name. */
    private static final String IN_MEMORY_REGION_NAME = "inMemoryRegion";

    /** In memory cache. */
    private static final String IN_MEM_CACHE_NAME = "tx-in-memory-cache";

    /** Persistent cache name. */
    private static final String PERSISTENT_CACHE_NAME = "tx-persistent-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setConsistentId(instanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalSegmentSize(1024 * 1024);

        dsCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setPersistenceEnabled(false)
            .setMaxSize(1024L * 1024 * 1024)
            .setName(IN_MEMORY_REGION_NAME));

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(
            cacheConfiguration(PERSISTENT_CACHE_NAME, null, 1),
            cacheConfiguration(IN_MEM_CACHE_NAME, IN_MEMORY_REGION_NAME, 1));

        // Plugin that creates a WAL manager that counts replays
        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            /** {@inheritDoc} */
            @Override public String name() {
                return "testPlugin";
            }

            /** {@inheritDoc} */
            @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteWriteAheadLogManager.class.equals(cls))
                    return (T) new TestRecordingWriteAheadLogManager((((IgniteEx)ctx.grid()).context()));

                return null;
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignite crd = startGridsMultiThreaded(2);

        crd.cluster().state(ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /**
     * Creates a new cache configuration with the given name, optional data region name and number of backups.
     *
     * @param cacheName Cache name.
     * @param dataRegionName Optional data region name.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(String cacheName, @Nullable String dataRegionName, int backups) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setBackups(backups);

        if (dataRegionName != null)
            ccfg.setDataRegionName(dataRegionName);

        return ccfg;
    }

    /** Starts recording of TxRecords on all nodes. */
    private void startRecordingTxRecords() {
        for (Ignite n : G.allGrids())
            TestRecordingWriteAheadLogManager.logManager((IgniteEx) n).startRecording();
    }

    /** Stops recording of TxRecords on all nodes. */
    private void stopRecordingTxRecords() {
        for (Ignite n : G.allGrids())
            TestRecordingWriteAheadLogManager.logManager((IgniteEx) n).stopRecording();
    }

    /**
     * Tests that there are no TxRecords written for in-memory data region.
     */
    @Test
    public void testTxRecordsIsNotWrittenForNonPersistentCache() {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);

        IgniteCache<Object, Object> cache = ignite0.cache(IN_MEM_CACHE_NAME);

        startRecordingTxRecords();

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        stopRecordingTxRecords();

        verifyTxRecords(ignite0, 0, null);
        verifyTxRecords(ignite1, 0, null);
    }

    /**
     * Tests that there are no TxRecords written for in-memory data region.
     */
    @Test
    public void testTxRecordsIsWrittenIfTxContainsPersistentCache() {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);

        IgniteCache<Object, Object> cachePersistent = ignite0.cache(PERSISTENT_CACHE_NAME);
        IgniteCache<Object, Object> cacheInMem = ignite0.cache(IN_MEM_CACHE_NAME);

        Integer primaryKeyPersist = primaryKey(cachePersistent);
        Integer primaryKeyInMem = primaryKey(cacheInMem);

        startRecordingTxRecords();

        try (Transaction tx = ignite0.transactions().txStart()) {
            cachePersistent.put(primaryKeyPersist, 1);

            cacheInMem.put(primaryKeyInMem, 2);

            tx.commit();
        }

        stopRecordingTxRecords();

        verifyTxRecords(ignite0, 1, asList(TransactionState.PREPARED, TransactionState.COMMITTED));
        verifyTxRecords(ignite1, 1, asList(TransactionState.PREPARED, TransactionState.COMMITTED));
    }

    /**
     * Tests that there are no TxRecords written for in-memory data region.
     */
    @Test
    public void testTxRecordsRollbackInMemSetup() {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);

        ignite0.cluster().state(ACTIVE);

        IgniteTransactions transactions = ignite0.transactions();
        IgniteCache<Object, Object> cacheInMem = ignite0.cache(IN_MEM_CACHE_NAME);

        startRecordingTxRecords();

        try (Transaction tx = transactions.txStart(PESSIMISTIC, SERIALIZABLE)) {

            cacheInMem.put(2, 2);

            tx.rollback();
        }

        stopRecordingTxRecords();

        verifyTxRecords(ignite0, 0, null);
        verifyTxRecords(ignite1, 0, null);
    }

    /**
     * Tests that there are no TxRecords written for in-memory data region.
     */
    @Test
    public void testTxRecordsRollbackMixedSetup() {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);

        IgniteCache<Object, Object> cachePersistent = ignite0.cache(PERSISTENT_CACHE_NAME);
        IgniteCache<Object, Object> cacheInMem = ignite0.cache(IN_MEM_CACHE_NAME);

        Integer primaryKeyPersist = primaryKey(cachePersistent);
        Integer primaryKeyInMem = primaryKey(cacheInMem);

        startRecordingTxRecords();

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
            cachePersistent.put(primaryKeyPersist, 1);
            cacheInMem.put(primaryKeyInMem, 2);

            tx.rollback();
        }

        stopRecordingTxRecords();

        verifyTxRecords(ignite0, 0, Collections.singletonList(TransactionState.ROLLED_BACK));
        verifyTxRecords(ignite1, 0, null);
    }

    /**
     * Tests that there are no TxRecords and data record written for read only transations.
     */
    @Test
    public void testReadOnlyTx() {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);

        IgniteCache<Object, Object> cachePersistent = ignite0.cache(PERSISTENT_CACHE_NAME);
        IgniteCache<Object, Object> cacheInMem = ignite0.cache(IN_MEM_CACHE_NAME);

        Integer primaryKeyPersist = primaryKey(cachePersistent);
        Integer primaryKeyInMem = primaryKey(cacheInMem);

        cachePersistent.put(primaryKeyPersist, 12);
        cacheInMem.put(primaryKeyInMem, 12);

        startRecordingTxRecords();

        // Read only transaction.
        try (Transaction readOnlyTx = ignite0.transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
            assertEquals(12, cachePersistent.get(primaryKeyPersist));
            assertEquals(12, cacheInMem.get(primaryKeyInMem));

            readOnlyTx.commit();
        }

        stopRecordingTxRecords();

        verifyTxRecords(ignite0, 0, null);
        verifyTxRecords(ignite1, 0, null);
    }

    /**
     * Tests that there are no TxRecords and data record written for read-write transactions,
     * when the transaction does not modify persistent cache participated in the operation.
     */
    @Test
    public void testReadWriteMixedTx() {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);

        IgniteCache<Object, Object> cachePersistent = ignite0.cache(PERSISTENT_CACHE_NAME);
        IgniteCache<Object, Object> cacheInMem = ignite0.cache(IN_MEM_CACHE_NAME);

        Integer primaryKeyPersist = primaryKey(cachePersistent);
        Integer primaryKeyInMem = primaryKey(cacheInMem);

        cachePersistent.put(primaryKeyPersist, 12);
        cacheInMem.put(primaryKeyInMem, 12);

        startRecordingTxRecords();

        // There is write-entry for in-memory cache only.
        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
            assertEquals(12, cachePersistent.get(primaryKeyPersist));

            cacheInMem.put(primaryKeyInMem, 12);

            tx.commit();
        }

        stopRecordingTxRecords();

        verifyTxRecords(ignite0, 0, null);
        verifyTxRecords(ignite1, 0, null);
    }

    /**
     * Tests that there are no TxRecords and data record written for read-write transactions,
     * when the transaction does not modify persistent cache participated in the operation.
     */
    @Test
    public void testReadWriteMixedTxDifferentPrimaries() throws Exception {
        IgniteEx ignite0 = grid(0);
        IgniteEx ignite1 = grid(1);

        String persistentCacneName = PERSISTENT_CACHE_NAME + "-0-backups";
        String inMemoryCacneName = IN_MEM_CACHE_NAME + "-0-backups";

        ignite0.getOrCreateCaches(asList(
            cacheConfiguration(persistentCacneName, null, 0),
            cacheConfiguration(inMemoryCacneName, IN_MEMORY_REGION_NAME, 0)
        ));

        try {
            awaitPartitionMapExchange();

            IgniteCache<Object, Object> cachePersistent = ignite0.cache(persistentCacneName);
            IgniteCache<Object, Object> cacheInMem = ignite0.cache(inMemoryCacneName);

            Integer primaryKeyPersist = primaryKey(cachePersistent);
            Integer primaryKeyInMem = primaryKey(ignite1.cache(inMemoryCacneName));

            startRecordingTxRecords();

            // There are two write-entries.
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, SERIALIZABLE)) {
                cachePersistent.put(primaryKeyPersist, 12);

                cacheInMem.put(primaryKeyInMem, 12);

                tx.commit();
            }

            stopRecordingTxRecords();

            verifyTxRecords(ignite0, 1, asList(TransactionState.PREPARED, TransactionState.COMMITTED));
            verifyTxRecords(ignite1, 0, null);
        }
        finally {
            ignite0.destroyCache(persistentCacneName);
            ignite0.destroyCache(inMemoryCacneName);

            awaitPartitionMapExchange();
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param expDataCnt Expected DataRecord count.
     * @param states Expected TxRecord ordered sequence.
     */
    private void verifyTxRecords(IgniteEx ignite, int expDataCnt, @Nullable List<TransactionState> states) {
        TestRecordingWriteAheadLogManager walMgr = TestRecordingWriteAheadLogManager.logManager(ignite);

        List<TxRecord> txRecords = walMgr.recordedTxRecords(true);
        List<DataRecord> dataRecords = walMgr.recordedDataRecords(true);

        int dataEntries = dataRecords.stream().map(DataRecord::writeEntries).mapToInt(List::size).sum();

        if (states != null) {
            assertEquals(states.size(), txRecords.size());

            int i = 0;

            for (TxRecord record : txRecords)
                assertEquals(states.get(i++), record.state());
        }
        else
            assertEquals(0, txRecords.size());

        assertEquals(expDataCnt, dataEntries);
    }

    /** WAL manager that counts how many times replay has been called. */
    private static class TestRecordingWriteAheadLogManager extends FileWriteAheadLogManager {
        /** Captured xx records. */
        private final List<TxRecord> txRecords = new ArrayList<>();

        /** Captured data records. */
        private final List<DataRecord> dataRecords = new ArrayList<>();

        /** Flag to start/stop recording of wal records. */
        private volatile boolean recordingEnabled;

        /** Constructor. */
        TestRecordingWriteAheadLogManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord rec, RolloverType rolloverType) throws IgniteCheckedException {
            WALPointer ptr = super.log(rec, rolloverType);

            if (recordingEnabled) {
                if (rec.type() == TX_RECORD) {
                    synchronized (this) {
                        txRecords.add((TxRecord) rec);
                    }
                }

                if (rec.type() == DATA_RECORD_V2) {
                    synchronized (this) {
                        dataRecords.add((DataRecord) rec);
                    }
                }
            }

            return ptr;
        }

        /** Starts recording of TxRecords. */
        void startRecording() {
            recordingEnabled = true;
        }

        /** Stops recording of TxRecords. */
        void stopRecording() {
            recordingEnabled = false;
        }

        /**
         * Returns captured tx records.
         *
         * @param cleanRecordedRecords If {@code true} then clears recorded records.
         * @return List of captured tx records.
         */
        List<TxRecord> recordedTxRecords(boolean cleanRecordedRecords) {
            synchronized (this) {
                List<TxRecord> res = new ArrayList<>(txRecords);

                if (cleanRecordedRecords)
                    txRecords.clear();

                return res;
            }
        }

        /**
         * Returns captured data records.
         *
         * @param cleanRecordedRecords If {@code true} then clears recorded records.
         * @return List of captured tx records.
         */
        List<DataRecord> recordedDataRecords(boolean cleanRecordedRecords) {
            synchronized (this) {
                List<DataRecord> res = new ArrayList<>(dataRecords);

                if (cleanRecordedRecords)
                    dataRecords.clear();

                return res;
            }
        }

        static TestRecordingWriteAheadLogManager logManager(IgniteEx ignite) {
            return (TestRecordingWriteAheadLogManager)ignite.context().cache().context().wal();
        }
    }
}
