/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry.GroupStateLazyStore;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointMarkersStorage;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CHECKPOINT_MAP_SNAPSHOT_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PREFER_WAL_REBALANCE;
import static org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCheckpointMapSnapshotTest.SnapshotAction.CLEAR_FILE;
import static org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCheckpointMapSnapshotTest.SnapshotAction.KEEP;
import static org.apache.ignite.internal.processors.cache.persistence.IgnitePdsCheckpointMapSnapshotTest.SnapshotAction.REMOVE;

/**
 * Tests checkpoint map snapshot.
 */
@WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
@WithSystemProperty(key = IGNITE_CHECKPOINT_MAP_SNAPSHOT_THRESHOLD, value = "1")
public class IgnitePdsCheckpointMapSnapshotTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(name);

        configuration.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            ).setCheckpointFrequency(TimeUnit.HOURS.toMillis(1))
        );

        // Plugin that creates a WAL manager that counts replays
        configuration.setPluginProviders(new AbstractTestPluginProvider() {
            /** {@inheritDoc} */
            @Override public String name() {
                return "testPlugin";
            }

            /** {@inheritDoc} */
            @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteWriteAheadLogManager.class.equals(cls))
                    return (T) new TestFileWriteAheadLogManager(((IgniteEx)ctx.grid()).context());

                return null;
            }
        });

        return configuration;
    }

    /** WAL manager that counts how many times replay has been called. */
    private static class TestFileWriteAheadLogManager extends FileWriteAheadLogManager {
        /** Count of times that {@link #replay(WALPointer, IgniteBiPredicate)} has been called. */
        private final AtomicInteger replayCount = new AtomicInteger();

        /** Constructor. */
        public TestFileWriteAheadLogManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@link GroupStateLazyStore} class name. */
        private static final String clsName = GroupStateLazyStore.class.getName();

        /** {@inheritDoc} */
        @Override public WALIterator replay(
            WALPointer start,
            @Nullable IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordDeserializeFilter
        ) throws IgniteCheckedException, StorageException {
            Exception exception = new Exception();
            StackTraceElement[] trace = exception.getStackTrace();

            // Here we only want to record replays from GroupStateLazyStore
            // because they're performed if the snapshot doesn't include the data for a checkpoint
            if (Arrays.stream(trace).anyMatch(el -> el.getClassName().equals(clsName)))
                replayCount.incrementAndGet();

            return super.replay(start, recordDeserializeFilter);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests that node can restart successfully with a checkpoint map snapshot.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithCheckpointMapSnapshot() throws Exception {
        testRestart(KEEP);
    }

    /**
     * Tests that node can restart successfully without a checkpoint map snapshot.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithoutCheckpointMapSnapshot() throws Exception {
        testRestart(REMOVE);
    }

    /**
     * Tests that node can restart successfully with an empty checkpoint map snapshot.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestartWithEmptyCheckpointMapSnapshot() throws Exception {
        testRestart(CLEAR_FILE);
    }

    /**
     * Tests node restart after a series of checkpoints. Node should use a checkpoint map snapshot if it is present.
     *
     * @param action Which action to perform with cpMapSnapshot.bin.
     * @throws Exception If failed.
     */
    private void testRestart(SnapshotAction action) throws Exception {
        IgniteEx grid = startGrid(0);

        grid.cluster().state(ClusterState.ACTIVE);

        // Count of inserts and checkpoints
        int cnt = 100;

        CacheConfiguration<Integer, Integer> configuration = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        IgniteCache<Integer, Integer> cache = grid.getOrCreateCache(configuration);

        for (int i = 0; i < cnt; i++) {
            cache.put(i, i);

            forceCheckpoint(grid);
        }

        stopGrid(0, true);

        File cpDir = dbMgr(grid).checkpointManager.checkpointDirectory();

        File cpSnapshotMap = new File(cpDir, CheckpointMarkersStorage.EARLIEST_CP_SNAPSHOT_FILE);

        if (action == REMOVE) {
            // Remove checkpoint map snapshot

            IgniteUtils.delete(cpSnapshotMap);

            assertFalse(cpSnapshotMap.exists());
        }
        else if (action == CLEAR_FILE) {
            try (
                FileOutputStream stream = new FileOutputStream(cpSnapshotMap, true);
                FileChannel outChan = stream.getChannel()
            ) {
                outChan.truncate(0);
                stream.flush();
                stream.getFD().sync();
            }
        }

        grid = startGrid(0);

        grid.cluster().state(ClusterState.ACTIVE);

        // Start new grids and wait for rebalance
        startGrid(1);
        startGrid(2);

        awaitPartitionMapExchange();

        TestFileWriteAheadLogManager wal = (TestFileWriteAheadLogManager) walMgr(grid);

        cache = grid.getOrCreateCache(configuration);

        // Check data in a cache
        for (int i = 0; i < cnt; i++)
            assertEquals(i, (int) cache.get(i));

        // Get count of WAL replays that are invoked from CheckpointEntry
        int replayCount = wal.replayCount.get();

        stopGrid(1, true);
        stopGrid(2, true);

        // 1 is the count of checkpoint on start of the node (see checkpoint with reason "node started")
        if (action == REMOVE || action == CLEAR_FILE)
            assertEquals(cnt + 1, replayCount);
        else
            assertEquals(0, replayCount);
    }

    /** Action to perform on checkpoint map snapshot. */
    enum SnapshotAction {
        /** Keep checkpoint map snapshot. */
        KEEP,
        /** Remove checkpoint map snapshot. */
        REMOVE,
        /** Clear checkpoint map snapshot file. */
        CLEAR_FILE
    }
}
