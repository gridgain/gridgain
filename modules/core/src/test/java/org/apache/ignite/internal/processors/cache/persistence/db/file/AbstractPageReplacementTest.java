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

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PageReplacementMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.DummyPageIO;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl.segmentIndex;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValueHierarchy;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/** Base class for testing various page replacement policies. */
public abstract class AbstractPageReplacementTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = 4 * 1024;

    /** */
    private static final int PAGE_COUNT = 10 * 1024;

    /** */
    private static final int MAX_MEMORY_SIZE = PAGE_COUNT * PAGE_SIZE;

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final int CACHE_ID = CU.cacheId(CACHE_NAME);

    /** */
    private static final int PARTITION_ID = 0;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setPageSize(PAGE_SIZE)
                    .setConcurrencyLevel(2)
                    .setCheckpointThreads(1)
                    .setFileIOFactory(createSpyRandomAccessFileIOFactory())
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setName("default")
                            .setInitialSize(MAX_MEMORY_SIZE)
                            .setMaxSize(MAX_MEMORY_SIZE)
                            .setPageReplacementMode(replacementMode())
                    )
            )
            .setCacheConfiguration(
                new CacheConfiguration<>(CACHE_NAME).setAffinity(new RendezvousAffinityFunction(false, 1))
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        IgniteEx node = startGrid(0);

        node.cluster().state(ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    abstract PageReplacementMode replacementMode();

    /** Checks that page replacement will occur after the start of the checkpoint and before its end. */
    @Test
    public void testPageReplacement() throws Throwable {
        CompletableFuture<Void> startWritePagesOnCheckpointFut = new CompletableFuture<>();
        CompletableFuture<Void> continueWritePagesOnCheckpointFut = new CompletableFuture<>();

        CompletableFuture<Void> startWritePagesOnPageReplacementFut = new CompletableFuture<>();

        CheckpointProgress checkpointProgress = inCheckpointReadLock(() -> {
            // We make only one dirty page for the checkpoint to work.
            createAndFillDummyPagesIntoEachPageMemorySegment();

            FileIO fileIO = fileIO();

            // First time the method should be invoked by the checkpoint writer, let's hold it for page replacement.
            doAnswer(invocation -> {
                startWritePagesOnCheckpointFut.complete(null);

                continueWritePagesOnCheckpointFut.get(1, MINUTES);

                return invocation.callRealMethod();
            }).doAnswer(invocation -> {
                // Second time the method should be invoked on page replacement.
                startWritePagesOnPageReplacementFut.complete(null);

                return invocation.callRealMethod();
            }).when(fileIO).writeFully(any(), anyLong());

            // Trigger checkpoint so that it writes a meta page and one dirty one. We do it under a read lock to ensure
            // that the background does not start after the lock is released.
            return database().getCheckpointManager().forceCheckpoint("for test", null);
        });

        GridFutureAdapter finishCheckpointFut = checkpointProgress.futureFor(FINISHED);

        // Let's wait for the checkpoint writer to start writing the first page.
        startWritePagesOnCheckpointFut.get(1, MINUTES);
        // Let's make sure that no one has tried to write another page.
        assertFalse(startWritePagesOnPageReplacementFut.isDone());

        // We will create dirty pages until the page replacement occurs.
        // Asynchronously so as not to get into dead locks or something like that.
        runAsync(() -> inCheckpointReadLock(
            () -> createAndFillDummyPages(() -> !startWritePagesOnPageReplacementFut.isDone())
        )).get(1, MINUTES);
        assertFalse(finishCheckpointFut.isDone());

        continueWritePagesOnCheckpointFut.complete(null);
        finishCheckpointFut.get(1, MINUTES);
        assertTrue(pageMemory().pageReplacementOccurred());
    }

    /** */
    @Test
    public void testFsyncPhaseWillNotStartOnCheckpointUntilPageReplacementIsComplete() throws Exception {
        CompletableFuture<Void> startWritePagesOnCheckpointFut = new CompletableFuture<>();
        CompletableFuture<Void> continueWritePagesOnCheckpointFut = new CompletableFuture<>();

        CompletableFuture<Void> startWritePagesOnPageReplacementFut = new CompletableFuture<>();
        CompletableFuture<Void> continueWritePagesOnPageReplacementFut = new CompletableFuture<>();

        CheckpointProgress checkpointProgress = inCheckpointReadLock(() -> {
            // We make only one dirty page for the checkpoint to work.
            createAndFillDummyPagesIntoEachPageMemorySegment();

            FileIO fileIO = fileIO();

            // First time the method should be invoked by the checkpoint writer, let's hold it for page replacement.
            doAnswer(invocation -> {
                startWritePagesOnCheckpointFut.complete(null);

                continueWritePagesOnCheckpointFut.get(1, MINUTES);

                return invocation.callRealMethod();
            }).doAnswer(invocation -> {
                // Second time the method should be invoked on page replacement, let's hold it.
                startWritePagesOnPageReplacementFut.complete(null);

                continueWritePagesOnPageReplacementFut.get(1, MINUTES);

                return invocation.callRealMethod();
            }).when(fileIO).writeFully(any(), anyLong());

            // Trigger checkpoint so that it writes a meta page and one dirty one. We do it under a read lock to ensure
            // that the background does not start after the lock is released.
            return database().getCheckpointManager().forceCheckpoint("for test", null);
        });

        GridFutureAdapter finishCheckpointFut = checkpointProgress.futureFor(FINISHED);

        // Let's wait for the checkpoint writer to start writing the first page.
        startWritePagesOnCheckpointFut.get(1, MINUTES);

        // We will create dirty pages until the page replacement occurs.
        // Asynchronously so as not to get into dead locks or something like that.

        IgniteInternalFuture createPagesForPageReplacementFut = runAsync(() -> inCheckpointReadLock(
            () -> createAndFillDummyPages(() -> !startWritePagesOnPageReplacementFut.isDone())
        ));
        startWritePagesOnPageReplacementFut.get(1, MINUTES);
        assertFalse(createPagesForPageReplacementFut.isDone());

        // Let's release the checkpoint and make sure that it does not complete and fsync does not occur until the page replacement is
        // complete.
        continueWritePagesOnCheckpointFut.complete(null);
        assertThrows(
            log,
            () -> finishCheckpointFut.get(250, MILLISECONDS), IgniteFutureTimeoutCheckedException.class,
            null
        );
        // 250 by analogy with willTimeoutFast().
        verify(fileIO(), timeout(250).times(0)).force();
        assertFalse(createPagesForPageReplacementFut.isDone());

        // Let's release the page replacement and make sure everything ends well.
        continueWritePagesOnPageReplacementFut.complete(null);

        finishCheckpointFut.get(1, MINUTES);
        createPagesForPageReplacementFut.get(1, MINUTES);
        verify(fileIO()).force();
    }

    /** */
    private void createAndFillTestDummyPage(long pageId) throws Exception {
        long page = pageMemory().acquirePage(CACHE_ID, pageId);

        try {
            long pageAddr = pageMemory().writeLock(CACHE_ID, pageId, page);

            try {
                new DummyPageIO().initNewPage(pageAddr, pageId, pageMemory().realPageSize(CACHE_ID), null);
            }
            finally {
                pageMemory().writeUnlock(CACHE_ID, pageId, page, null, true);
            }
        }
        finally {
            pageMemory().releasePage(CACHE_ID, pageId, page);
        }
    }

    /** */
    private <V> V inCheckpointReadLock(Callable<V> call) throws Exception {
        database().checkpointReadLock();

        try {
            return call.call();
        }
        finally {
            database().checkpointReadUnlock();
        }
    }

    /** */
    private void inCheckpointReadLock(RunnableX runnableX) throws Exception {
        database().checkpointReadLock();

        try {
            runnableX.run();
        }
        finally {
            database().checkpointReadUnlock();
        }
    }

    /** */
    private void createAndFillDummyPagesIntoEachPageMemorySegment() throws Exception {
        PageMemoryImpl pageMemory = pageMemory();

        int segmentCnt = ((Object[])getFieldValue(pageMemory, "segments")).length;

        Set<Integer> segmentIndexes = new HashSet<>();

        while (segmentIndexes.size() < segmentCnt) {
            long pageId = pageMemory.allocatePage(CACHE_ID, PARTITION_ID, FLAG_DATA);

            createAndFillTestDummyPage(pageId);

            segmentIndexes.add(segmentIndex(CACHE_ID, pageId, segmentCnt));
        }
    }

    /** */
    private void createAndFillDummyPages(BooleanSupplier continuePred) throws Exception {
        while (continuePred.getAsBoolean())
            createAndFillTestDummyPage(pageMemory().allocatePage(CACHE_ID, PARTITION_ID, FLAG_DATA));
    }

    /** */
    private GridCacheDatabaseSharedManager database() {
        return (GridCacheDatabaseSharedManager)grid(0).context().cache().context().database();
    }

    /** */
    private PageMemoryImpl pageMemory() throws IgniteCheckedException {
        return (PageMemoryImpl)database().dataRegion(null).pageMemory();
    }

    /** */
    private FileIO fileIO() throws IgniteCheckedException {
        FilePageStore filePageStore = (FilePageStore)database().getFileStoreManager().getStore(CACHE_ID, PARTITION_ID);

        filePageStore.ensure();

        return getFieldValueHierarchy(filePageStore, "fileIO");
    }

    /** */
    private static RandomAccessFileIOFactory createSpyRandomAccessFileIOFactory() {
        return new RandomAccessFileIOFactory() {
            /** {@inheritDoc} */
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                return spy(super.create(file, modes));
            }
        };
    }
}
