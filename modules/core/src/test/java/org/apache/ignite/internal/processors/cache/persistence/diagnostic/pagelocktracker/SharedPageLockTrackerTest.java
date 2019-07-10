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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.junit.Assert;
import org.junit.Test;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.HEAP_LOG;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.HEAP_STACK;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.OFF_HEAP_LOG;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.OFF_HEAP_STACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class SharedPageLockTrackerTest extends AbstractPageLockTest {
    /** */
    private static final String THREAD_NAME_PREFFIX = "my-thread-";

    /**
     *
     */
    @Test
    public void testTakeDumpByCount() throws Exception {
        int[] trackerTypes = new int[] {HEAP_STACK, HEAP_LOG, OFF_HEAP_STACK, OFF_HEAP_LOG};

        LockTrackerFactory.DEFAULT_CAPACITY = 512;

        for (int i = 0; i < trackerTypes.length; i++) {
            LockTrackerFactory.DEFAULT_TYPE = trackerTypes[i];

            int dumps = SF.apply(30, 10, 40);

            doTestTakeDumpByCount(5, 1, dumps, 1);

            doTestTakeDumpByCount(20, 6, dumps, Runtime.getRuntime().availableProcessors() * 2);
        }
    }

    /**
     *
     */
    private void doTestTakeDumpByCount(
        int pagesCnt,
        int structuresCnt,
        int dumpCnt,
        int threads
    ) throws IgniteCheckedException, InterruptedException {
        SharedPageLockTracker sharedPageLockTracker = new SharedPageLockTracker();

        List<PageMeta> pageMetas = new CopyOnWriteArrayList<>();

        int id = 1;

        for (int i = 0; i < pagesCnt; i++)
            pageMetas.add(new PageMeta((id++) % structuresCnt, id++, id++, id++));

        List<PageLockListener> pageLsnrs = new ArrayList<>();

        for (int i = 0; i < structuresCnt; i++)
            pageLsnrs.add(sharedPageLockTracker.registrateStructure("my-structure-" + i));

        AtomicBoolean stop = new AtomicBoolean();

        CountDownLatch awaitThreadStartLatch = new CountDownLatch(threads);

        IgniteInternalFuture f = GridTestUtils.runMultiThreadedAsync(() -> {
            List<PageLockListener> locks = new ArrayList<>(pageLsnrs);
            List<PageMeta> pages = new ArrayList<>();

            pages.addAll(pageMetas);

            while (!stop.get()) {
                Collections.shuffle(locks);
                Collections.shuffle(pages);

                for (PageLockListener lsnr : locks) {
                    for (PageMeta pageMeta : pages) {
                        parkNanos(nextRandomWaitTimeout(500));

                        lsnr.onBeforeReadLock(pageMeta.structureId, pageMeta.pageId, pageMeta.page);

                        parkNanos(nextRandomWaitTimeout(500));

                        lsnr.onReadLock(pageMeta.structureId, pageMeta.pageId, pageMeta.page, pageMeta.pageAddr);
                    }
                }

                parkNanos(nextRandomWaitTimeout(1000));

                Collections.reverse(locks);
                Collections.reverse(pages);

                for (PageLockListener lsnr : locks) {
                    for (PageMeta pageMeta : pages) {
                        parkNanos(nextRandomWaitTimeout(500));

                        lsnr.onReadUnlock(pageMeta.structureId, pageMeta.pageId, pageMeta.page, pageMeta.pageAddr);
                    }
                }

                if (awaitThreadStartLatch.getCount() > 0)
                    awaitThreadStartLatch.countDown();
            }
        }, threads, "PageLocker");

        awaitThreadStartLatch.await();

        for (int i = 0; i < dumpCnt; i++) {
            awaitRandom(100);

            ThreadPageLocksDumpLock dump = sharedPageLockTracker.dump();

            assertEquals(threads, dump.threadStates.size());
            assertEquals(0, dump.threadStates.stream().filter(e -> e.invalidContext != null).count());
        }

        stop.set(true);

        f.get();
    }

    /**
     * Test for checking that internal maps is not leaked after threads stopped.
     */
    @Test
    public void testMemoryLeakOnThreadTerminates() throws Exception {
        int threadLimits = 1000;

        SharedPageLockTracker sharedPageLockTracker = new SharedPageLockTracker(
            threadLimits, 10_000, (threads) -> {
        }, new MemoryCalculator());

        doLockingInThreads(10_000, sharedPageLockTracker.registrateStructure("test"));

        sharedPageLockTracker.start();

        ThreadPageLocksDumpLock dump = sharedPageLockTracker.dump();

        assertTrue(dump.time > 0);
        assertTrue(!dump.threadStates.isEmpty());

        for (ThreadPageLocksDumpLock.ThreadState threadState : dump.threadStates) {
            assertNull(threadState.invalidContext);
            assertTrue(threadState.threadName.startsWith(THREAD_NAME_PREFFIX));
            assertSame(Thread.State.TERMINATED, threadState.state);

        }

        Assert.assertEquals(1, dump.structureIdToStrcutureName.size());

        synchronized (sharedPageLockTracker) {
            Map<Long, Thread> threadMap0 = U.field(sharedPageLockTracker, "threadIdToThreadRef");
            Map<Long, ?> threadStacksMap0 = U.field(sharedPageLockTracker, "threadStacks");

            // Stopped threads should remove from map after map limit reached.
            assertTrue(threadMap0.size() <= threadLimits);
            assertTrue(threadStacksMap0.size() <= threadLimits);
        }
    }

    /**
     * Test for checking that internal maps is empty hen timeout worker have finished.
     */
    @Test
    public void testCleanTermintatedThreads() throws Exception {
        int timeOutWorkerInterval = 100;

        SharedPageLockTracker sharedPageLockTracker = new SharedPageLockTracker(
            1000, timeOutWorkerInterval, (threads1) -> {
        }, new MemoryCalculator()
        );

        doLockingInThreads(3, sharedPageLockTracker.registrateStructure("test"));

        sharedPageLockTracker.start();

        // Await cleanup worker interval.
        U.sleep(timeOutWorkerInterval * 2);

        synchronized (sharedPageLockTracker) {
            Map<Long, Thread> threadMap1 = U.field(sharedPageLockTracker, "threadIdToThreadRef");
            Map<Long, ?> threadStacksMap1 = U.field(sharedPageLockTracker, "threadStacks");

            // Cleanup worker should remove all stopped threads.
            assertTrue(threadMap1.isEmpty());
            assertTrue(threadStacksMap1.isEmpty());
        }

        ThreadPageLocksDumpLock dump1 = sharedPageLockTracker.dump();

        assertTrue(dump1.time > 0);
        assertTrue(dump1.threadStates.isEmpty());
    }

    /**
     * Run and wait threads which do locking.
     */
    private void doLockingInThreads(int threads, PageLockListener lt) {
        int cacheId = 1;
        long pageId = 2, page = 3, pageAdder = 4;

        List<Thread> threadsList = new ArrayList<>(threads);

        for (int i = 0; i < threads; i++) {
            Thread th = new Thread(() -> {
                lt.onBeforeReadLock(cacheId, pageId, page);

                lt.onReadLock(cacheId, pageId, page, pageAdder);

                lt.onReadUnlock(cacheId, pageId, page, pageAdder);
            });

            th.setName(THREAD_NAME_PREFFIX + i);

            threadsList.add(th);

            th.start();

            System.out.println(">>> start thread:" + th.getName());
        }

        threadsList.forEach(th -> {
            try {
                System.out.println(">>> await thread:" + th.getName());

                th.join();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     *
     */
    @Test
    public void testAutoDetectHangThreads() throws Exception {
        String thInWaitName = "threadInWait";
        String thInRunnableName = "threadInRunnable";
        String thInAwaitWithoutLocksName = "threadInAwaitWithoutLocks";

        AtomicReference<Exception> error = new AtomicReference<>(new Exception("Threads are not finished"));

        CountDownLatch awaitLatch = new CountDownLatch(1);

        SharedPageLockTracker sharedPageLockTracker = new SharedPageLockTracker(
            1000,
            10,
            hangsThreads -> {
                if (hangsThreads.isEmpty()) {
                    error.set(new Exception("No one thread is hangs."));

                    return;
                }

                // Checking threads.
                for (SharedPageLockTracker.State state : hangsThreads) {
                    String name = state.thread.getName();

                    if (name.equals(thInAwaitWithoutLocksName)) {
                        error.set(new Exception("Thread without locks should not be here." + state));
                        continue;
                    }

                    if (name.equals(thInWaitName)) {
                        if (state.heldLockCnt == 0)
                            error.set(new Exception("Thread should hold lock." + state));

                        if (state.thread.getState() != Thread.State.WAITING)
                            error.set(new Exception("Thread should in WAITING state." + state));

                        continue;
                    }

                    if (name.equals(thInRunnableName)) {
                        if (state.heldLockCnt == 0)
                            error.set(new Exception("Thread should hold lock." + state));

                        if (state.thread.getState() != Thread.State.RUNNABLE)
                            error.set(new Exception("Thread should in RUNNABLE state." + state));

                        continue;
                    }
                }

                //Release threads only when all two threads are hangs.
                if (hangsThreads.size() == 2) {
                    error.set(null);

                    awaitLatch.countDown();
                }
            }, new MemoryCalculator()
        );

        int cacheId = 1;
        long pageId = 2;
        long page = 3;
        long pageAdder = 4;

        PageLockListener lt = sharedPageLockTracker.registrateStructure("test");

        Thread thInWait = new Thread(() -> {
            lt.onBeforeReadLock(cacheId, pageId, page);

            lt.onReadLock(cacheId, pageId, page, pageAdder);

            try {
                awaitLatch.await();
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
        });

        thInWait.setName(thInWaitName);

        Thread thInRunnable = new Thread(() -> {
            lt.onBeforeReadLock(cacheId, pageId, page);

            lt.onReadLock(cacheId, pageId, page, pageAdder);

            while (awaitLatch.getCount() > 0) {
                // Busy wait. Can not park this thread, we should check running hangs too.
            }
        });

        thInRunnable.setName(thInRunnableName);

        Thread thInAwaitWithoutLocks = new Thread(() -> {
            lt.onBeforeReadLock(cacheId, pageId, page);

            lt.onReadLock(cacheId, pageId, page, pageAdder);

            lt.onReadUnlock(cacheId, pageId, page, pageAdder);

            try {
                awaitLatch.await();
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
        });

        thInAwaitWithoutLocks.setName(thInAwaitWithoutLocksName);

        sharedPageLockTracker.start();

        thInRunnable.start();
        thInWait.start();
        thInAwaitWithoutLocks.start();

        thInRunnable.join(5000);
        thInWait.join(5000);
        thInAwaitWithoutLocks.join(5000);

        if (error.get() != null)
            throw error.get();
    }

    /**
     *
     */
    private static class PageMeta {
        /** */
        final int structureId;
        /** */
        final long pageId;
        /** */
        final long page;
        /** */
        final long pageAddr;

        /** */
        private PageMeta(
            int structureId,
            long pageId,
            long page,
            long pageAddr
        ) {
            this.structureId = structureId;
            this.pageId = pageId;
            this.page = page;
            this.pageAddr = pageAddr;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "PageMeta{" +
                "structureId=" + structureId +
                ", pageId=" + pageId +
                ", page=" + page +
                ", pageAddr=" + pageAddr +
                '}';
        }
    }
}