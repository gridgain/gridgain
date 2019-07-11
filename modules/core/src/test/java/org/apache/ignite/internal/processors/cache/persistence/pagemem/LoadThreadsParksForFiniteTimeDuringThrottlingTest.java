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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks that, throttled load threads will be unparked earlier than parkTime, if number of pages in checkpoint buffer
 * would decrease.
 **/
public class LoadThreadsParksForFiniteTimeDuringThrottlingTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache1";

    /** */
    private static final int KEYS_COUNT = 1000;

    /** */
    private static final long MAX_DATA_REGION_SIZE = 1024L * 1024L * 1024L;

    /** */
    private static final long CP_BUFFER_SIZE = MAX_DATA_REGION_SIZE / 4;

    /** */
    private static final int AVG_RECORD_SIZE = (int)(MAX_DATA_REGION_SIZE / KEYS_COUNT);

    /** */
    private static final int LOAD_THREADS_COUNT = 20;

    /** */
    private static final String DFLT_DATA_REGION_NAME = "dfltDataRegion";

    /** */
    private final List<Thread> loadThreads = new ArrayList<>(LOAD_THREADS_COUNT);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setCacheConfiguration(new CacheConfiguration(CACHE_NAME))
            .setConsistentId(gridName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setCheckpointThreads(1)
                .setCheckpointReadLockTimeout(getTestTimeout())
                .setFileIOFactory(new SlowCheckpointFileIOFactory())
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setCheckpointPageBufferSize(CP_BUFFER_SIZE)
                    .setMaxSize(MAX_DATA_REGION_SIZE)
                    .setName(DFLT_DATA_REGION_NAME)
                    .setPersistenceEnabled(true)
                )
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        SlowCheckpointFileIOFactory.enableSlowCheckpoint();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        SlowCheckpointFileIOFactory.disableSlowCheckpoint();

        loadThreads.forEach(Thread::interrupt);

        loadThreads.clear();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000;
    }

    /**
     * See test class description.
     */
    @Test
    public void testThrottleLongSleep() throws Exception {
        IgniteEx crd = startGrid(0);

        crd.cluster().active(true);

        final IgniteCache<Integer, TestValue> cache = crd.getOrCreateCache(CACHE_NAME);

        AtomicBoolean stopLoad = new AtomicBoolean(false);

        for (int tn = 0; tn < LOAD_THREADS_COUNT; tn++) {
            loadThreads.add(new Thread(
                () -> {
                    while (!stopLoad.get()) {
                        int key = ThreadLocalRandom.current().nextInt(KEYS_COUNT);
                        int val = ThreadLocalRandom.current().nextInt(KEYS_COUNT);

                        cache.put(key, new TestValue(key + val));
                    }
                },
                "load-" + tn
            ));
        }

        loadThreads.forEach(Thread::start);

        AtomicBoolean throttlingEnabled = new AtomicBoolean(false);

        IgniteInternalFuture<Boolean> throttlingEnabledFut = GridTestUtils.runAsync(() -> {
            final long sleepTime = 500L;
            final int numOfLoops = (int)(getTestTimeout() / sleepTime);
            PageMemory pm = crd.context().cache().context().database().dataRegion(DFLT_DATA_REGION_NAME).pageMemory();

            // throttling will be enabled, if 2/3 of checkpoint buffer size is busy.
            final double limit = (0.67 * Math.ceil((CP_BUFFER_SIZE + 0.) / pm.pageSize()));

            for (int i = 0; i < numOfLoops; i++) {
                if (pm.checkpointBufferPagesCount() > limit) {
                    log.info("Throttling enabled!");

                    throttlingEnabled.set(true);

                    return true;
                }

                doSleep(sleepTime);
            }

            log.error("Throttling wasn't enabled!");

            return false;
        });

        // Emulate spurious wakeups.
        while (!throttlingEnabled.get()) {
            doSleep(500L);

            loadThreads.forEach(LockSupport::unpark);
        }

        assertTrue("Throttling wasn't enabled!", throttlingEnabledFut.get());

        SlowCheckpointFileIOFactory.disableSlowCheckpoint();

        IgniteInternalFuture forceCpFut = GridTestUtils.runAsync(() -> {
            try {
                forceCheckpoint(crd);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        });

        try {
            // Throttled threads hold read lock's on pages. Checkpoint cannot begin while other threads hold checkpoint
            // readlock, because of checkpoint thread must get checkpoint writelock. So checkpoint cannot begin while
            // load threads are not unparked.
            forceCpFut.get(60000L);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            log.error("awaiting checkpoint failed!", e);

            forceCpFut.cancel();

            fail("awaiting force checkpoint failed!");
        }

        stopLoad.set(true);

        for (Thread t : loadThreads)
            t.join();
    }

    /** */
    private static class TestValue implements Serializable {
        /** */
        private final int id;

        /** */
        private final byte[] payload = new byte[AVG_RECORD_SIZE + ThreadLocalRandom.current().nextInt(20)];

        /** */
        private TestValue(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestValue value = (TestValue)o;

            return id == value.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     * Create File I/O that emulates poor checkpoint write speed.
     */
    private static class SlowCheckpointFileIOFactory implements FileIOFactory {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        private static final AtomicBoolean slowCheckpointEnabled = new AtomicBoolean();

        /** */
        private static final long PARK_TIME = U.millisToNanos(1000L);

        /** Delegate factory. */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** */
        private static void enableSlowCheckpoint() {
            slowCheckpointEnabled.set(true);

            log.info("Slow checkpointing enabled!");
        }

        /** */
        private static void disableSlowCheckpoint() {
            slowCheckpointEnabled.set(false);

            log.info("Slow checkpointing disabled!");
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... openOption) throws IOException {
            final FileIO delegate = delegateFactory.create(file, openOption);

            return new FileIODecorator(delegate) {
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (slowCheckpointEnabled.get() && Thread.currentThread().getName().contains("checkpoint"))
                        LockSupport.parkNanos(PARK_TIME);

                    return delegate.write(srcBuf);
                }

                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    if (slowCheckpointEnabled.get() && Thread.currentThread().getName().contains("checkpoint"))
                        LockSupport.parkNanos(PARK_TIME);

                    return delegate.write(srcBuf, position);
                }

                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    if (slowCheckpointEnabled.get() && Thread.currentThread().getName().contains("checkpoint"))
                        LockSupport.parkNanos(PARK_TIME);

                    return delegate.write(buf, off, len);
                }

                /** {@inheritDoc} */
                @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
                    return delegate.map(sizeBytes);
                }
            };
        }
    }
}
