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
package org.apache.ignite.internal.processors.query.oom;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.List;
import javax.cache.CacheException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.metric.SqlMemoryStatisticsHolder;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.query.h2.H2MemoryTracker;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.internal.processors.query.h2.disk.TrackableFileIoFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Test cases for IO error happened on file creation.
 */
public class DiskSpillingIoErrorTest extends DiskSpillingAbstractTest {
    /** {@inheritDoc} */
    @Override protected boolean persistence() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op. Test environment wil be set up in the @beforeTest method.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        initGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        destroyGrid();
    }

    /**
     * Test 5 (IO exception on first first “offload” file creation)
     *
     * start 1 server node grid
     * create tables (partitioned cache), populate with data
     * run query and get query cursor asynchronous. During this:
     * throw IO exception when disk offload started
     * Check correct exception
     */
    @Test
    public void testSpillFilesDeletedOnErrorOnFirstCreation() {
        checkSpillFilesCleanedOnFileCreation(1);
    }

    /**
     * Test 6 (IO exception on second first “offload” file creation)
     *
     * start 1 server node grid
     * create tables (partitioned cache), populate with data
     * run query and get query cursor asynchronous. During this:
     * throw IO exception when disk offload start creating second file.
     * Check all offload files was deleted
     * Check correct exception
     */
    @Test
    public void testSpillFilesDeletedOnErrorOnSecondCreation() {
        checkSpillFilesCleanedOnFileCreation(2);
    }

    /**
     * Checks if spill files are deleted on IO error.
     *
     * @param crashOnCreateCnt The number of file which creation will induce the IO error.
     */
    private void checkSpillFilesCleanedOnFileCreation(int crashOnCreateCnt) {
        QueryMemoryManager memMgr = ((IgniteH2Indexing)grid(0).context().query().getIndexing()).memoryManager();

        // Set broken file factory.
        BrokenIoFactory ioFactory = new BrokenIoFactory(crashOnCreateCnt, grid(0).context().metric(), memMgr);

        GridTestUtils.setFieldValue(memMgr, "fileIOFactory", ioFactory);

        try (FieldsQueryCursor<List<?>> cur = grid(0).cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQueryEx(
                "SELECT id, name, code, depId FROM person WHERE depId >= 0 " +
                    " EXCEPT " +
                    "SELECT id, name, code, depId FROM person WHERE depId > 5 ", null)
                .setMaxMemory(SMALL_MEM_LIMIT)
                .setLazy(true))) {

            cur.iterator();

            fail("Exception is not thrown.");
        }
        catch (CacheException e) {
            assertNotNull(e.getMessage());
        }

        assertEquals(0, ioFactory.crashOnCreateCnt);

        // Check spill files were deleted.
        assertWorkDirClean();

        checkMemoryManagerState();
    }

    /**
     * Broken IO factory.
     */
    private static class BrokenIoFactory extends TrackableFileIoFactory {
        /** The number of file which creation will induce the IO error. */
        private int crashOnCreateCnt;

        /**
         * @param crashOnCreateCnt The number of file which creation will induce the IO error.
         */
        BrokenIoFactory(int crashOnCreateCnt, GridMetricManager metric, QueryMemoryManager memMgr) {
            super(new AsyncFileIOFactory(), new SqlMemoryStatisticsHolder(memMgr, metric));
            this.crashOnCreateCnt = crashOnCreateCnt;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, H2MemoryTracker tracker, OpenOption... modes) throws IOException {
            if (--crashOnCreateCnt == 0)
                throw new IOException("Test crash.");

            return super.create(file, NO_OP_TRACKER, modes);
        }
    }

    /** */
    private static final H2MemoryTracker NO_OP_TRACKER = new H2MemoryTracker() {
        /** {@inheritDoc} */
        @Override public boolean reserve(long size) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void release(long size) {
        }

        /** {@inheritDoc} */
        @Override public long writtenOnDisk() {
            return -1;
        }

        /** {@inheritDoc} */
        @Override public long totalWrittenOnDisk() {
            return -1;
        }

        /** {@inheritDoc} */
        @Override public long reserved() {
            return -1;
        }

        /** {@inheritDoc} */
        @Override public void spill(long size) {
        }

        /** {@inheritDoc} */
        @Override public void unspill(long size) {
        }

        /** {@inheritDoc} */
        @Override public void close() {
        }

        /** {@inheritDoc} */
        @Override public void incrementFilesCreated() {
        }

        /** {@inheritDoc} */
        @Override public H2MemoryTracker createChildTracker() {
            return NO_OP_TRACKER;
        }

        /** {@inheritDoc} */
        @Override public void onChildClosed(H2MemoryTracker child) {
        }

        /** {@inheritDoc} */
        @Override public boolean closed() {
            return false;
        }
    };
}
