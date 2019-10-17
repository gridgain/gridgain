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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Contains tests for verification that meta
 */
public class IgnitePdsBinaryMetadataAsyncWritingTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicReference<CountDownLatch> fileWriteLatchRef = new AtomicReference<>(null);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
                .setFileIOFactory(
                    new SlowFileIOFactory(new RandomAccessFileIOFactory())
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setBackups(1)
        );

        return cfg;
    }

    @Test
    public void testThreadRequestingUpdateBlockedTillWriteCompletion() throws Exception {
        CountDownLatch fileWriteLatch = new CountDownLatch(1);

        fileWriteLatchRef.set(fileWriteLatch);

        Ignite ig = startGrid();

        ig.cluster().active(true);

        IgniteCache<Object, Object> cache = ig.cache(DEFAULT_CACHE_NAME);

        GridTestUtils.runAsync(() -> cache.put(1, new TestCacheValue(0, "payload0")));

        assertEquals(0, cache.size(CachePeekMode.PRIMARY));

        fileWriteLatch.countDown();

        assertTrue(GridTestUtils.waitForCondition(() -> cache.size(CachePeekMode.PRIMARY) == 1, 10_000));
    }

    final class TestCacheValue {
        /** */
        private final int id;
        /** */
        private final String payload;

        /** */
        TestCacheValue(int id, String payload) {
            this.id = id;
            this.payload = payload;
        }
    }

    /**
     *
     */
    static final class SlowFileIOFactory implements FileIOFactory {
        private final FileIOFactory delegateFactory;

        /**
         * @param delegateFactory Delegate factory.
         */
        SlowFileIOFactory(FileIOFactory delegateFactory) {
            this.delegateFactory = delegateFactory;
        }

        /** */
        private boolean isBinaryMetaFile(File file) {
            return file.getPath().contains("binary_meta");
        }

        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            if (isBinaryMetaFile(file))
                return new SlowFileIO(delegate, fileWriteLatchRef.get());

            return delegate;
        }
    }

    /**
     *
     */
    static class SlowFileIO extends FileIODecorator {
        /** */
        private final CountDownLatch fileWriteLatch;

        /**
         * @param delegate File I/O delegate
         */
        public SlowFileIO(FileIO delegate, CountDownLatch fileWriteLatch) {
            super(delegate);

            this.fileWriteLatch = fileWriteLatch;
        }

        /** {@inheritDoc} */
        @Override public int write(byte[] buf, int off, int len) throws IOException {
            try {
                fileWriteLatch.await();
            }
            catch (InterruptedException e) {
                // No-op.
            }

            return super.write(buf, off, len);
        }
    }
}
