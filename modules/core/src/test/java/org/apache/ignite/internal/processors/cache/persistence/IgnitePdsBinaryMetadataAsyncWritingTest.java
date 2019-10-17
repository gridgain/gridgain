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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for verification of binary metadata async writing to disk.
 */
public class IgnitePdsBinaryMetadataAsyncWritingTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicReference<CountDownLatch> fileWriteLatchRef = new AtomicReference<>(null);

    /** */
    private FileIOFactory specialFileIOFactory;

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
                    specialFileIOFactory != null ? specialFileIOFactory : new RandomAccessFileIOFactory()
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
        );

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Verifies that request adding/modifying binary metadata (e.g. put to cache a new value)
     * is blocked until write to disk is finished.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testThreadRequestingUpdateBlockedTillWriteCompletion() throws Exception {
        specialFileIOFactory = new SlowFileIOFactory(new RandomAccessFileIOFactory());

        final CountDownLatch fileWriteLatch = new CountDownLatch(1);
        fileWriteLatchRef.set(fileWriteLatch);

        Ignite ig = startGrid();

        ig.cluster().active(true);

        IgniteCache<Object, Object> cache = ig.cache(DEFAULT_CACHE_NAME);

        GridTestUtils.runAsync(() -> cache.put(1, new TestPerson(0, "John", "Oliver")));

        assertEquals(0, cache.size(CachePeekMode.PRIMARY));

        fileWriteLatch.countDown();

        assertTrue(GridTestUtils.waitForCondition(() -> cache.size(CachePeekMode.PRIMARY) == 1, 10_000));
    }

    /**
     * @throws Exception
     */
    @Test
    public void testDiscoveryIsNotBlockedOnMetadataWrite() throws Exception {
        specialFileIOFactory = new SlowFileIOFactory(new RandomAccessFileIOFactory());

        final CountDownLatch fileWriteLatch = new CountDownLatch(1);
        fileWriteLatchRef.set(fileWriteLatch);

        IgniteKernal ig = (IgniteKernal)startGrid();

        ig.cluster().active(true);

        IgniteCache<Object, Object> cache = ig.cache(DEFAULT_CACHE_NAME);

        TestAddress addr = new TestAddress(0, "RUS", "Spb", "Nevsky");
        TestPerson person = new TestPerson(0, "John", "Oliver");
        person.address(addr);
        TestAccount account = new TestAccount(person, 0, 1000);

        GridTestUtils.runAsync(() -> cache.put(0, addr));
        GridTestUtils.runAsync(() -> cache.put(0, person));
        GridTestUtils.runAsync(() -> cache.put(0, account));

        assertEquals(0, cache.size(CachePeekMode.PRIMARY));

        Map locCache = GridTestUtils.getFieldValue((CacheObjectBinaryProcessorImpl)ig.context().cacheObjects(), "metadataLocCache");

        assertEquals(3, locCache.size());

        fileWriteLatch.countDown();
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeIsStoppedOnExceptionDuringStoringMetadata() throws Exception {
        Ignite ig0 = startGrid(0);

        specialFileIOFactory = new FailingFileIOFactory(new RandomAccessFileIOFactory());

        Ignite ig1 = startGrid(1);

        ig0.cluster().active(true);

        int ig1Key = findKeyForNode(ig0.affinity(DEFAULT_CACHE_NAME), ig1.cluster().localNode());

        IgniteCache<Object, Object> cache = ig0.cache(DEFAULT_CACHE_NAME);

        cache.put(ig1Key, new TestAddress(0, "USA", "NYC", "6th Ave"));

        waitForTopology(1, 10_000);
    }

    private int findKeyForNode(Affinity aff, ClusterNode targetNode) {
        int key = 0;

        while (true) {
            if (aff.isPrimary(targetNode, key))
                return key;

            key++;
        }
    }

    /** */
    static final class TestPerson {
        /** */
        private final int id;
        /** */
        private final String firstName;
        /** */
        private final String surname;
        /** */
        private TestAddress addr;

        /** */
        TestPerson(int id, String firstName, String surname) {
            this.id = id;
            this.firstName = firstName;
            this.surname = surname;
        }

        /** */
        void address(TestAddress addr) {
            this.addr = addr;
        }
    }

    static final class TestAddress {
        /** */
        private final int id;
        /** */
        private final String country;
        /** */
        private final String city;
        /** */
        private final String address;

        /** */
        TestAddress(int id, String country, String city, String street) {
            this.id = id;
            this.country = country;
            this.city = city;
            this.address = street;
        }
    }

    /** */
    static final class TestAccount {
        /** */
        private final TestPerson person;
        /** */
        private final int accountId;
        /** */
        private final long accountBalance;
        /** */
        TestAccount(
            TestPerson person, int id, long balance) {
            this.person = person;
            accountId = id;
            accountBalance = balance;
        }
    }

    private static boolean isBinaryMetaFile(File file) {
        return file.getPath().contains("binary_meta");
    }

    /**
     *
     */
    static final class SlowFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegateFactory;

        /**
         * @param delegateFactory Delegate factory.
         */
        SlowFileIOFactory(FileIOFactory delegateFactory) {
            this.delegateFactory = delegateFactory;
        }

        /** {@inheritDoc} */
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

    /** */
    static final class FailingFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegateFactory;

        /**
         * @param factory Delegate factory.
         */
        FailingFileIOFactory(FileIOFactory factory) {
            delegateFactory = factory;
        }

        /** {@inheritDoc}*/
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            if (isBinaryMetaFile(file))
                return new FailingFileIO(delegate);

            return delegate;
        }
    }

    static final class FailingFileIO extends FileIODecorator {
        /**
         * @param delegate File I/O delegate
         */
        public FailingFileIO(FileIO delegate) {
            super(delegate);
        }

        /** {@inheritDoc}*/
        @Override public int write(byte[] buf, int off, int len) throws IOException {
            throw new IOException("Error occured during write of binary metadata");
        }
    }
}
