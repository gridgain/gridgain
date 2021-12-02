/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.util.lang.GridClosure3;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_SKIP_CRC;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.T_PART_META;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2.GAPS_LINK;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2.PART_META_REUSE_LIST_ROOT_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;

public class PartitionMetasInconsistencyOnNodeStartTest extends GridCommonAbstractTest {
    private static final int PAGE_SIZE = 4096;

    private static final int PAGE_STORE_VER = 2;

    private static boolean t = false;

    private FilePageStoreFactory storeFactory = new FileVersionCheckingFactory(
        new AsyncFileIOFactory(),
        new AsyncFileIOFactory(),
        new DataStorageConfiguration().setPageSize(PAGE_SIZE)
    ) {
        /** {@inheritDoc} */
        @Override public int latestVersion() {
            return PAGE_STORE_VER;
        }
    };

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    @WithSystemProperty(key = IGNITE_PDS_SKIP_CRC, value = "true")
    public void test() throws Exception {
        // 1st start - to create cache and partition.
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
        );

        cache.put(1, 1);

        forceCheckpoint();

        final PdsFolderSettings folderSettings = ignite.context().pdsFolderResolver().resolveFolders();

        File storeWorkDir = new File(folderSettings.persistentStoreRootPath(), folderSettings.folderName());

        File cacheWorkDir = new File(storeWorkDir, CACHE_DIR_PREFIX + DEFAULT_CACHE_NAME);

        File partFile = new File(cacheWorkDir, format(PART_FILE_TEMPLATE, 0));

        // Partition created.
        stopAllGrids();

        FilePageStore store = (FilePageStore)storeFactory.createPageStore(FLAG_DATA, () -> partFile.toPath(), a -> {});

        store.ensure();

        Map<Short, Long> pageIdsMap = findPages(0, FLAG_DATA, store, new HashSet<Short>() {{ add(T_PART_META); }});

        long metaPageId = pageIdsMap.get(T_PART_META);

        int pageIdx = PageIdUtils.pageIndex(metaPageId);

        long partMetaReuseOffset = store.headerSize() + pageIdx * PAGE_SIZE + PART_META_REUSE_LIST_ROOT_OFF;

        long gapsLinkOff = store.headerSize() + pageIdx * PAGE_SIZE + GAPS_LINK;

        // Partitions of older versions didn't have these fields, so set them to 0.
        writeLongToFileByOffset(store.getFileAbsolutePath(), partMetaReuseOffset, 0L);
        writeLongToFileByOffset(store.getFileAbsolutePath(), gapsLinkOff, 0L);

        // 2nd start - to create counter gaps and write them to partition.
        ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        PartitionUpdateCounter counter = counter(0, DEFAULT_CACHE_NAME, ignite.name());

        counter.update(11, 6);

        forceCheckpoint();

        doSleep(1000);

        stopAllGrids();

        // 3rd start - to wait for checkpoint and fail it with assertion error.
        ignite = startGrid(0);

        ((GridCacheDatabaseSharedManager)ignite.context().cache().context().database()).addCheckpointListener(new Lsnr());

        ignite.cluster().state(ClusterState.ACTIVE);

        t = true;

        try {
            forceCheckpoint();

            doSleep(1000);
        }
        catch (Throwable e) {

        }

        stopAllGrids();

        // 4th start - now next checkpoint will cause Missing tails.
        ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        counter = counter(0, DEFAULT_CACHE_NAME, ignite.name());

        counter.update(12, 6);

        forceCheckpoint();

        doSleep(1000);
    }

    private void writeLongToFileByOffset(String path, long offset, long val) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(8);

        buf.putLong(val);

        buf.rewind();

        try (FileChannel c = new RandomAccessFile(new File(path), "rw").getChannel()) {
            c.position(offset);

            c.write(buf);
        }
    }

    private long readLongFromFileByOffset(String path, long offset, long val) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(8);

        buf.order(ByteOrder.LITTLE_ENDIAN);

        buf.rewind();

        try (FileChannel c = new RandomAccessFile(new File(path), "rw").getChannel()) {
            c.position(offset);

            c.read(buf);
        }

        buf.rewind();

        return buf.getLong();
    }

    /**
     * Allocates buffer and does some work in closure, then frees the buffer.
     *
     * @param c Closure.
     * @param <T> Result type.
     * @return Result of closure.
     * @throws IgniteCheckedException If failed.
     */
    protected <T> T doWithBuffer(BufferClosure<T> c) throws IgniteCheckedException {
        ByteBuffer buf = allocateBuffer(PAGE_SIZE);

        try {
            long addr = bufferAddress(buf);

            return c.apply(buf, addr);
        }
        finally {
            freeBuffer(buf);
        }
    }

    /**
     * Scans given file page store and executes closure for each page.
     *
     * @param partId Partition id.
     * @param flag Flag.
     * @param store Page store.
     * @param c Closure that accepts page id, page address, page IO. If it returns false, scan stops.
     * @return List of errors that occured while scanning.
     * @throws IgniteCheckedException If failed.
     */
    private List<Throwable> scanFileStore(int partId, byte flag, FilePageStore store, GridClosure3<Long, Long, PageIO, Boolean> c)
        throws IgniteCheckedException {
        return doWithBuffer((buf, addr) -> {
            List<Throwable> errors = new ArrayList<>();

            long pagesNum = isNull(store) ? 0 : (store.size() - store.headerSize()) / PAGE_SIZE;

            for (int i = 0; i < pagesNum; i++) {
                buf.rewind();

                try {
                    long pageId = PageIdUtils.pageId(partId, flag, i);

                    readPage(store, pageId, buf);

                    PageIO io = PageIO.getPageIO(addr);

                    if (!c.apply(pageId, addr, io))
                        break;
                }
                catch (Throwable e) {
                    String err = "Exception occurred on step " + i + ": " + e.getMessage();

                    errors.add(new IgniteException(err, e));
                }
            }

            return errors;
        });
    }

    /**
     * Reading pages into buffer.
     *
     * @param store Source for reading pages.
     * @param pageId Page ID.
     * @param buf Buffer.
     */
    protected void readPage(FilePageStore store, long pageId, ByteBuffer buf) throws IgniteCheckedException {
        try {
            store.read(pageId, buf, false);
        }
        catch (IgniteDataIntegrityViolationException | IllegalArgumentException e) {
            // Replacing exception due to security reasons, as IgniteDataIntegrityViolationException prints page content.
            // Catch IllegalArgumentException for output page information.
            throw new IgniteException("Failed to read page, id=" + pageId + ", idx=" + pageIndex(pageId) +
                ", file=" + store.getFileAbsolutePath());
        }
    }

    /**
     * Finds certain pages in file page store. When all pages corresponding given types is found, at least one page
     * for each type, we return the result.
     *
     * @param partId Partition id.
     * @param flag Page store flag.
     * @param store File page store.
     * @param pageTypesIn Page types to find.
     * @return Map of found pages. First page of this class that was found, is put to this map.
     * @throws IgniteCheckedException If failed.
     */
    protected Map<Short, Long> findPages(int partId, byte flag, FilePageStore store, Set<Short> pageTypesIn)
        throws IgniteCheckedException {
        Map<Short, Long> res = new HashMap<>();

        Set<Short> pageTypes = new HashSet<>();

        pageTypes.addAll(pageTypesIn);

        scanFileStore(partId, flag, store, (pageId, addr, io) -> {
            if (pageTypes.contains((short)io.getType())) {
                res.put((short)io.getType(), pageId);

                pageTypes.remove((short)io.getType());
            }

            return !pageTypes.isEmpty();
        });

        return res;
    }

    /**
     *
     */
    protected interface BufferClosure<T> {
        /** */
        T apply(ByteBuffer buf, Long addr) throws IgniteCheckedException;
    }

    public static class Lsnr implements CheckpointListener {

        @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {

        }

        @Override public void onAfterMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
            if (t)
                throw new AssertionError("test");
        }

        @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {

        }

        @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {

        }
    }
}
