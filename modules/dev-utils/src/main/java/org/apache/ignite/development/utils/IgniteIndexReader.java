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
package org.apache.ignite.development.utils;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.DataRegionMetricsProvider;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointWriteProgressSupplier;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorage;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreV2;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneIgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.io.AbstractH2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.logger.NullLogger;

import static org.apache.ignite.internal.metric.IoStatisticsType.SORTED_INDEX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO.ITEMS_OFF;
import static org.apache.ignite.internal.util.IgniteUtils.checkpointBufferSize;

public class IgniteIndexReader {
    static {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS, H2MvccInnerIO.VERSIONS, H2MvccLeafIO.VERSIONS);

        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();
    }

    private GridKernalContext kernalContext() throws IgniteCheckedException {
        return new StandaloneGridKernalContext(
            new NullLogger(),
            null,
            null
        );
    }

    /** */
    private long[] calculateFragmentSizes(int concLvl, long cacheSize, long chpBufSize) {
        if (concLvl < 2)
            concLvl = Runtime.getRuntime().availableProcessors();

        long fragmentSize = cacheSize / concLvl;

        if (fragmentSize < 1024 * 1024)
            fragmentSize = 1024 * 1024;

        long[] sizes = new long[concLvl + 1];

        for (int i = 0; i < concLvl; i++)
            sizes[i] = fragmentSize;

        sizes[concLvl] = chpBufSize;

        return sizes;
    }

    private IgnitePageStoreManager storeManager(GridKernalContext kernalContext) {
        return new FilePageStoreManager(kernalContext);
    }

    public void indexRead(String cacheWorkDirPath, int grpId, int partCnt, int pageSize, boolean encrypted, int filePageStoreVer) throws IgniteCheckedException {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setPageSize(pageSize);

        File cacheWorkDir = new File(cacheWorkDirPath);

        CacheConfiguration cacheCfg = new CacheConfiguration();

        AtomicReference<GridCacheProcessor> cacheProcRef = new AtomicReference<>(null);

        GridKernalContext ctx = new StandaloneGridKernalContext(
            new NullLogger(),
            null,
            null
        ) {
            @Override public GridCacheProcessor cache() {
                return cacheProcRef.get();
            }
        };

        FilePageStoreManager storeManager = new FilePageStoreManager(ctx);

        storeManager.setPageStoreFileIOFactories(new AsyncFileIOFactory(), new AsyncFileIOFactory());

        storeManager.initDir(cacheWorkDir, grpId, partCnt, new LongAdderMetric("n", "d"), encrypted);

        StandaloneIgniteCacheDatabaseSharedManager dbMgr = new StandaloneIgniteCacheDatabaseSharedManager();

        dbMgr.setPageSize(pageSize);

        GridCacheSharedContext cacheSharedContext = new GridCacheSharedContext<>(
            ctx, null, null, null,
            storeManager, null, null, dbMgr, null,
            null, null, null, null, null,
            null, null, null, null, null, null
        );

        DataRegionConfiguration plcCfg = dsCfg.getDefaultDataRegionConfiguration();

        AtomicInteger commonCntr = new AtomicInteger(); // Fake counter.

        PageMemory pageMemory = new PageMemoryImpl(
            new UnsafeMemoryProvider(new NullLogger()),
            calculateFragmentSizes(dsCfg.getConcurrencyLevel(), plcCfg.getMaxSize(), checkpointBufferSize(plcCfg)),
            cacheSharedContext,
            pageSize,
            (fullId, pageBuf, tag) -> {
            },
            new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
                @Override public void applyx(Long aLong, FullPageId id, PageMemoryEx ex) throws IgniteCheckedException {

                }
            },
            () -> false,
            new DataRegionMetricsImpl(
                plcCfg,
                new GridMetricManager(ctx),
                new DataRegionMetricsProvider() {
                    @Override public long partiallyFilledPagesFreeSpace() {
                        return 0;
                    }

                    @Override public long emptyDataPages() {
                        return 0;
                    }
                }
            ),
            PageMemoryImpl.ThrottlingPolicy.DISABLED,
            new CheckpointWriteProgressSupplier() {
                @Override public AtomicInteger writtenPagesCounter() {
                    return commonCntr;
                }

                @Override public AtomicInteger syncedPagesCounter() {
                    return commonCntr;
                }

                @Override public AtomicInteger evictedPagesCntr() {
                    return commonCntr;
                }

                @Override public int currentCheckpointPagesCount() {
                    return 0;
                }
            }
        );

        ReuseList reuseList = new ReuseListImpl(
            grp.groupId(),
            grp.cacheOrGroupName(),
            grp.dataRegion().pageMemory(),
            ctx.wal(),
            reuseListRoot.pageId().pageId(),
            reuseListRoot.isAllocated(),
            diagnosticMgr.pageLockTracker().createPageLockTracker(reuseListName),
            ctx.kernalContext(),
            pageListCacheLimit
        );

        IndexStorage idxStorage = new IndexStorageImpl(
            pageMemory,
            new FileWriteAheadLogManager(ctx),
            new AtomicLong(0),
            grpId,

            );

        for (String idxName : idxStorage.getIndexNames()) {
            RootPage rootPage = idxStorage.allocateIndex(idxName);

            String treeName = idxName;

            GridCacheContext cctx = grpCtx.caches().get(0); // todo

            IoStatisticsHolderIndex stats = new IoStatisticsHolderIndex(
                SORTED_INDEX,
                cctx.name(),
                idxName,
                cctx.kernalContext().metric(),
                cctx.group().statisticsHolderData()
            );

            H2Tree tree = new H2Tree(
                cctx,
                null,
                treeName,
                idxName,
                cctx.name(),
                null,
                cctx.offheap().reuseListForIndex(treeName),
                CU.cacheGroupId(cctx.name(), cctx.group().name()),
                cctx.group().name(),
                cctx.dataRegion().pageMemory(),
                ctx.cache().context().wal(),
                cctx.offheap().globalRemoveId(),
                rootPage.pageId().pageId(),
                rootPage.isAllocated(),
                Collections.emptyList(),
                Collections.emptyList(),
                new AtomicInteger(0),
                false,
                false,
                false,
                null,
                ctx.failure(),
                null,
                stats,
                null,
                0
            );
        }
    }

    private Path getPartitionFilePath(File cacheWorkDir, int partId) {
        return new File(cacheWorkDir, String.format(PART_FILE_TEMPLATE, partId)).toPath();
    }

    private void indexReadWithoutPageMem(String cacheWorkDirPath, int partCnt, int pageSize) throws IgniteCheckedException {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setPageSize(pageSize);

        File idxFile = new File(cacheWorkDirPath, INDEX_FILE_NAME);

        File cacheWorkDir = new File(cacheWorkDirPath);

        long fileSize = idxFile.length();

        Set<String> ioClasses = new HashSet<>();

        FilePageStore idxPageStore = new FilePageStoreV2(
            PageMemory.FLAG_IDX,
            () -> idxFile.toPath(),
            new AsyncFileIOFactory(),
            dsCfg,
            new LongAdderMetric("name", "desc")
        );

        List<FilePageStore> partPageStores = new ArrayList<>(partCnt);

        for (int i = 0; i < partCnt; i++) {
            int partId = i;

            partPageStores.add(
                new FilePageStoreV2(
                    FLAG_DATA,
                    () -> getPartitionFilePath(cacheWorkDir, partId),
                    new AsyncFileIOFactory(),
                    dsCfg,
                    new LongAdderMetric("name", "desc")
                )
            );
        }

        for (int i = 0; i < (fileSize - idxPageStore.headerSize()) / pageSize; i++) {
            ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

            try {
                long addr = GridUnsafe.bufferAddress(buf);

                idxPageStore.readByOffset(i * pageSize + idxPageStore.headerSize(), buf, false);

                try {
                    PageIO io = PageIO.getPageIO(addr);

                    if (io instanceof AbstractH2ExtrasLeafIO) {
                        for (int j = 0; j < (pageSize - ITEMS_OFF) / ((BPlusIO)io).getItemSize(); j++) {
                            long link = ((AbstractH2ExtrasLeafIO) io).getLink(addr, j);

                            long pageId = pageId(link);

                            int partId = partId(pageId);

                            int itemId = itemId(link);

                            ByteBuffer dataBuf = GridUnsafe.allocateBuffer(pageSize);

                            long dataBufAddr = GridUnsafe.bufferAddress(dataBuf);

                            try {
                                partPageStores.get(partId).read(pageId, dataBuf, false);

                                PageIO dataIo = PageIO.getPageIO(dataBuf);

                                if (dataIo instanceof DataPageIO) {
                                    DataPageIO dataPageIO = (DataPageIO)dataIo;

                                    DataPagePayload payload = dataPageIO.readPayload(dataBufAddr, itemId, pageSize);

                                    int payloadOff = dataPageIO.getPayloadOffset(dataBufAddr, itemId, pageSize, payload.payloadSize());

                                    int cacheId = PageUtils.getInt(dataBufAddr, payloadOff + 6);

                                    //System.out.println(cacheId);
                                }

                                //System.out.println(dataIo.getClass().getSimpleName());
                            }
                            finally {
                                GridUnsafe.freeBuffer(dataBuf);
                            }
                        }

                    }

                    ioClasses.add(io.getClass().getSimpleName());
                }
                catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
            finally {
                GridUnsafe.freeBuffer(buf);
            }
        }

        ioClasses.forEach(System.out::println);
    }

    private void findLostRootPages(String cacheWorkDirPath, int partCnt, int pageSize, int filePageStoreVer) throws IgniteCheckedException {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setPageSize(pageSize);

        File cacheWorkDir = new File(cacheWorkDirPath);

        File idxFile = new File(cacheWorkDir, INDEX_FILE_NAME);

        if (!idxFile.exists())
            throw new RuntimeException("File not found: " + idxFile.getPath());
        else
            System.out.println("Analyzing file: " + idxFile.getPath());

        long fileSize = idxFile.length();

        FilePageStore idxPageStore = filePageStoreVer == 1
            ? new FilePageStore(
                PageMemory.FLAG_IDX,
                () -> idxFile.toPath(),
                new AsyncFileIOFactory(),
                dsCfg,
                new LongAdderMetric("name", "desc")
            )
            : new FilePageStoreV2(
            PageMemory.FLAG_IDX,
            () -> idxFile.toPath(),
            new AsyncFileIOFactory(),
            dsCfg,
            new LongAdderMetric("name", "desc")
        );

        Set<Long> treeMetaPageIds = new HashSet<>();
        Set<Long> bPlusMetaIds = new HashSet<>();

        for (int i = 0; i < (fileSize - idxPageStore.headerSize()) / pageSize; i++) {
            ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

            try {
                long addr = GridUnsafe.bufferAddress(buf);

                idxPageStore.readByOffset(i * pageSize + idxPageStore.headerSize(), buf, false);

                PageIO io = PageIO.getPageIO(addr);

                if (io instanceof PageMetaIO) {
                    PageMetaIO pageMetaIO = (PageMetaIO)io;

                    treeMetaPageIds.add(pageMetaIO.getTreeRoot(addr));
                }
                else if (io instanceof IndexStorageImpl.MetaStoreLeafIO) {
                    IndexStorageImpl.MetaStoreLeafIO metaStoreLeafIO = (IndexStorageImpl.MetaStoreLeafIO)io;

                    for (int j = 0; j < (pageSize - ITEMS_OFF) / metaStoreLeafIO.getItemSize(); j++) {
                        IndexStorageImpl.IndexItem indexItem = metaStoreLeafIO.getLookupRow(null, addr, j);

                        if (indexItem.pageId() != 0) {
                            ByteBuffer idxMetaBuf = GridUnsafe.allocateBuffer(pageSize); // for tree meta page

                            try {
                                long idxMetaBufAddr = GridUnsafe.bufferAddress(idxMetaBuf);

                                idxPageStore.read(indexItem.pageId(), idxMetaBuf, false);

                                treeMetaPageIds.add(indexItem.pageId());
                            }
                            finally {
                                GridUnsafe.freeBuffer(idxMetaBuf);
                            }
                        }
                    }
                }
                else if (io instanceof BPlusMetaIO) {
                    BPlusMetaIO bPlusMetaIO = (BPlusMetaIO)io;

                    long pageId = PageIdUtils.pageId(INDEX_PARTITION, FLAG_IDX, i);

                    bPlusMetaIds.add(pageId);
                }

                //System.out.println(io.getClass().getSimpleName());
            }
            finally {
                GridUnsafe.freeBuffer(buf);
            }
        }

        System.out.println("---Meta tree entries without actual index trees:");

        for (Long id : treeMetaPageIds) {
            if (!bPlusMetaIds.contains(id))
                System.out.println(id);
        }

        System.out.println("---");
        System.out.println();
        System.out.println("---Index root pages missing in meta tree: ");

        for (Long id : bPlusMetaIds) {
            if (!treeMetaPageIds.contains(id))
                System.out.println(id);
        }

        System.out.println("---");
    }

    public static void main(String[] args) throws IgniteCheckedException {
        try {
            String dir = args[0];

            int partCnt = args.length > 1 ? Integer.parseInt(args[1]) : 1024;

            int pageSize = args.length > 2 ? Integer.parseInt(args[2]) : 4096;

            int filePageStoreVer = args.length > 3 ? Integer.parseInt(args[3]) : 2;

            new IgniteIndexReader().findLostRootPages(dir, partCnt, pageSize, filePageStoreVer);
        }
        catch (Exception e) {
            System.out.println("options: path [partCnt] [pageSize] [filePageStoreVersion]");

            throw e;
        }
    }
}
