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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.development.utils.arguments.CLIArgument;
import org.apache.ignite.development.utils.arguments.CLIArgumentParser;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.tree.PendingRowIO;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.collection.BitSetIntSet;
import org.apache.ignite.internal.util.collection.IntSet;
import org.apache.ignite.internal.util.lang.GridClosure3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.development.utils.IgniteIndexReader.Args.CHECK_PARTS;
import static org.apache.ignite.development.utils.IgniteIndexReader.Args.DEST;
import static org.apache.ignite.development.utils.IgniteIndexReader.Args.DEST_FILE;
import static org.apache.ignite.development.utils.IgniteIndexReader.Args.DIR;
import static org.apache.ignite.development.utils.IgniteIndexReader.Args.FILE_MASK;
import static org.apache.ignite.development.utils.IgniteIndexReader.Args.INDEXES;
import static org.apache.ignite.development.utils.IgniteIndexReader.Args.PAGE_SIZE;
import static org.apache.ignite.development.utils.IgniteIndexReader.Args.PAGE_STORE_VER;
import static org.apache.ignite.development.utils.IgniteIndexReader.Args.PART_CNT;
import static org.apache.ignite.development.utils.IgniteIndexReader.Args.TRANSFORM;
import static org.apache.ignite.development.utils.arguments.CLIArgument.mandatoryArg;
import static org.apache.ignite.development.utils.arguments.CLIArgument.optionalArg;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getType;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getVersion;
import static org.apache.ignite.internal.util.GridUnsafe.allocateBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.GridUnsafe.freeBuffer;

/**
 * Offline reader for index files.
 */
public class IgniteIndexReader implements AutoCloseable {
    /** */
    private static final String META_TREE_NAME = "MetaTree";

    /** */
    private static final String FROM_ROOT_TO_LEAFS_TRAVERSE_NAME = "<FROM_ROOT> ";

    /** */
    private static final String HORIZONTAL_SCAN_NAME = "<HORIZONTAL> ";

    /** */
    private static final Pattern IDX_NAME_SEACH_PATTERN = Pattern.compile("idxName=(?<name>.*?)##");

    /** */
    private static final Pattern CACHE_ID_SEACH_PATTERN = Pattern.compile("(?<id>[-0-9]{1,15})_[-0-9]{1,15}_.*");

    /** */
    private static final int CHECK_PARTS_MAX_ERRORS_PER_PARTITION = 10;

    /** */
    static {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS, H2MvccInnerIO.VERSIONS, H2MvccLeafIO.VERSIONS);

        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();
    }

    /** */
    private final int pageSize;

    /** */
    private final int partCnt;

    /** */
    private final File cacheWorkDir;

    /** */
    private final DataStorageConfiguration dsCfg;

    /** */
    private final FileVersionCheckingFactory storeFactory;

    /** */
    private final LongAdderMetric allocationTracker = new LongAdderMetric("n", "d");

    /** */
    private final Set<String> indexes;

    /** */
    private final PrintStream outStream;

    /** */
    private final PrintStream outErrStream;

    /** */
    private final File idxFile;

    /** */
    private final FilePageStore idxStore;

    /** */
    private final FilePageStore[] partStores;

    /** */
    private final boolean checkParts;

    /** */
    private final Set<Integer> missingPartitions = new HashSet<>();

    /** */
    private PageIOProcessor innerPageIOProcessor = new InnerPageIOProcessor();

    /** */
    private PageIOProcessor leafPageIOProcessor = new LeafPageIOProcessor();

    /** Mapping IO classes to their processors. */
    private final Map<Class, PageIOProcessor> ioProcessorsMap = new HashMap<Class, PageIOProcessor>() {{
        put(BPlusMetaIO.class, new MetaPageIOProcessor());
        put(BPlusInnerIO.class, innerPageIOProcessor);
        put(H2ExtrasInnerIO.class, innerPageIOProcessor);
        put(IndexStorageImpl.MetaStoreInnerIO.class, innerPageIOProcessor);
        put(BPlusLeafIO.class, leafPageIOProcessor);
        put(H2ExtrasLeafIO.class, leafPageIOProcessor);
        put(IndexStorageImpl.MetaStoreLeafIO.class, leafPageIOProcessor);
    }};

    /** */
    public IgniteIndexReader(
        String cacheWorkDirPath,
        int pageSize,
        int partCnt,
        int filePageStoreVer,
        String[] indexes,
        boolean checkParts,
        OutputStream outputStream
    ) throws IgniteCheckedException {
        this.pageSize = pageSize;
        this.partCnt = partCnt;
        this.dsCfg = new DataStorageConfiguration().setPageSize(pageSize);
        this.cacheWorkDir = new File(cacheWorkDirPath);
        this.checkParts = checkParts;
        this.indexes = indexes == null ? null : new HashSet<>(asList(indexes));
        this.storeFactory = new FileVersionCheckingFactory(new AsyncFileIOFactory(), new AsyncFileIOFactory(), dsCfg) {
            /** {@inheritDoc} */
            @Override public int latestVersion() {
                return filePageStoreVer;
            }
        };

        if (outputStream == null) {
            outStream = System.out;
            outErrStream = System.out;
        }
        else {
            this.outStream = new PrintStream(outputStream);
            this.outErrStream = outStream;
        }

        idxFile = getFile(INDEX_PARTITION);

        if (idxFile == null)
            throw new RuntimeException("index.bin file not found");

        idxStore = getIdxStore();

        partStores = new FilePageStore[partCnt];

        if (idxStore != null) {
            for (int i = 0; i < partCnt; i++) {
                final File file = getFile(i);

                // Some of array members will be null if node doesn't have all partition files locally.
                if (file != null)
                    partStores[i] = (FilePageStore)storeFactory.createPageStore(FLAG_DATA, file, allocationTracker);
            }
        }
    }

    /** */
    private FilePageStore getIdxStore() throws IgniteCheckedException {
        try {
            return (FilePageStore)storeFactory.createPageStore(FLAG_IDX, idxFile, allocationTracker);
        }
        catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** */
    private void print(String s) {
        outStream.println(s);
    }

    /** */
    private void printErr(String s) {
        outErrStream.println(s);
    }

    /** */
    private void printStackTrace(Throwable e) {
        OutputStream os = new StringBuilderOutputStream();

        e.printStackTrace(new PrintStream(os));

        printErr(os.toString());
    }

    /** */
    private static long normalizePageId(long pageId) {
        return pageId(partId(pageId), flag(pageId), pageIndex(pageId));
    }

    /** */
    private File getFile(int partId) {
        File file =  new File(cacheWorkDir, partId == INDEX_PARTITION ? INDEX_FILE_NAME : String.format(PART_FILE_TEMPLATE, partId));

        if (!file.exists())
            return null;
        else if (partId == -1)
            print("Analyzing file: " + file.getPath());

        return file;
    }

    /**
     * Read index file.
     */
    public void readIdx() {
        long partPageStoresNum = Arrays.stream(partStores)
            .filter(Objects::nonNull)
            .count();

        print("Partitions files num: " + partPageStoresNum);

        Map<Class<? extends PageIO>, Long> pageClasses = new HashMap<>();

        long pagesNum = idxStore == null ? 0 : (idxFile.length() - idxStore.headerSize()) / pageSize;

        print("Going to check " + pagesNum + " pages.");

        Map<Class, Set<Long>> pageIoIds = new HashMap<>();

        AtomicReference<Map<String, TreeTraversalInfo>> treeInfo = new AtomicReference<>();

        AtomicReference<Map<String, TreeTraversalInfo>> horizontalScans = new AtomicReference<>();

        AtomicReference<PageListsInfo> pageListsInfo = new AtomicReference<>();

        List<Throwable> errors = new LinkedList<>();

        ProgressPrinter progressPrinter = new ProgressPrinter(System.out, "Reading pages sequentially", pagesNum);

        try {
            scanFileStore(INDEX_PARTITION, FLAG_IDX, idxStore, (pageId, addr, io) -> {
                progressPrinter.printProgress();

                pageClasses.merge(io.getClass(), 1L, (oldVal, newVal) -> ++oldVal);

                if (io instanceof PageMetaIO) {
                    PageMetaIO pageMetaIO = (PageMetaIO)io;

                    long metaTreeRootId = normalizePageId(pageMetaIO.getTreeRoot(addr));

                    treeInfo.set(traverseAllTrees("Index trees traversal", metaTreeRootId, CountOnlyStorage::new, this::traverseTree));

                    treeInfo.get().forEach((name, info) -> {
                        info.innerPageIds.forEach(id -> {
                            Class cls = name.equals(META_TREE_NAME)
                                ? IndexStorageImpl.MetaStoreInnerIO.class
                                : H2ExtrasInnerIO.class;

                            pageIoIds.computeIfAbsent(cls, k -> new HashSet<>()).add(id);
                        });

                        pageIoIds.computeIfAbsent(BPlusMetaIO.class, k -> new HashSet<>()).add(info.rootPageId);
                    });

                    Supplier<ItemStorage> itemStorageFactory = checkParts ? LinkStorage::new : CountOnlyStorage::new;

                    horizontalScans.set(traverseAllTrees("Scan index trees horizontally", metaTreeRootId, itemStorageFactory, this::horizontalTreeScan));
                }
                else if (io instanceof PagesListMetaIO)
                    pageListsInfo.set(getPageListsMetaInfo(pageId));
                else {
                    ofNullable(pageIoIds.get(io.getClass())).ifPresent((pageIds) -> {
                        if (!pageIds.contains(pageId)) {
                            boolean foundInList =
                                (pageListsInfo.get() != null && pageListsInfo.get().allPages.contains(pageId));

                            throw new IgniteException(
                                "Possibly orphan " + io.getClass().getSimpleName() + " page, pageId=" + pageId +
                                    (foundInList ? ", it has been found in page list." : "")
                            );
                        }
                    });
                }

                return true;
            });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("index.bin scan problem", e);
        }

        if (treeInfo == null)
            printErr("No tree meta info found.");
        else {
            printTraversalResults(FROM_ROOT_TO_LEAFS_TRAVERSE_NAME, treeInfo.get());

            printTraversalResults(HORIZONTAL_SCAN_NAME, horizontalScans.get());
        }

        compareTraversals(treeInfo.get(), horizontalScans.get());

        if (pageListsInfo.get() == null)
            printErr("No page lists meta info found.");
        else
            printPagesListsInfo(pageListsInfo.get());

        print("\n---These pages types were encountered during sequential scan:");
        pageClasses.forEach((key, val) -> print(key.getSimpleName() + ": " + val));

        if (!errors.isEmpty()) {
            printErr("---");
            printErr("Errors:");

            errors.forEach(this::printStackTrace);
        }

        print("---");
        print("Total pages encountered during sequential scan: " + pageClasses.values().stream().mapToLong(a -> a).sum());
        print("Total errors occurred during sequential scan: " + errors.size());
        print("Note that some pages can be occupied by meta info, tracking info, etc., so total page count can differ " +
            "from count of pages found in index trees and page lists.");

        if (checkParts) {
            List<Throwable> checkPartsErrors = checkParts(horizontalScans.get());

            print("");

            checkPartsErrors.forEach(e -> printErr("<ERROR> " + e.getMessage()));

            print("\nPartition check finished, total errors: " + checkPartsErrors.size());
        }
    }

    /**
     * Allocates buffer and does some work in closure, then frees the buffer.
     *
     * @param c Closure.
     * @param <T> Result type.
     * @return Result of closure.
     * @throws IgniteCheckedException If failed.
     */
    private <T> T doWithBuffer(BufferClosure<T> c) throws IgniteCheckedException {
        ByteBuffer buf = allocateBuffer(pageSize);

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
            List<Throwable> errors = new LinkedList<>();

            long size = new File(store.getFileAbsolutePath()).length();

            long pagesNum = store == null ? 0 : (size - store.headerSize()) / pageSize;

            for (int i = 0; i < pagesNum; i++) {
                buf.rewind();

                try {
                    long pageId = PageIdUtils.pageId(partId, flag, i);

                    store.read(pageId, buf, false);

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

    /** */
    private List<Throwable> checkParts(Map<String, TreeTraversalInfo> aTreesInfo) {
        System.out.println();

        List<Throwable> errors = new LinkedList<>();

        Map<String, TreeTraversalInfo> treesInfo = new HashMap<>(aTreesInfo);

        treesInfo.remove(META_TREE_NAME);

        ProgressPrinter progressPrinter = new ProgressPrinter(System.out, "Checking partitions", partCnt);

        for (int i = 0; i < partCnt; i++) {
            progressPrinter.printProgress();

            FilePageStore partStore = partStores[i];

            if (partStore == null)
                continue;

            AtomicInteger partErrCnt = new AtomicInteger(0);

            final int partId = i;

            try {
                Map<Class, Long> metaPages = findPages(i, FLAG_DATA, partStore, singleton(PagePartitionMetaIOV2.class));

                long partMetaId = metaPages.get(PagePartitionMetaIOV2.class);

                doWithBuffer((buf, addr) -> {
                    partStore.read(partMetaId, buf, false);

                    PagePartitionMetaIOV2 partMetaIO = PageIO.getPageIO(addr);

                    long cacheDataTreeRoot = partMetaIO.getTreeRoot(addr);

                    TreeTraversalInfo cacheDataTreeInfo = horizontalTreeScan(partStore, cacheDataTreeRoot, new ItemsListStorage());

                    for (Object dataTreeItem : cacheDataTreeInfo.itemStorage) {
                        CacheAwareLink cacheAwareLink = (CacheAwareLink)dataTreeItem;

                        for (Map.Entry<String, TreeTraversalInfo> e : treesInfo.entrySet()) {
                            String name = e.getKey();

                            TreeTraversalInfo tree = e.getValue();

                            int cacheId = getCacheId(name);

                            if (cacheId != cacheAwareLink.cacheId)
                                continue; // It's index for other cache, don't check.

                            if (!tree.itemStorage.contains(cacheAwareLink)) {
                                errors.add(new IgniteException(cacheDataTreeEntryMissingError(name, cacheAwareLink)));

                                partErrCnt.incrementAndGet();
                            }
                        }

                        if (partErrCnt.get() >= CHECK_PARTS_MAX_ERRORS_PER_PARTITION) {
                            errors.add(new IgniteException("Too many errors (" + CHECK_PARTS_MAX_ERRORS_PER_PARTITION +
                                ") found for partId=" + partId + ", stopping analysis for this partition."));

                            break;
                        }
                    }

                    return null;
                });
            }
            catch (IgniteCheckedException e) {
                errors.add(new IgniteException("Partition check failed, partId=" + i, e));
            }
        }

        return errors;
    }

    /** */
    private String cacheDataTreeEntryMissingError(String treeName, CacheAwareLink cacheAwareLink) {
        long link = cacheAwareLink.link;

        long pageId = pageId(link);

        int itemId = itemId(link);

        int partId = partId(pageId);

        int pageIdx = pageIndex(pageId);

        return "Entry is missing in index: " + treeName +
            ", cacheId=" + cacheAwareLink.cacheId + ", partId=" + partId +
            ", pageIndex=" + pageIdx + ", itemId=" + itemId + ", link=" + link;
    }

    /** */
    private Map<Class, Long> findPages(int partId, byte flag, FilePageStore store, Set<Class> pageTypes)
        throws IgniteCheckedException {
        Map<Class, Long> res = new HashMap<>();

        scanFileStore(partId, flag, store, (pageId, addr, io) -> {
            if (pageTypes.contains(io.getClass())) {
                res.put(io.getClass(), pageId);

                pageTypes.remove(io.getClass());
            }

            return !pageTypes.isEmpty();
        });

        return res;
    }

    /**
     * Compares result of traversals.
     *
     * @param treeInfos Traversal from root to leafs.
     * @param treeScans Traversal using horizontal scan.
     */
    private void compareTraversals(Map<String, TreeTraversalInfo> treeInfos, Map<String, TreeTraversalInfo> treeScans) {
        List<String> errors = new LinkedList<>();

        Set<String> treeIdxNames = new HashSet<>();

        treeInfos.forEach((name, tree) -> {
            treeIdxNames.add(name);

            TreeTraversalInfo scan = treeScans.get(name);

            if (scan == null) {
                errors.add("Tree was detected in " + FROM_ROOT_TO_LEAFS_TRAVERSE_NAME + " but absent in  "
                    + HORIZONTAL_SCAN_NAME + ": " + name);

                return;
            }

            if (tree.itemStorage.size() != scan.itemStorage.size())
                errors.add(compareError("items", name, tree.itemStorage.size(), scan.itemStorage.size(), null));

            Set<Class> classesInStat = new HashSet<>();

            tree.ioStat.forEach((cls, cnt) -> {
                classesInStat.add(cls);

                AtomicLong scanCnt = scan.ioStat.get(cls);

                if (scanCnt == null)
                    scanCnt = new AtomicLong(0);

                if (scanCnt.get() != cnt.get())
                    errors.add(compareError("pages", name, cnt.get(), scanCnt.get(), cls));
            });

            scan.ioStat.forEach((cls, cnt) -> {
                if (classesInStat.contains(cls))
                    // Already checked.
                    return;

                errors.add(compareError("pages", name, 0, cnt.get(), cls));
            });
        });

        treeScans.forEach((name, tree) -> {
            if (!treeIdxNames.contains(name))
                errors.add("Tree was detected in " + HORIZONTAL_SCAN_NAME + " but absent in  "
                    + FROM_ROOT_TO_LEAFS_TRAVERSE_NAME + ": " + name);
        });

        print("");

        errors.forEach(e -> printErr("<ERROR>" + e));
    }

    /** */
    private String compareError(String itemName, String idxName, long fromRoot, long scan, Class pageType) {
        return String.format(
            "Different count of %s; index: %s, %s:%s, %s:%s" + (pageType == null ? "" : ", pageType: " + pageType.getName()),
            itemName,
            idxName,
            FROM_ROOT_TO_LEAFS_TRAVERSE_NAME,
            fromRoot,
            HORIZONTAL_SCAN_NAME,
            scan
        );
    }

    /**
     * Gets meta info about page lists.
     * @param metaPageListId Page list meta id.
     * @return Meta info.
     */
    private PageListsInfo getPageListsMetaInfo(long metaPageListId) {
        Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData = new HashMap<>();

        Set<Long> allPages = new HashSet<>();

        Map<Class, AtomicLong> pageListStat = new HashMap<>();

        Map<Long, Throwable> errors = new HashMap<>();

        try {
            doWithBuffer((buf, addr) -> {
                long nextMetaId = metaPageListId;

                while (nextMetaId != 0) {
                    try {
                        buf.rewind();

                        idxStore.read(nextMetaId, buf, false);

                        PagesListMetaIO io = PageIO.getPageIO(addr);

                        Map<Integer, GridLongList> data = new HashMap<>();

                        io.getBucketsData(addr, data);

                        final long fNextMetaId = nextMetaId;

                        data.forEach((k, v) -> {
                            List<Long> listIds = LongStream.of(v.array()).map(IgniteIndexReader::normalizePageId).boxed().collect(toList());

                            listIds.forEach(listId -> allPages.addAll(getPageList(listId, pageListStat)));

                            bucketsData.put(new IgniteBiTuple<>(fNextMetaId, k), listIds);
                        });

                        nextMetaId = io.getNextMetaPageId(addr);
                    }
                    catch (Exception e) {
                        errors.put(nextMetaId, e);

                        nextMetaId = 0;
                    }
                }

                return null;
            });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return new PageListsInfo(bucketsData, allPages, pageListStat, errors);
    }

    /**
     * Get single page list.
     * @param pageListStartId Id of the start page of the page list.
     * @param pageStat Page types statistics.
     * @return List of page ids.
     */
    private List<Long> getPageList(long pageListStartId, Map<Class, AtomicLong> pageStat) {
        List<Long> res = new LinkedList<>();

        long nextNodeId = pageListStartId;

        ByteBuffer nodeBuf = allocateBuffer(pageSize);
        ByteBuffer pageBuf = allocateBuffer(pageSize);

        long nodeAddr = bufferAddress(nodeBuf);
        long pageAddr = bufferAddress(pageBuf);

        try {
            while (nextNodeId != 0) {
                try {
                    nodeBuf.rewind();

                    idxStore.read(nextNodeId, nodeBuf, false);

                    PagesListNodeIO io = PageIO.getPageIO(nodeAddr);

                    for (int i = 0; i < io.getCount(nodeAddr); i++) {
                        pageBuf.rewind();

                        long pageId = normalizePageId(io.getAt(nodeAddr, i));

                        res.add(pageId);

                        idxStore.read(pageId, pageBuf, false);

                        PageIO pageIO = PageIO.getPageIO(pageAddr);

                        pageStat.computeIfAbsent(pageIO.getClass(), k -> new AtomicLong(0)).incrementAndGet();
                    }

                    nextNodeId = io.getNextId(nodeAddr);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e.getMessage(), e);
                }
            }
        }
        finally {
            freeBuffer(nodeBuf);
            freeBuffer(pageBuf);
        }

        return res;
    }

    /**
     * Traverse all trees in file and return their info.
     * @param metaTreeRoot Meta tree root page id.
     * @return Index trees info.
     */
    private Map<String, TreeTraversalInfo> traverseAllTrees(
        String traverseProcCaption,
        long metaTreeRoot,
        Supplier<ItemStorage> itemStorageFactory,
        GridClosure3<FilePageStore, Long, ItemStorage, TreeTraversalInfo> traverseProc
    ) {
        Map<String, TreeTraversalInfo> treeInfos = new HashMap<>();

        TreeTraversalInfo metaTreeTraversalInfo = traverseProc.apply(idxStore, metaTreeRoot, new ItemsListStorage());

        treeInfos.put(META_TREE_NAME, metaTreeTraversalInfo);

        ProgressPrinter progressPrinter =
            new ProgressPrinter(System.out, traverseProcCaption, metaTreeTraversalInfo.itemStorage.size());

        metaTreeTraversalInfo.itemStorage.forEach(item -> {
            progressPrinter.printProgress();

            IndexStorageImpl.IndexItem idxItem = (IndexStorageImpl.IndexItem)item;

            if (indexes != null && !indexes.contains(idxItem.nameString()))
                return;

            TreeTraversalInfo treeTraversalInfo =
                traverseProc.apply(idxStore, normalizePageId(idxItem.pageId()), itemStorageFactory.get());

            treeInfos.put(idxItem.toString(), treeTraversalInfo);
        });

        return treeInfos;
    }

    /**
     * Prints traversal info.
     * @param treeInfos Tree traversal info.
     */
    private void printTraversalResults(String prefix, Map<String, TreeTraversalInfo> treeInfos) {
        print("\nTree traversal results " + prefix);

        Map<Class, AtomicLong> totalStat = new HashMap<>();

        AtomicInteger totalErr = new AtomicInteger(0);

        // Map cacheId -> (map idxName -> size))
        Map<Integer, Map<String, Long>> cacheIdxSizes = new HashMap<>();

        treeInfos.forEach((idxName, validationInfo) -> {
            print(prefix + "-----");
            print(prefix + "Index tree: " + idxName);
            print(prefix + "-- Page stat:");

            validationInfo.ioStat.forEach((cls, cnt) -> {
                print(prefix + cls.getSimpleName() + ": " + cnt.get());

                totalStat.computeIfAbsent(cls, k -> new AtomicLong(0)).addAndGet(cnt.get());
            });

            print(prefix + "-- Count of items found in leaf pages: " + validationInfo.itemStorage.size());

            if (!validationInfo.errors.isEmpty()) {
                print(prefix + "<ERROR> -- Errors:");

                validationInfo.errors.forEach((id, errors) -> {
                    print(prefix + "Page id=" + id + ", exceptions:");

                    errors.forEach(this::printStackTrace);

                    totalErr.addAndGet(errors.size());
                });
            }
            else
                print(prefix + "No errors occurred while traversing.");

            cacheIdxSizes.computeIfAbsent(getCacheId(idxName), k -> new HashMap<>())
                .put(idxName, validationInfo.itemStorage.size());
        });

        print(prefix + "---");
        print(prefix + "Total page stat collected during trees traversal:");

        totalStat.forEach((cls, cnt) -> print(prefix + cls.getSimpleName() + ": " + cnt.get()));

        print("");
        print(prefix + "Total trees: " + treeInfos.keySet().size());
        print(prefix + "Total pages found in trees: " + totalStat.values().stream().mapToLong(AtomicLong::get).sum());
        print(prefix + "Total errors during trees traversal: " + totalErr.get());
        print("");

        cacheIdxSizes.forEach((cacheId, idxSizes) -> {
            if (idxSizes.values().stream().distinct().count() > 1) {
                print("<ERROR> Index size inconsistency: cacheId=" + cacheId);
                idxSizes.forEach((name, size) -> print("     Index name: " + name + ", size=" + size));
            }
        });

        print("------------------");
    }

    /**
     * Tries to get cache id from index name.
     *
     * @param name Index name.
     * @return Cache id, except of {@code 0} for cache groups with single cache and {@code -1} for meta tree.
     */
    private int getCacheId(String name) {
        Matcher mName = IDX_NAME_SEACH_PATTERN.matcher(name);

        if (!mName.find())
            return -1;

        String idxName = mName.group("name");

        Matcher mId = CACHE_ID_SEACH_PATTERN.matcher(idxName);

        if (mId.find()) {
            String id = mId.group("id");

            return Integer.parseInt(id);
        }

        return 0;
    }

    /**
     * Prints page lists info.
     * @param pageListsInfo Page lists info.
     */
    private void printPagesListsInfo(PageListsInfo pageListsInfo) {
        print("\n---Page lists info.");

        if (!pageListsInfo.bucketsData.isEmpty())
            print("---Printing buckets data:");

        pageListsInfo.bucketsData.forEach((bucket, bucketData) -> {
            GridStringBuilder sb = new GridStringBuilder()
                .a("List meta id=")
                .a(bucket.get1())
                .a(", bucket number=")
                .a(bucket.get2())
                .a(", lists=[")
                .a(bucketData.stream().map(String::valueOf).collect(joining(", ")))
                .a("]");

            print(sb.toString());
        });

        if (!pageListsInfo.allPages.isEmpty()) {
            print("-- Page stat:");

            pageListsInfo.pageListStat.forEach((cls, cnt) -> print(cls.getSimpleName() + ": " + cnt.get()));
        }

        if (!pageListsInfo.errors.isEmpty()) {
            print("---Errors:");

            pageListsInfo.errors.forEach((id, error) -> {
                printErr("Page id: " + id + ", exception: ");

                printStackTrace(error);
            });
        }

        print("");
        print("Total index pages found in lists: " + pageListsInfo.allPages.size());
        print("Total errors during lists scan: " + pageListsInfo.errors.size());
        print("------------------");
    }

    /**
     * Traverse single index tree from root to leafs.
     *
     * @param rootPageId Root page id.
     * @param itemStorage Items storage.
     * @return Tree traversal info.
     */
    private TreeTraversalInfo traverseTree(FilePageStore store, long rootPageId, ItemStorage itemStorage) {
        Map<Class, AtomicLong> ioStat = new HashMap<>();

        Map<Long, Set<Throwable>> errors = new HashMap<>();

        Set<Long> innerPageIds = new HashSet<>();

        PageCallback innerCb = (content, pageId) -> innerPageIds.add(normalizePageId(pageId));

        ItemCallback itemCb = (currPageId, item, link) -> itemStorage.add(item);

        getTreeNode(rootPageId, new TreeTraverseContext(store, ioStat, errors, innerCb, null, itemCb));

        return new TreeTraversalInfo(ioStat, errors, innerPageIds, rootPageId, itemStorage);
    }

    /**
     * Traverse single index tree by each level horizontally.
     *
     * @param rootPageId Root page id.
     * @param itemStorage Items storage.
     * @return Tree traversal info.
     */
    private TreeTraversalInfo horizontalTreeScan(FilePageStore store, long rootPageId, ItemStorage itemStorage) {
        Map<Long, Set<Throwable>> errors = new HashMap<>();

        Map<Class, AtomicLong> ioStat = new HashMap<>();

        TreeTraverseContext treeCtx = new TreeTraverseContext(store, ioStat, errors, null, null, null);

        ByteBuffer buf = allocateBuffer(pageSize);

        try {
            long addr = bufferAddress(buf);

            buf.rewind();

            store.read(rootPageId, buf, false);

            PageIO pageIO = PageIO.getPageIO(addr);

            if (!(pageIO instanceof BPlusMetaIO))
                throw new IgniteException("Root page is not meta, pageId=" + rootPageId);

            BPlusMetaIO metaIO = (BPlusMetaIO)pageIO;

            ioStat.computeIfAbsent(metaIO.getClass(), k -> new AtomicLong(0)).incrementAndGet();

            int lvlsCnt = metaIO.getLevelsCount(addr);

            long[] firstPageIds = IntStream.range(0, lvlsCnt).mapToLong(i -> metaIO.getFirstPageId(addr, i)).toArray();

            for (int i = 0; i < lvlsCnt; i++) {
                long pageId = firstPageIds[i];

                while (pageId > 0) {
                    try {
                        buf.rewind();

                        store.read(pageId, buf, false);

                        pageIO = PageIO.getPageIO(addr);

                        if (i == 0 && !(pageIO instanceof BPlusLeafIO))
                            throw new IgniteException("Not-leaf page found on leaf level, pageId=" + pageId + ", level=" + i);

                        if (!(pageIO instanceof BPlusIO))
                            throw new IgniteException("Not-BPlus page found, pageId=" + pageId + ", level=" + i);

                        ioStat.computeIfAbsent(pageIO.getClass(), k -> new AtomicLong(0)).incrementAndGet();

                        if (pageIO instanceof BPlusLeafIO) {
                            PageIOProcessor ioProcessor = getIOProcessor(pageIO);

                            PageContent pageContent = ioProcessor.getContent(pageIO, addr, pageId, treeCtx);

                            pageContent.items.forEach(itemStorage::add);
                        }

                        pageId = ((BPlusIO)pageIO).getForward(addr);
                    }
                    catch (Throwable e) {
                        errors.computeIfAbsent(pageId, k -> new HashSet<>()).add(e);

                        pageId = 0;
                    }
                }
            }
        }
        catch (Throwable e) {
            errors.computeIfAbsent(rootPageId, k -> new HashSet<>()).add(e);
        }
        finally {
            freeBuffer(buf);
        }

        return new TreeTraversalInfo(ioStat, errors, null, rootPageId, itemStorage);
    }

    /**
     * Gets tree node and all its children.
     *
     * @param pageId Page id, where tree node is located.
     * @param nodeCtx Tree traverse context.
     * @return Tree node.
     */
    private TreeNode getTreeNode(long pageId, TreeTraverseContext nodeCtx) {
        PageContent pageContent;

        PageIOProcessor ioProcessor;

        try {
            final ByteBuffer buf = allocateBuffer(pageSize);

            try {
                nodeCtx.store.read(pageId, buf, false);

                final long addr = bufferAddress(buf);

                final PageIO io = PageIO.getPageIO(addr);

                nodeCtx.ioStat.computeIfAbsent(io.getClass(), k -> new AtomicLong(0)).incrementAndGet();

                ioProcessor = getIOProcessor(io);

                pageContent = ioProcessor.getContent(io, addr, pageId, nodeCtx);
            }
            finally {
                freeBuffer(buf);
            }

            return ioProcessor.getNode(pageContent, pageId, nodeCtx);
        }
        catch (Throwable e) {
            nodeCtx.errors.computeIfAbsent(pageId, k -> new HashSet<>()).add(e);

            return new TreeNode(pageId, null, "exception: " + e.getMessage(), Collections.emptyList());
        }
    }

    /** */
    private PageIOProcessor getIOProcessor(PageIO io) {
        Class ioCls = io.getClass();

        PageIOProcessor ioProcessor = ioProcessorsMap.getOrDefault(io.getClass(), getDefaultIoProcessor(io));

        if (ioProcessor == null)
            throw new IgniteException("Unexpected page io: " + ioCls.getSimpleName());

        return ioProcessor;
    }

    /** */
    private PageIOProcessor getDefaultIoProcessor(PageIO io) {
        if (io instanceof BPlusInnerIO)
            return ioProcessorsMap.get(BPlusInnerIO.class);
        else if (io instanceof BPlusLeafIO)
            return ioProcessorsMap.get(BPlusLeafIO.class);
        else
            return null;
    }


    /** {@inheritDoc} */
    @Override public void close() throws StorageException {
        if (idxStore != null)
            idxStore.stop(false);

        for (FilePageStore store : partStores) {
            if (store != null)
                store.stop(false);
        }
    }

    /** */
    public void transform(String dest, String fileMask) {
        File destDir = new File(dest);

        if (!destDir.exists())
            destDir.mkdirs();

        try (DirectoryStream<Path> files = Files.newDirectoryStream(cacheWorkDir.toPath(), "*" + fileMask)) {
            for (Path f : files) {
                if (f.toString().toLowerCase().endsWith(fileMask))
                    copyFromStreamToFile(f.toFile(), new File(destDir.getPath(), f.getFileName().toString()));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private int copyFromStreamToFile(File idxIn, File idxOut) throws IOException, IgniteCheckedException {
        ByteBuffer readBuf = GridUnsafe.allocateBuffer(pageSize);

        try {
            readBuf.order(ByteOrder.nativeOrder());

            long readAddr = GridUnsafe.bufferAddress(readBuf);

            ByteBuffer hdrBuf = headerBuffer(FLAG_IDX, pageSize);

            try (FileChannel ch = FileChannel.open(idxOut.toPath(), WRITE, CREATE)) {
                int hdrSize = hdrBuf.limit();

                ch.write(hdrBuf, 0);

                int pageCnt = 0;

                FileChannel stream = new RandomAccessFile(idxIn, "r").getChannel();

                while (readNextPage(readBuf, stream, pageSize)) {
                    pageCnt++;

                    readBuf.rewind();

                    long pageId = PageIO.getPageId(readAddr);

                    assert pageId != 0;

                    int pageIdx = PageIdUtils.pageIndex(pageId);

                    int crcSaved = PageIO.getCrc(readAddr);
                    PageIO.setCrc(readAddr, 0);

                    int calced = FastCrc.calcCrc(readBuf, pageSize);

                    if (calced != crcSaved)
                        throw new IgniteCheckedException("Snapshot corrupted");

                    PageIO.setCrc(readAddr, crcSaved);

                    readBuf.rewind();

                    boolean changed = false;

                    int pageType = PageIO.getType(readAddr);

                    switch (pageType) {
                        case PageIO.T_PAGE_UPDATE_TRACKING:
                            PageHandler.zeroMemory(readAddr, TrackingPageIO.COMMON_HEADER_END,
                                readBuf.capacity() - TrackingPageIO.COMMON_HEADER_END);

                            changed = true;

                            break;

                        case PageIO.T_META:
                        case PageIO.T_PART_META:
                            PageMetaIO io = PageIO.getPageIO(pageType, PageIO.getVersion(readAddr));

                            io.setLastAllocatedPageCount(readAddr, 0);
                            io.setLastSuccessfulFullSnapshotId(readAddr, 0);
                            io.setLastSuccessfulSnapshotId(readAddr, 0);
                            io.setLastSuccessfulSnapshotTag(readAddr, 0);
                            io.setNextSnapshotTag(readAddr, 1);
                            io.setCandidatePageCount(readAddr, 0);

                            changed = true;

                            break;
                    }

                    if (changed) {
                        PageIO.setCrc(readAddr, 0);

                        int crc32 = FastCrc.calcCrc(readBuf, pageSize);

                        PageIO.setCrc(readAddr, crc32);

                        readBuf.rewind();
                    }

                    ch.write(readBuf, hdrSize + ((long)pageIdx) * pageSize);

                    readBuf.rewind();
                }

                ch.force(true);

                return pageCnt;
            }
        }
        finally {
            GridUnsafe.freeBuffer(readBuf);
        }
    }

    /** */
    private static boolean readNextPage(ByteBuffer buf, FileChannel ch, int pageSize) throws IOException {
        assert buf.remaining() == pageSize;

        do {
            if (ch.read(buf) == -1)
                break;
        }
        while (buf.hasRemaining());

        if (!buf.hasRemaining() && PageIO.getPageId(buf) != 0)
            return true; //pageSize bytes read && pageId != 0
        else if (buf.remaining() == pageSize)
            return false; //0 bytes read
        else
            // 1 <= readBytes < pageSize || readBytes == pagesIze && pageId != 0
            throw new IgniteException("Corrupted page in partitionId " +
                ", readByte=" + buf.position() + ", pageSize=" + pageSize + ", content=" + U.toHexString(buf));
    }

    /** */
    private ByteBuffer headerBuffer(byte type, int pageSize) {
        FilePageStore store = storeFactory.createPageStore(type, null, storeFactory.latestVersion(), allocationTracker);

        return store.header(type, pageSize);
    }

    /**
     * Gets CLI option from filled options map.
     * @param options Options and their values.
     * @param name Option name.
     * @param cls Value class,
     * @param dfltVal Default value supplier.
     * @param <T> Value type.
     * @return Value.
     */
    private static <T> T getOptionFromMap(Map<String, String> options, String name, Class<T> cls, Supplier<T> dfltVal) {
        String s = options.get(name);

        if (s == null)
            return dfltVal.get();

        T val = null;

        if (cls.equals(String.class))
            val = (T)s;
        else if (cls.equals(Integer.class))
            val = (T)new Integer(Integer.parseInt(s));
        else if (cls.equals(Boolean.class))
            val = (T)Boolean.valueOf(s);

        return val == null ? dfltVal.get() : val;
    }

    /**
     * Entry point.
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("THIS UTILITY MUST BE LAUNCHED ON PERSISTENT STORE WHICH IS NOT UNDER RUNNING GRID!");

        try {
            AtomicReference<CLIArgumentParser> parserRef = new AtomicReference<>();

            List<CLIArgument> argsConfiguration = asList(
                mandatoryArg(DIR.arg(), "partition directory, where index.bin and (optionally) partition files are located.", String.class),
                optionalArg(PART_CNT.arg(), "full partitions count in cache group.", Integer.class, () -> 0),
                optionalArg(PAGE_SIZE.arg(), "page size.", Integer.class, () -> 4096),
                optionalArg(PAGE_STORE_VER.arg(), "page store version.", Integer.class, () -> 2),
                optionalArg(INDEXES.arg(), "you can specify index tree names that will be processed, separated by comma " +
                    "without spaces, other index trees will be skipped.", String[].class, () -> null),
                optionalArg(DEST_FILE.arg(), "file to print the report to (by default report is printed to console).", String.class, () -> null),
                optionalArg(TRANSFORM.arg(), "if specified, this utility assumes that all *.bin files " +
                    "in --dir directory are snapshot files, and transforms them to normal format and puts to --dest" +
                    " directory.", Boolean.class, () -> false),
                optionalArg(DEST.arg(),
                    "directory where to put files transformed from snapshot (needed if you use --transform).",
                    String.class,
                    () -> {
                        if (parserRef.get().get(TRANSFORM.arg()))
                            throw new IgniteException("Destination path for transformed files is not specified (use --dest)");
                        else
                            return null;
                    }
                ),
                optionalArg(FILE_MASK.arg(), "mask for files to transform (optional if you use --transform).", String.class, () -> ".bin"),
                optionalArg(CHECK_PARTS.arg(), "check cache data tree in partition files and it's consistency with indexes.", Boolean.class, () -> false)
            );

            CLIArgumentParser p = new CLIArgumentParser(argsConfiguration);

            parserRef.set(p);

            if (args.length == 0) {
                System.out.println(p.usage());

                return;
            }

            p.parse(asList(args).iterator());

            String destFile = p.get(DEST_FILE.arg());

            OutputStream destStream = destFile == null ? null : new FileOutputStream(destFile);

            try (IgniteIndexReader reader = new IgniteIndexReader(
                p.get(DIR.arg()),
                p.get(PAGE_SIZE.arg()),
                p.get(PART_CNT.arg()),
                p.get(PAGE_STORE_VER.arg()),
                p.get(INDEXES.arg()),
                p.get(CHECK_PARTS.arg()),
                destStream
            )) {
                if (p.get(TRANSFORM.arg()))
                    reader.transform(p.get(DEST.arg()), p.get(FILE_MASK.arg()));
                else
                    reader.readIdx();
            }
        }
        catch (Exception e) {
            throw e;
        }
    }

    /**
     *
     */
    public enum Args {
        /** */
        DIR("--dir"),
        /** */
        PART_CNT("--partCnt"),
        /** */
        PAGE_SIZE("--pageSize"),
        /** */
        PAGE_STORE_VER("--pageStoreVer"),
        /** */
        INDEXES("--indexes"),
        /** */
        DEST_FILE("--destFile"),
        /** */
        TRANSFORM("--transform"),
        /** */
        DEST("--dest"),
        /** */
        FILE_MASK("--fileMask"),
        /** */
        CHECK_PARTS("--checkParts");

        /** */
        private String arg;

        /** */
        Args(String arg) {
            this.arg = arg;
        }

        /** */
        public String arg() {
            return arg;
        }
    }

    /**
     * Storage for items of index tree.
     */
    private interface ItemStorage<T> extends Iterable<T> {
        /** */
        void add(T item);

        /** */
        boolean contains(T item);

        /** */
        long size();
    }

    /**
     * Imitates item storage, but stores only items count.
     */
    private static class CountOnlyStorage<T> implements ItemStorage<T> {
        /** */
        private long size = 0;

        /** {@inheritDoc} */
        @Override public void add(T item) {
            size++;
        }

        /** {@inheritDoc} */
        @Override public boolean contains(T item) {
            throw new UnsupportedOperationException("'contains' operation is not supported by SizeOnlyStorage.");
        }

        /** {@inheritDoc} */
        @Override public long size() {
            return size;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<T> iterator() {
            throw new UnsupportedOperationException("Iteration is not supported by SizeOnlyStorage.");
        }
    }

    /**
     * Stores link items.
     */
    private static class LinkStorage implements ItemStorage<CacheAwareLink> {
        /** */
        private final Map<Integer, Map<Integer, Map<Byte, IntSet>>> store = new HashMap<>();

        /** */
        private long size = 0;

        /** {@inheritDoc} */
        @Override public void add(CacheAwareLink cacheAwareLink) {
            long link = cacheAwareLink.link;

            long pageId = pageId(link);

            store.computeIfAbsent(cacheAwareLink.cacheId, k -> new HashMap<>())
                .computeIfAbsent(partId(pageId), k -> new HashMap<>())
                .computeIfAbsent((byte)itemId(link), k -> new BitSetIntSet())
                .add(pageIndex(pageId));

            size++;
        }

        /** {@inheritDoc} */
        @Override public boolean contains(CacheAwareLink cacheAwareLink) {
            long link = cacheAwareLink.link;

            long pageId = pageId(link);

            // Cache id from index.bin indexes is always 0.
            Map<Integer, Map<Byte, IntSet>> map = store.get(0);

            if (map != null) {
                Map<Byte, IntSet> innerMap = map.get(partId(pageId));

                if (innerMap != null) {
                    IntSet set = innerMap.get((byte)itemId(link));

                    if (set != null)
                        return set.contains(pageIndex(pageId));
                }
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public long size() {
            return size;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<CacheAwareLink> iterator() {
            throw new UnsupportedOperationException("Item iteration is not supported by link storage.");
        }
    }

    /**
     * Stores index items (items of meta tree).
     */
    private static class ItemsListStorage<T> implements ItemStorage<T> {
        private final List<T> store = new LinkedList<>();

        /** {@inheritDoc} */
        @Override public void add(T item) {
            store.add(item);
        }

        /** {@inheritDoc} */
        @Override public boolean contains(T item) {
            throw new UnsupportedOperationException("'contains' operation is not supported by ItemsListStorage.");
        }

        /** {@inheritDoc} */
        @Override public long size() {
            return store.size();
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<T> iterator() {
            return store.iterator();
        }
    }

    /**
     *
     */
    private static class CacheAwareLink {
        /** */
        public final int cacheId;

        /** */
        public final long link;

        /** */
        public CacheAwareLink(int cacheId, long link) {
            this.cacheId = cacheId;
            this.link = link;
        }
    }

    /**
     * Tree node info.
     */
    private static class TreeNode {
        /** */
        final long pageId;

        /** */
        final PageIO io;

        /** */
        final String additionalInfo;

        /** */
        final List<TreeNode> children;

        /** */
        public TreeNode(long pageId, PageIO io, String additionalInfo, List<TreeNode> children) {
            this.pageId = pageId;
            this.io = io;
            this.additionalInfo = additionalInfo;
            this.children = children;
        }
    }

    /**
     * Traverse context, which is unique for traversal of one single tree.
     */
    private static class TreeTraverseContext {
        /** Page store. */
        final FilePageStore store;

        /** Page type statistics. */
        final Map<Class, AtomicLong> ioStat;

        /** Map of errors, pageId -> set of exceptions. */
        final Map<Long, Set<Throwable>> errors;

        /** Callback that is called for each inner node page. */
        final PageCallback innerCb;

        /** Callback that is called for each leaf node page.*/
        final PageCallback leafCb;

        /** Callback that is called for each leaf item. */
        final ItemCallback itemCb;

        /** */
        private TreeTraverseContext(
            FilePageStore store,
            Map<Class, AtomicLong> ioStat,
            Map<Long, Set<Throwable>> errors,
            PageCallback innerCb,
            PageCallback leafCb,
            ItemCallback itemCb
        ) {
            this.store = store;
            this.ioStat = ioStat;
            this.errors = errors;
            this.innerCb = innerCb;
            this.leafCb = leafCb;
            this.itemCb = itemCb;
        }
    }

    /**
     * Content of the deserialized page. When content is gained, we can free the page buffer.
     */
    private static class PageContent {
        /** */
        final PageIO io;

        /** List of children page ids, or links to root pages (for meta leaf). */
        final List<Long> linkedPageIds;

        /** List of items (for leaf pages). */
        final List<Object> items;

        /** Some info. */
        final String info;

        /** */
        public PageContent(PageIO io, List<Long> linkedPageIds, List<Object> items, String info) {
            this.io = io;
            this.linkedPageIds = linkedPageIds;
            this.items = items;
            this.info = info;
        }
    }

    /**
     *
     */
    private interface PageCallback {
        /** */
        void cb(PageContent pageContent, long pageId);
    }

    /**
     *
     */
    private interface ItemCallback {
        /** */
        void cb(long currPageId, Object item, long link);
    }

    /**
     *
     */
    private interface BufferClosure<T> {
        /** */
        T apply(ByteBuffer buf, Long addr) throws IgniteCheckedException;
    }

    /**
     * Processor for page IOs.
     */
    private interface PageIOProcessor {
        /**
         * Gets deserialized content.
         * @param io Page IO.
         * @param addr Page address.
         * @param pageId Page id.
         * @param nodeCtx Tree traversal context.
         * @return Page content.
         */
        PageContent getContent(PageIO io, long addr, long pageId, TreeTraverseContext nodeCtx);

        /**
         * Gets node info from page contents.
         * @param content Page content.
         * @param pageId Page id.
         * @param nodeCtx Tree traversal context.
         * @return Tree node info.
         */
        TreeNode getNode(PageContent content, long pageId, TreeTraverseContext nodeCtx);
    }

    /**
     *
     */
    private class MetaPageIOProcessor implements PageIOProcessor {
        /** {@inheritDoc} */
        @Override public PageContent getContent(PageIO io, long addr, long pageId, TreeTraverseContext nodeCtx) {
            BPlusMetaIO bPlusMetaIO = (BPlusMetaIO)io;

            int rootLvl = bPlusMetaIO.getRootLevel(addr);
            long rootId = bPlusMetaIO.getFirstPageId(addr, rootLvl);

            return new PageContent(io, singletonList(rootId), null, null);
        }

        /** {@inheritDoc} */
        @Override public TreeNode getNode(PageContent content, long pageId, TreeTraverseContext nodeCtx) {
            return new TreeNode(pageId, content.io, null, singletonList(getTreeNode(content.linkedPageIds.get(0), nodeCtx)));
        }
    }

    /**
     *
     */
    private class InnerPageIOProcessor implements PageIOProcessor {
        /** {@inheritDoc} */
        @Override public PageContent getContent(PageIO io, long addr, long pageId, TreeTraverseContext nodeCtx) {
            BPlusInnerIO innerIo = (BPlusInnerIO)io;

            int cnt = innerIo.getCount(addr);

            List<Long> childrenIds;

            if (cnt > 0) {
                childrenIds = new ArrayList<>(cnt + 1);

                for (int i = 0; i < cnt; i++)
                    childrenIds.add(innerIo.getLeft(addr, i));

                childrenIds.add(innerIo.getRight(addr, cnt - 1));
            }
            else {
                long left = innerIo.getLeft(addr, 0);

                childrenIds = left == 0 ? Collections.<Long>emptyList() : singletonList(left);
            }

            return new PageContent(io, childrenIds, null, null);
        }

        /** {@inheritDoc} */
        @Override public TreeNode getNode(PageContent content, long pageId, TreeTraverseContext nodeCtx) {
            List<TreeNode> children = new ArrayList<>(content.linkedPageIds.size());

            for (Long id : content.linkedPageIds)
                children.add(getTreeNode(id, nodeCtx));

            if (nodeCtx.innerCb != null)
                nodeCtx.innerCb.cb(content, pageId);

            return new TreeNode(pageId, content.io, null, children);
        }
    }

    /**
     *
     */
    private class LeafPageIOProcessor implements PageIOProcessor {
        /** {@inheritDoc} */
        @Override public PageContent getContent(PageIO io, long addr, long pageId, TreeTraverseContext nodeCtx) {
            GridStringBuilder sb = new GridStringBuilder();

            List<Object> items = new LinkedList<>();

            BPlusLeafIO leafIO = (BPlusLeafIO)io;

            for (int j = 0; j < leafIO.getCount(addr); j++) {
                Object idxItem = null;

                try {
                    if (io.getClass().equals(IndexStorageImpl.MetaStoreLeafIO.class)) {
                        idxItem = ((IndexStorageImpl.MetaStoreLeafIO)io).getLookupRow(null, addr, j);

                        if (idxItem != null)
                            sb.a(idxItem.toString() + " ");
                    }
                    else
                        idxItem = getLeafItem(leafIO, addr, j);
                }
                catch (Exception e) {
                    nodeCtx.errors.computeIfAbsent(pageId, k -> new HashSet<>()).add(e);
                }

                if (idxItem != null)
                    items.add(idxItem);
            }

            return new PageContent(io, null, items, sb.toString());
        }

        /** */
        private Object getLeafItem(BPlusLeafIO io, long addr, int idx) throws IgniteCheckedException {
            if (isLinkIo(io)) {
                final long link = getLink(io, addr, idx);

                int cacheId = 0;

                if (io instanceof RowLinkIO)
                    cacheId = ((RowLinkIO)io).getCacheId(addr, idx);

                final CacheAwareLink res = new CacheAwareLink(cacheId, link);

                if (partCnt > 0) {
                    long linkedPageId = pageId(link);

                    int linkedPagePartId = partId(linkedPageId);

                    if (missingPartitions.contains(linkedPagePartId))
                        return res; // just skip

                    int linkedItemId = itemId(link);

                    if (linkedPagePartId > partStores.length - 1) {
                        missingPartitions.add(linkedPagePartId);

                        throw new IgniteException("Calculated data page partition id exceeds given partitions count: " +
                            linkedPagePartId + ", partCnt=" + partCnt);
                    }

                    final FilePageStore store = partStores[linkedPagePartId];

                    if (store == null) {
                        missingPartitions.add(linkedPagePartId);

                        throw new IgniteException("Corresponding store wasn't found for partId=" + linkedPagePartId + ". Does partition file exist?");
                    }

                    doWithBuffer((dataBuf, dataBufAddr) -> {
                        store.read(linkedPageId, dataBuf, false);

                        PageIO dataIo = PageIO.getPageIO(getType(dataBuf), getVersion(dataBuf));

                        if (dataIo instanceof AbstractDataPageIO) {
                            AbstractDataPageIO dataPageIO = (AbstractDataPageIO)dataIo;

                            DataPagePayload payload = dataPageIO.readPayload(dataBufAddr, linkedItemId, pageSize);

                            if (payload.offset() <= 0 || payload.payloadSize() <= 0) {
                                GridStringBuilder payloadInfo = new GridStringBuilder("Invalid data page payload: ")
                                    .a("off=").a(payload.offset())
                                    .a(", size=").a(payload.payloadSize())
                                    .a(", nextLink=").a(payload.nextLink());

                                throw new IgniteException(payloadInfo.toString());
                            }
                        }

                        return null;
                    });
                }

                return res;
            }
            else
                throw new IgniteException("Unexpected page io: " + io.getClass().getSimpleName());
        }

        /** */
        private boolean isLinkIo(PageIO io) {
            return io instanceof H2RowLinkIO || io instanceof PendingRowIO || io instanceof RowLinkIO;
        }

        /** */
        private long getLink(BPlusLeafIO io, long addr, int idx) {
            if (io instanceof RowLinkIO)
                return ((RowLinkIO)io).getLink(addr, idx);
            if (io instanceof H2RowLinkIO)
                return ((H2RowLinkIO)io).getLink(addr, idx);
            else if (io instanceof PendingRowIO)
                return ((PendingRowIO)io).getLink(addr, idx);
            else
                throw new IgniteException("No link to data page on idx=" + idx);
        }

        /** {@inheritDoc} */
        @Override public TreeNode getNode(PageContent content, long pageId, TreeTraverseContext nodeCtx) {
            if (nodeCtx.leafCb != null)
                nodeCtx.leafCb.cb(content, pageId);

            if (nodeCtx.itemCb != null) {
                for (Object item : content.items)
                    nodeCtx.itemCb.cb(pageId, item, 0);
            }

            return new TreeNode(pageId, content.io, content.info, Collections.emptyList());
        }
    }

    /**
     *
     */
    private static class TreeTraversalInfo {
        /** Page type statistics. */
        final Map<Class, AtomicLong> ioStat;

        /** Map of errors, pageId -> set of exceptions. */
        final Map<Long, Set<Throwable>> errors;

        /** Set of all inner page ids. */
        final Set<Long> innerPageIds;

        /** Root page id. */
        final long rootPageId;

        /**
         * List of items storage.
         */
        final ItemStorage itemStorage;

        /** */
        public TreeTraversalInfo(
            Map<Class, AtomicLong> ioStat,
            Map<Long, Set<Throwable>> errors,
            Set<Long> innerPageIds,
            long rootPageId,
            ItemStorage itemStorage
        ) {
            this.ioStat = ioStat;
            this.errors = errors;
            this.innerPageIds = innerPageIds;
            this.rootPageId = rootPageId;
            this.itemStorage = itemStorage;
        }
    }

    /**
     *
     */
    private static class PageListsInfo {
        /**
         * Page list bucket data (next meta id, bucket index) -> list of page ids.
         * See {@link PagesListMetaIO#getBucketsData }.
         */
        final Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData;

        /** All page ids from page lists. */
        final Set<Long> allPages;

        /** Page type statistics. */
        final Map<Class, AtomicLong> pageListStat;

        /** Map of errors, pageId -> exception. */
        final Map<Long, Throwable> errors;

        /** */
        public PageListsInfo(
            Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData,
            Set<Long> allPages,
            Map<Class, AtomicLong> pageListStat,
            Map<Long, Throwable> errors
        ) {
            this.bucketsData = bucketsData;
            this.allPages = allPages;
            this.pageListStat = pageListStat;
            this.errors = errors;
        }
    }
}
