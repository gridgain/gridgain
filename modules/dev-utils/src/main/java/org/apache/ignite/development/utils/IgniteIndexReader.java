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
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataStorageConfiguration;
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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.tree.PendingRowIO;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
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
    private final long pagesNum;

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
    public IgniteIndexReader(String cacheWorkDirPath, int pageSize, int partCnt, int filePageStoreVer, OutputStream outputStream) throws IgniteCheckedException {
        this.pageSize = pageSize;
        this.partCnt = partCnt;
        this.dsCfg = new DataStorageConfiguration().setPageSize(pageSize);
        this.cacheWorkDir = new File(cacheWorkDirPath);
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

        pagesNum = idxStore == null ? 0 : (idxFile.length() - idxStore.headerSize()) / pageSize;

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

        print("Going to check " + pagesNum + " pages.");

        Map<Class, Set<Long>> pageIoIds = new HashMap<>();

        Map<String, TreeTraversalInfo> treeInfo = null;

        Map<String, TreeTraversalInfo> horizontalScans = null;

        AtomicReference<PageListsInfo> pageListsInfo = new AtomicReference<>();

        List<Throwable> errors = new LinkedList<>();

        ProgressPrinter progressPrinter = new ProgressPrinter(System.out, "Reading pages sequentially", pagesNum);

        ByteBuffer buf = allocateBuffer(pageSize);

        long addr = bufferAddress(buf);

        try {
            for (int i = 0; i < pagesNum; i++) {
                try {
                    buf.rewind();

                    progressPrinter.printProgress();

                    long pageId = PageIdUtils.pageId(INDEX_PARTITION, FLAG_IDX, i);

                    final long off = (long)i * pageSize + idxStore.headerSize();

                    idxStore.readByOffset(off, buf, false);

                    PageIO io = PageIO.getPageIO(addr);

                    pageClasses.merge(io.getClass(), 1L, (oldVal, newVal) -> ++oldVal);

                    if (io instanceof PageMetaIO) {
                        PageMetaIO pageMetaIO = (PageMetaIO)io;

                        long metaTreeRootId = normalizePageId(pageMetaIO.getTreeRoot(addr));

                        treeInfo = traverseAllTrees("Index trees traversal", metaTreeRootId, this::traverseTree);

                        treeInfo.forEach((name, info) -> {
                            info.innerPageIds.forEach(id -> {
                                Class cls = name.equals(META_TREE_NAME)
                                    ? IndexStorageImpl.MetaStoreInnerIO.class
                                    : H2ExtrasInnerIO.class;

                                pageIoIds.computeIfAbsent(cls, k -> new HashSet<>()).add(id);
                            });

                            pageIoIds.computeIfAbsent(BPlusMetaIO.class, k -> new HashSet<>()).add(info.rootPageId);
                        });

                        horizontalScans = traverseAllTrees("Scan index trees horizontally", metaTreeRootId, this::horizontalTreeScan);
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
                } catch (Throwable e) {
                    String err = "Exception occurred on step " + i + ": " + e.getMessage();

                    errors.add(new IgniteException(err, e));
                }
            }
        }
        finally {
            freeBuffer(buf);
        }

        if (treeInfo == null)
            printErr("No tree meta info found.");
        else {
            printTraversalResults(FROM_ROOT_TO_LEAFS_TRAVERSE_NAME, treeInfo);

            printTraversalResults(HORIZONTAL_SCAN_NAME, horizontalScans);
        }

        compareTraversals(treeInfo, horizontalScans);

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

            if (tree.itemsCnt != scan.itemsCnt)
                errors.add(compareError("items", name, tree.itemsCnt, scan.itemsCnt, null));

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

        long nextMetaId = metaPageListId;

        ByteBuffer buf = allocateBuffer(pageSize);

        long addr = bufferAddress(buf);

        try {
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
        }
        finally {
            freeBuffer(buf);
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
        BiFunction<Long, Boolean, TreeTraversalInfo> traverseProc
    ) {
        Map<String, TreeTraversalInfo> treeInfos = new HashMap<>();

        TreeTraversalInfo metaTreeTraversalInfo = traverseProc.apply(metaTreeRoot, true);

        treeInfos.put(META_TREE_NAME, metaTreeTraversalInfo);

        ProgressPrinter progressPrinter = new ProgressPrinter(System.out, traverseProcCaption, metaTreeTraversalInfo.idxItems.size());

        metaTreeTraversalInfo.idxItems.forEach(item -> {
            progressPrinter.printProgress();

            IndexStorageImpl.IndexItem idxItem = (IndexStorageImpl.IndexItem)item;

            TreeTraversalInfo treeTraversalInfo = traverseProc.apply(normalizePageId(idxItem.pageId()), false);

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

        List<String> idxInconsistencyErrors = new LinkedList<>();

        treeInfos.forEach((idxName, validationInfo) -> {
            print(prefix + "-----");
            print(prefix + "Index tree: " + idxName);
            print(prefix + "-- Page stat:");

            validationInfo.ioStat.forEach((cls, cnt) -> {
                print(prefix + cls.getSimpleName() + ": " + cnt.get());

                totalStat.computeIfAbsent(cls, k -> new AtomicLong(0)).addAndGet(cnt.get());
            });

            print(prefix + "-- Count of items found in leaf pages: " + validationInfo.itemsCnt);

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
                .put(idxName, validationInfo.itemsCnt);
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
     * @param isMetaTree Whether it is meta tree.
     * @return Tree traversal info.
     */
    private TreeTraversalInfo traverseTree(long rootPageId, boolean isMetaTree) {
        Map<Class, AtomicLong> ioStat = new HashMap<>();

        Map<Long, Set<Throwable>> errors = new HashMap<>();

        Set<Long> innerPageIds = new HashSet<>();

        PageCallback innerCb = (content, pageId) -> innerPageIds.add(normalizePageId(pageId));

        List<Object> idxItems = new LinkedList<>();

        AtomicLong idxItemsCnt = new AtomicLong(0);

        ItemCallback itemCb = isMetaTree
            ? (currPageId, item, link) -> idxItems.add(item)
            : (currPageId, item, link) -> idxItemsCnt.incrementAndGet();

        getTreeNode(rootPageId, new TreeTraverseContext(idxStore, ioStat, errors, innerCb, null, itemCb));

        return isMetaTree
            ? new TreeTraversalInfo(ioStat, errors, innerPageIds, rootPageId, idxItems)
            : new TreeTraversalInfo(ioStat, errors, innerPageIds, rootPageId, idxItemsCnt.get());
    }

    /**
     * Traverse single index tree by each level horizontally.
     *
     * @param rootPageId Root page id.
     * @param isMetaTree Whether it is meta tree.
     * @return Tree traversal info.
     */
    private TreeTraversalInfo horizontalTreeScan(long rootPageId, boolean isMetaTree) {
        Map<Long, Set<Throwable>> errors = new HashMap<>();

        Map<Class, AtomicLong> ioStat = new HashMap<>();

        AtomicLong itemsCnt = new AtomicLong(0);

        List<Object> items = new LinkedList<>();

        TreeTraverseContext treeCtx = new TreeTraverseContext(idxStore, ioStat, errors, null, null, null);

        ByteBuffer buf = allocateBuffer(pageSize);

        try {
            long addr = bufferAddress(buf);

            buf.rewind();

            idxStore.read(rootPageId, buf, false);

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

                        idxStore.read(pageId, buf, false);

                        pageIO = PageIO.getPageIO(addr);

                        if (i == 0 && !(pageIO instanceof BPlusLeafIO))
                            throw new IgniteException("Not-leaf page found on leaf level, pageId=" + pageId + ", level=" + i);

                        if (!(pageIO instanceof BPlusIO))
                            throw new IgniteException("Not-BPlus page found, pageId=" + pageId + ", level=" + i);

                        ioStat.computeIfAbsent(pageIO.getClass(), k -> new AtomicLong(0)).incrementAndGet();

                        if (pageIO instanceof BPlusLeafIO) {
                            PageIOProcessor ioProcessor = getIOProcessor(pageIO);

                            PageContent pageContent = ioProcessor.getContent(pageIO, addr, pageId, treeCtx);

                            itemsCnt.addAndGet(pageContent.items.size());

                            if (isMetaTree)
                                items.addAll(pageContent.items);
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

        return isMetaTree
            ? new TreeTraversalInfo(ioStat, errors, null, rootPageId, items)
            : new TreeTraversalInfo(ioStat, errors, null, rootPageId, itemsCnt.get());
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
        try {
            System.out.println("THIS UTILITY MUST BE LAUNCHED ON PERSISTENT STORE WHICH IS NOT UNDER RUNNING GRID!");

            Map<String, String> options = new HashMap<String, String>() {{
                put("--dir", null);
                put("--partCnt", null);
                put("--pageSize", null);
                put("--pageStoreVer", null);
                put("--destFile", null);
                put("--dest", null);
                put("--transform", null);
                put("--fileMask", null);
            }};

            for (Iterator<String> iterator = Arrays.asList(args).iterator(); iterator.hasNext();) {
                String option = iterator.next();

                if (!options.containsKey(option))
                    throw new Exception("Unexpected option: " + option);

                if (!iterator.hasNext())
                    throw new Exception("Please specify a value for option: " + option);

                String val = iterator.next();

                options.put(option, val);
            }

            String dir = getOptionFromMap(options, "--dir", String.class, () -> { throw new IgniteException("File path was not specified."); } );

            int partCnt = getOptionFromMap(options, "--partCnt", Integer.class, () -> 0);

            int pageSize = getOptionFromMap(options, "--pageSize", Integer.class, () -> 4096);

            int pageStoreVer = getOptionFromMap(options, "--pageStoreVer", Integer.class, () -> 2);

            String destFile = getOptionFromMap(options, "--destFile", String.class, () -> null);

            boolean transform = getOptionFromMap(options, "--transform", Boolean.class, () -> false);

            String dest = getOptionFromMap(options, "--dest", String.class, () -> null);

            String fileMask = getOptionFromMap(options, "--fileMask", String.class, () -> ".bin");

            OutputStream destStream = destFile == null ? null : new FileOutputStream(destFile);

            try (IgniteIndexReader reader = new IgniteIndexReader(dir, pageSize, partCnt, pageStoreVer, destStream)) {
                if (transform)
                    reader.transform(dest, fileMask);
                else
                    reader.readIdx();
            }
        }
        catch (Exception e) {
            System.err.println("How to use: please pass option names, followed by space and option values. Options list:");
            System.err.println("--dir: partition directory, where index.bin and (optionally) partition files are located (obligatory)");
            System.err.println("--partCnt: full partitions count in cache group (optional)");
            System.err.println("--pageSize: page size (optional, default value is 4096)");
            System.err.println("--pageStoreVer: page store version (optional, default value is 2)");
            System.err.println("--destFile: file to print the report to (optional, by default report is printed to console)");
            System.err.println("--transform: if 'true' then this utility assumes that all *.bin files "
                + "in --dir directory are snapshot files, transforms them to normal format and puts to --dest directory."
                + " (optional)"
            );
            System.err.println("--dest: directory where to put files transformed from snapshot (needed if you use "
                + "--transform true)");
            System.err.println("--fileMask: mask for files to transform.");
            System.err.println();

            throw e;
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

            if (io instanceof IndexStorageImpl.MetaStoreLeafIO) {
                IndexStorageImpl.MetaStoreLeafIO metaLeafIO = (IndexStorageImpl.MetaStoreLeafIO)io;

                for (int j = 0; j < metaLeafIO.getCount(addr); j++) {
                    IndexStorageImpl.IndexItem indexItem = null;

                    try {
                        indexItem = metaLeafIO.getLookupRow(null, addr, j);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }

                    if (indexItem.pageId() != 0) {
                        sb.a(indexItem.toString() + " ");

                        items.add(indexItem);
                    }
                }
            }
            else {
                boolean processed = processIndexLeaf(io, addr, pageId, items, nodeCtx);

                if (!processed)
                    throw new IgniteException("Unexpected page io: " + io.getClass().getSimpleName());
            }

            return new PageContent(io, null, items, sb.toString());
        }

        /** */
        private boolean processIndexLeaf(PageIO io, long addr, long pageId, List<Object> items, TreeTraverseContext nodeCtx) {
            if (io instanceof BPlusIO && (io instanceof H2RowLinkIO || io instanceof PendingRowIO)) {
                int itemsCnt = ((BPlusIO)io).getCount(addr);

                for (int j = 0; j < itemsCnt; j++) {
                    long link = 0;

                    if (io instanceof H2RowLinkIO)
                        link = ((H2RowLinkIO)io).getLink(addr, j);
                    else if (io instanceof PendingRowIO)
                        link = ((PendingRowIO)io).getLink(addr, j);

                    if (link == 0)
                        throw new IgniteException("No link to data page on idx=" + j);

                    items.add(link);

                    if (partCnt > 0) {
                        long linkedPageId = pageId(link);

                        int linkedPagePartId = partId(linkedPageId);

                        if (missingPartitions.contains(linkedPagePartId))
                            continue;

                        int linkedItemId = itemId(link);

                        ByteBuffer dataBuf = GridUnsafe.allocateBuffer(pageSize);

                        try {
                            long dataBufAddr = GridUnsafe.bufferAddress(dataBuf);

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
                        }
                        catch (Exception e) {
                            String err = "Failed to read data payload from partition, partId=" + linkedPagePartId +
                                ", pageIndex=" + pageIndex(linkedPageId) + ", linkedPageId=" + linkedPageId;

                            nodeCtx.errors.computeIfAbsent(pageId, k -> new HashSet<>())
                                .add(new IgniteException(err, e));
                        }
                        finally {
                            GridUnsafe.freeBuffer(dataBuf);
                        }
                    }
                }

                return true;
            }
            else
                return false;
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
         * List of index items. This list is filled only for meta tree. For other index trees only {@link #itemsCnt}
         * is set.
         */
        final List<Object> idxItems;

        /** Items count. */
        final long itemsCnt;

        /** */
        public TreeTraversalInfo(
            Map<Class, AtomicLong> ioStat,
            Map<Long, Set<Throwable>> errors,
            Set<Long> innerPageIds,
            long rootPageId,
            List<Object> idxItems
        ) {
            this.ioStat = ioStat;
            this.errors = errors;
            this.innerPageIds = innerPageIds;
            this.rootPageId = rootPageId;
            this.idxItems = idxItems;
            this.itemsCnt = idxItems.size();
        }


        /** */
        public TreeTraversalInfo(
            Map<Class, AtomicLong> ioStat,
            Map<Long, Set<Throwable>> errors,
            Set<Long> innerPageIds,
            long rootPageId,
            long itemsCnt
        ) {
            this.ioStat = ioStat;
            this.errors = errors;
            this.innerPageIds = innerPageIds;
            this.rootPageId = rootPageId;
            this.idxItems = null;
            this.itemsCnt = itemsCnt;
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
