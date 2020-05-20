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
package org.apache.ignite.development.utils.indexreader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2MvccLeafIO;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.configuration.GridGainConfiguration;
import org.gridgain.grid.configuration.SnapshotConfiguration;
import org.gridgain.grid.internal.processors.cache.database.snapshot.GridCacheSnapshotManager;
import org.gridgain.grid.persistentstore.GridSnapshot;
import org.gridgain.grid.persistentstore.SnapshotFuture;
import org.gridgain.grid.persistentstore.snapshot.file.FileDatabaseSnapshotSpi;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.ERROR_PREFIX;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.RECURSIVE_TRAVERSE_NAME;
import static org.apache.ignite.development.utils.indexreader.IgniteIndexReader.HORIZONTAL_SCAN_NAME;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Class for testing {@link IgniteIndexReader}.
 */
public class IgniteIndexReaderTest extends GridCommonAbstractTest {
    /** Page size. */
    private static final int PAGE_SIZE = 4096;

    /** Partitions count. */
    private static final int PART_CNT = RendezvousAffinityFunction.DFLT_PARTITION_COUNT;

    /** Version of file page stores. */
    private static final int PAGE_STORE_VER = 2;

    /** Cache group name. */
    private static final String CACHE_GROUP_NAME = "defaultGroup";

    /** Cache group without indexes. */
    private static final String EMPTY_CACHE_NAME = "empty";

    /** Cache group without indexes. */
    private static final String EMPTY_CACHE_GROUP_NAME = "emptyGroup";

    /** Cache with static query configuration. */
    private static final String QUERY_CACHE_NAME = "query";

    /** Cache group with static query configuration. */
    private static final String QUERY_CACHE_GROUP_NAME = "queryGroup";

    /** Count of tables that will be created for test. */
    private static final int CREATED_TABLES_CNT = 3;

    /** Line delimiter. */
    private static final String LINE_DELIM = System.lineSeparator();

    /** Common part of regexp for single index output validation. */
    private static final String CHECK_IDX_PTRN_COMMON =
        "<PREFIX>Index tree: I \\[idxName=[\\-_0-9]{1,20}_%s##H2Tree.0, pageId=[0-9a-f]{16}\\]" +
            LINE_DELIM + "<PREFIX>-- Page stat:" +
            LINE_DELIM + "<PREFIX>([0-9a-zA-Z]{1,50}: [0-9]{1,5}" +
            LINE_DELIM + "<PREFIX>){%s,1000}-- Count of items found in leaf pages: %s" +
            LINE_DELIM;

    /** Regexp to validate output of correct index. */
    private static final String CHECK_IDX_PTRN_CORRECT =
        CHECK_IDX_PTRN_COMMON + "<PREFIX>No errors occurred while traversing.";

    /** Regexp to validate output of corrupted index. */
    private static final String CHECK_IDX_PTRN_WITH_ERRORS =
        CHECK_IDX_PTRN_COMMON + "<PREFIX>" + ERROR_PREFIX + "Errors:" +
            LINE_DELIM + "<PREFIX>" + ERROR_PREFIX + "Page id=[0-9]{1,30}, exceptions:" +
            LINE_DELIM + "class.*?Exception.*";

    /** Work directory, containing cache group directories. */
    private static File workDir;

    /** Directory containing full snapshot. */
    private static File fullSnapshotDir;

    /** Directory containing incremental snapshot. */
    private static File incSnapshotDir;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        incSnapshotDir = new File("C:\\Users\\tkalk\\IdeaProjects\\apache-ignite\\work\\snapshot\\ts_20200519184344_1589903024520.snapshot\\c2fb2a59_0c8a_4359_80eb_7d1e71a25b24");
        fullSnapshotDir = new File("C:\\Users\\tkalk\\IdeaProjects\\apache-ignite\\work\\snapshot\\ts_20200519184338_1589903018264.snapshot\\c2fb2a59_0c8a_4359_80eb_7d1e71a25b24");
        workDir = new File("C:\\Users\\tkalk\\IdeaProjects\\apache-ignite\\work\\db\\copy");

        if (1 == 1)
            return;

        cleanPersistenceDir();

        try (IgniteEx node = startGrid(0)) {
            populateData(node, null);

            workDir = ((FilePageStoreManager)node.context().cache().context().pageStore()).workDir();
            fullSnapshotDir = createSnapshot(node, true);
        }

        File workDirCp = new File(workDir.getParent(), "copy");
        U.copy(workDir, workDirCp, true);

        U.delete(workDir);
        workDir = workDirCp;

        try (IgniteEx node = startGrid(0)) {
            populateData(node, true);
            createSnapshot(node, true);

            populateData(node, false);
            incSnapshotDir = createSnapshot(node, false);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        if (1 == 1)
            return;

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setPageSize(PAGE_SIZE)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(10 * 1024L * 1024L)
                        .setMaxSize(50 * 1024L * 1024L)
                )
        ).setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setGroupName(CACHE_GROUP_NAME)
                .setAffinity(new RendezvousAffinityFunction(false))
                .setSqlSchema(QueryUtils.DFLT_SCHEMA),
            new CacheConfiguration(EMPTY_CACHE_NAME)
                .setGroupName(EMPTY_CACHE_GROUP_NAME),
            new CacheConfiguration(QUERY_CACHE_NAME)
                .setGroupName(QUERY_CACHE_GROUP_NAME)
                .setQueryEntities(asList(
                    new QueryEntity(Integer.class, TestClass1.class)
                        .addQueryField("id", Integer.class.getName(), null)
                        .addQueryField("f", Integer.class.getName(), null)
                        .addQueryField("s", String.class.getName(), null)
                        .setIndexes(singleton(new QueryIndex("f")))
                        .setTableName("QT1"),
                    new QueryEntity(Integer.class, TestClass2.class)
                        .addQueryField("id", Integer.class.getName(), null)
                        .addQueryField("f", Integer.class.getName(), null)
                        .addQueryField("s", String.class.getName(), null)
                        .setIndexes(singleton(new QueryIndex("s")))
                        .setTableName("QT2")
                ))
        ).setPluginConfigurations(new GridGainConfiguration().setSnapshotConfiguration(new SnapshotConfiguration()));
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        U.delete(resolveSnapshotDirectory());
    }

    /**
     * Resolve snapshot directory.
     *
     * @return Snapshot directory.
     * @throws Exception If fails.
     */
    private File resolveSnapshotDirectory() throws Exception {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), SnapshotConfiguration.DFLT_SNAPSHOTS_PATH, false);
    }

    /**
     * Filling node with data.
     *
     * @param node Node.
     * @param insert {@code True} if only insert data, {@code false} if delete from {@link #DEFAULT_CACHE_NAME} and {@code null} all at once.
     * @throws Exception If fails.
     */
    private void populateData(IgniteEx node, @Nullable Boolean insert) throws Exception {
        requireNonNull(node);

        IgniteClusterEx cluster = node.cluster();

        if (!cluster.active())
            cluster.active(true);

        if (isNull(insert) || insert) {
            IgniteCache<Integer, Object> qryCache = node.cache(QUERY_CACHE_NAME);

            for (int i = 0; i < 100; i++)
                qryCache.put(i, new TestClass1(i, valueOf(i)));

            for (int i = 0; i < 70; i++)
                qryCache.put(i, new TestClass2(i, valueOf(i)));
        }

        IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < CREATED_TABLES_CNT; i++)
            createAndFillTable(cache, TableInfo.generate(i), insert);

        forceCheckpoint(node);
    }

    /**
     * Creating a snapshot and returning path to it on node.
     *
     * @param node Node.
     * @param full Full or incremental snapshot creation.
     * @return Path to snapshot.
     */
    private File createSnapshot(IgniteEx node, boolean full) {
        requireNonNull(node);

        IgniteClusterEx cluster = node.cluster();

        if (!cluster.active())
            cluster.active(true);

        GridSnapshot gridSnapshot = ((GridGain)node.plugin(GridGain.PLUGIN_NAME)).snapshot();

        SnapshotFuture<Void> snapshotFut = full ? gridSnapshot.createFullSnapshot(null, "full") :
            gridSnapshot.createSnapshot(null, "inc");

        snapshotFut.get();

        GridCacheSnapshotManager snapshotMgr = (GridCacheSnapshotManager)node.context().cache().context().snapshot();
        FileDatabaseSnapshotSpi snapshotSpi = (FileDatabaseSnapshotSpi)snapshotMgr.snapshotSpi();

        return snapshotSpi.findCurNodeSnapshotDir(
            snapshotSpi.snapshotWorkingDirectory().toPath(),
            snapshotFut.snapshotOperation().snapshotId()
        ).toFile();
    }

    /**
     * Get data directory for cache group.
     *
     * @param cacheGrpName Cache group name.
     * @param snapshot Snapshot directory or not.
     * @return Directory name.
     */
    private String dataDir(String cacheGrpName, boolean snapshot) {
        requireNonNull(cacheGrpName);

        return snapshot ? valueOf(CU.cacheId(cacheGrpName)) : CACHE_GRP_DIR_PREFIX + cacheGrpName;
    }

    /**
     * Corrupts partition file.
     *
     * @param workDir Work directory.
     * @param partId Partition id.
     * @param pageNum Page to corrupt.
     * @param snapshot Snapshot directory or not.
     * @throws IOException If failed.
     */
    private void corruptFile(File workDir, int partId, int pageNum, boolean snapshot) throws IOException {
        String fileName = partId == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, partId);

        File cacheWorkDir = new File(workDir, dataDir(CACHE_GROUP_NAME, snapshot));

        File file = new File(cacheWorkDir, fileName);

        File backup = new File(cacheWorkDir, fileName + ".backup");

        if (!backup.exists())
            Files.copy(file.toPath(), backup.toPath());

        try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
            byte[] trash = new byte[PAGE_SIZE];

            ThreadLocalRandom.current().nextBytes(trash);

            int hdrSize = snapshot ? 0 : new FileVersionCheckingFactory(
                new AsyncFileIOFactory(),
                new AsyncFileIOFactory(),
                new DataStorageConfiguration().setPageSize(PAGE_SIZE)
            ).headerSize(PAGE_STORE_VER);

            f.seek(pageNum * PAGE_SIZE + hdrSize);

            f.write(trash);
        }
    }

    /**
     * Restores corrupted file from backup after corruption.
     *
     * @param workDir Work directory.
     * @param partId Partition id.
     * @param snapshot Snapshot directory or not.
     * @throws IOException If failed.
     */
    private void restoreFile(File workDir, int partId, boolean snapshot) throws IOException {
        String fileName = partId == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, partId);

        File cacheWorkDir = new File(workDir, dataDir(CACHE_GROUP_NAME, snapshot));

        Path backupFilesPath = new File(cacheWorkDir, fileName + ".backup").toPath();

        Files.copy(backupFilesPath, new File(cacheWorkDir, fileName).toPath(), REPLACE_EXISTING);

        Files.delete(backupFilesPath);
    }

    /**
     * Generates fields for sql table.
     *
     * @param cnt Count of fields.
     * @return List of pairs, first is field name, second is field type.
     */
    private static List<IgnitePair<String>> fields(int cnt) {
        List<IgnitePair<String>> res = new LinkedList<>();

        for (int i = 0; i < cnt; i++)
            res.add(new IgnitePair<>("f" + i, i % 2 == 0 ? "integer" : "varchar(100)"));

        return res;
    }

    /**
     * Generates indexes for given table and fields.
     *
     * @param tblName Table name.
     * @param fields Fields list, returned by {@link #fields}.
     * @return List of pairs, first is index name, second is list of fields, covered by index, divived by comma.
     */
    private static List<IgnitePair<String>> idxs(String tblName, List<IgnitePair<String>> fields) {
        List<IgnitePair<String>> res = new LinkedList<>();

        res.addAll(fields.stream().map(f -> new IgnitePair<>(tblName + "_" + f.get1() + "_idx", f.get1())).collect(toList()));

        // Add one multicolumn index.
        if (fields.size() > 1) {
            res.add(new IgnitePair<>(
                tblName + "_" + fields.get(0).get1() + "_" + fields.get(1).get1() + "_idx",
                fields.get(0).get1() + "," + fields.get(1).get1()
            ));
        }

        return res;
    }

    /**
     * Creates an sql table, indexes and fill it with some data.
     *
     * @param cache Ignite cache
     * @param insert {@code True} if only insert data, {@code false} if delete and {@code null} all at once.
     * @param info Table info.
     */
    private static void createAndFillTable(IgniteCache cache, TableInfo info, @Nullable Boolean insert) {
        String idxToDelName = info.tblName + "_idx_to_delete";

        if (isNull(insert) || insert) {
            List<IgnitePair<String>> fields = fields(info.fieldsCnt);
            List<IgnitePair<String>> idxs = idxs(info.tblName, fields);

            String strFields = fields.stream().map(f -> f.get1() + " " + f.get2()).collect(joining(", "));

            query(
                cache,
                "create table " + info.tblName + " (id integer primary key, " + strFields + ") with " +
                    "\"CACHE_NAME=" + info.tblName + ", CACHE_GROUP=" + CACHE_GROUP_NAME + "\""
            );

            for (IgnitePair<String> idx : idxs)
                query(cache, format("create index %s on %s (%s)", idx.get1(), info.tblName, idx.get2()));

            query(cache, format("create index %s on %s (%s)", idxToDelName, info.tblName, fields.get(0).get1()));

            for (int i = 0; i < info.rec; i++)
                insertQuery(cache, info.tblName, fields, i);
        }

        if (isNull(insert) || !insert) {
            for (int i = info.rec - info.del; i < info.rec; i++)
                query(cache, "delete from " + info.tblName + " where id = " + i);

            query(cache, "drop index " + idxToDelName);
        }
    }

    /**
     * Performs an insert query.
     *
     * @param cache Ignite cache.
     * @param tblName Table name.
     * @param fields List of fields.
     * @param cntr Counter which is used to generate data.
     */
    private static void insertQuery(IgniteCache cache, String tblName, List<IgnitePair<String>> fields, int cntr) {
        GridStringBuilder q = new GridStringBuilder().a("insert into ").a(tblName).a(" (id, ");

        q.a(fields.stream().map(IgniteBiTuple::get1).collect(joining(", ")));
        q.a(") values (");
        q.a(fields.stream().map(f -> "?").collect(joining(", ", "?, ", ")")));

        Object[] paramVals = new Object[fields.size() + 1];

        for (int i = 0; i < fields.size() + 1; i++)
            paramVals[i] = (i % 2 == 0) ? cntr : valueOf(cntr);

        query(cache, q.toString(), paramVals);
    }

    /**
     * Performs a query.
     *
     * @param cache Ignite cache.
     * @param qry Query string.
     * @return Result.
     */
    private static List<List<?>> query(IgniteCache cache, String qry) {
        return cache.query(new SqlFieldsQuery(qry)).getAll();
    }

    /**
     * Performs a query.
     *
     * @param cache Ignite cache.
     * @param qry Query string.
     * @param args Query arguments.
     * @return Result.
     */
    private static List<List<?>> query(IgniteCache cache, String qry, Object... args) {
        return cache.query(new SqlFieldsQuery(qry).setArgs(args)).getAll();
    }

    /**
     * Checks the index reader output.
     *
     * @param output Output.
     * @param treesCnt Count of b+ trees.
     * @param travErrCnt Count of errors that can occur during traversal.
     * @param pageListsErrCnt Count of errors that can occur during page lists scan.
     * @param seqErrCnt Count of errors that can occur during sequential scan.
     */
    private void checkOutput(
        String output,
        int treesCnt,
        int travErrCnt,
        int pageListsErrCnt,
        int seqErrCnt
    ) {
        assertContains(log, output, RECURSIVE_TRAVERSE_NAME + "Total trees: " + treesCnt);
        assertContains(log, output, HORIZONTAL_SCAN_NAME + "Total trees: " + treesCnt);
        assertContains(log, output, RECURSIVE_TRAVERSE_NAME + "Total errors during trees traversal: " +
            (travErrCnt >= 0 ? travErrCnt : ""));
        assertContains(log, output, HORIZONTAL_SCAN_NAME + "Total errors during trees traversal: " +
            (travErrCnt >= 0 ? travErrCnt : ""));
        assertContains(log, output, "Total errors during lists scan: " + pageListsErrCnt);

        if (travErrCnt == 0)
            assertContains(log, output, "No index size consistency errors found.");
        else if (travErrCnt > 0)
            assertContains(log, output, "Index size inconsistency");

        if (seqErrCnt >= 0)
            assertContains(log, output, "Total errors occurred during sequential scan: " + seqErrCnt);
        else
            assertContains(log, output, "Orphan pages were not reported due to --indexes filter.");

        if (travErrCnt == 0 && pageListsErrCnt == 0 && seqErrCnt == 0)
            assertFalse(output.contains(ERROR_PREFIX));
    }

    /**
     * Checks output info for indexes.
     *
     * @param output Output string.
     * @param info Table info, which is used to calculate index info.
     * @param corruptedIdx Whether some index is corrupted.
     */
    private void checkIdxs(String output, TableInfo info, boolean corruptedIdx) {
        List<IgnitePair<String>> fields = fields(info.fieldsCnt);
        List<IgnitePair<String>> idxs = idxs(info.tblName, fields);

        int entriesCnt = info.rec - info.del;

        idxs.stream().map(IgniteBiTuple::get1)
            .forEach(idx -> {
                checkIdx(output, RECURSIVE_TRAVERSE_NAME, idx.toUpperCase(), entriesCnt, corruptedIdx);
                checkIdx(output, HORIZONTAL_SCAN_NAME, idx.toUpperCase(), entriesCnt, corruptedIdx);
            });
    }

    /**
     * Checks output info for single index.
     *
     * @param output Output string.
     * @param traversePrefix Traverse prefix.
     * @param idx Index name.
     * @param entriesCnt Count of entries that should be present in index.
     * @param canBeCorrupted Whether index can be corrupted.
     */
    private void checkIdx(String output, String traversePrefix, String idx, int entriesCnt, boolean canBeCorrupted) {
        Pattern ptrnCorrect = compile(checkIdxRegex(traversePrefix, false, idx, 1, valueOf(entriesCnt)));

        Matcher mCorrect = ptrnCorrect.matcher(output);

        if (canBeCorrupted) {
            Pattern ptrnCorrupted = compile(checkIdxRegex(traversePrefix, true, idx, 0, "[0-9]{1,4}"));

            Matcher mCorrupted = ptrnCorrupted.matcher(output);

            assertTrue("could not find index " + idx + ":\n" + output, mCorrect.find() || mCorrupted.find());
        }
        else
            assertTrue("could not find index " + idx + ":\n" + output, mCorrect.find());
    }

    /**
     * Returns regexp string for index check.
     *
     * @param traversePrefix Traverse prefix.
     * @param withErrors Whether errors should be present.
     * @param idxName Index name.
     * @param minimumPageStatSize Minimum size of page stats for index.
     * @param itemsCnt Count of data entries.
     * @return Regexp string.
     */
    private String checkIdxRegex(
        String traversePrefix,
        boolean withErrors,
        String idxName,
        int minimumPageStatSize,
        String itemsCnt
    ) {
        return format(
                withErrors ? CHECK_IDX_PTRN_WITH_ERRORS : CHECK_IDX_PTRN_CORRECT,
                idxName,
                minimumPageStatSize,
                itemsCnt
            )
            .replace("<PREFIX>", traversePrefix);
    }

    /**
     * Runs index reader on given cache group.
     *
     * @param workDir Work directory.
     * @param cacheGrp Cache group name.
     * @param idxs Indexes to check.
     * @param checkParts Whether to check cache data tree in partitions.
     * @param snapshot Snapshot directory or not.
     * @return Index reader output.
     * @throws IgniteCheckedException If failed.
     */
    private String runIndexReader(
        File workDir,
        String cacheGrp,
        @Nullable String[] idxs,
        boolean checkParts,
        boolean snapshot
    ) throws IgniteCheckedException {
        File dir = new File(workDir, dataDir(cacheGrp, snapshot));

        OutputStream destStream = new ByteArrayOutputStream();

        try (IgniteIndexReader reader = new IgniteIndexReader(
            dir,
            snapshot,
            PAGE_SIZE,
            PART_CNT,
            PAGE_STORE_VER,
            idxs,
            checkParts,
            null,
            destStream
        )) {
            reader.readIdx();
        }

        return destStream.toString();
    }

    /**
     * Test checks correctness of index for normal pds and snapshots.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testCorrectIdx() throws IgniteCheckedException {
        checkCorrectIdx(workDir, false);
//        checkCorrectIdx(fullSnapshotDir, true);
        checkCorrectIdx(incSnapshotDir, true);
    }

    /**
     * Test checks correctness of index and consistency of partitions with it for normal pds and snapshots.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testCorrectIdxWithCheckParts() throws IgniteCheckedException {
        checkCorrectIdxWithCheckParts(workDir, false);
//        checkCorrectIdxWithCheckParts(fullSnapshotDir, true);
        checkCorrectIdxWithCheckParts(incSnapshotDir, true);
    }

    /**
     * Test verifies that specific indexes being checked are correct for normal pds and snapshots.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testCorrectIdxWithFilter() throws IgniteCheckedException {
        checkCorrectIdxWithFilter(workDir, false);
//        checkCorrectIdxWithFilter(fullSnapshotDir, true);
        checkCorrectIdxWithFilter(incSnapshotDir, true);
    }

    /**
     * Test checks whether the index of an empty group is correct for normal pds and snapshots.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testEmpty() throws IgniteCheckedException {
        checkEmpty(workDir, false);
//        checkEmpty(fullSnapshotDir, true);
        checkEmpty(incSnapshotDir, true);
    }

    /**
     * Test for finding corrupted pages in index for normal pds and snapshots.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedIdx() throws Exception {
        checkCorruptedIdx(workDir, false);
//        checkCorruptedIdx(fullSnapshotDir, true);
        checkCorruptedIdx(incSnapshotDir, true);
    }

    /**
     * Test for finding corrupted pages in index
     * and checking for consistency in partitions for normal pds and snapshots.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedIdxWithCheckParts() throws Exception {
        if (1 != 1) {
            PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS, H2MvccInnerIO.VERSIONS, H2MvccLeafIO.VERSIONS);

            H2ExtrasInnerIO.register();
            H2ExtrasLeafIO.register();

            ByteBuffer buf = GridUnsafe.allocateBuffer(PAGE_SIZE);
            long addr = GridUnsafe.bufferAddress(buf);

            File file = new File("C:\\Users\\tkalk\\IdeaProjects\\apache-ignite\\work\\snapshot\\ts_20200519184344_1589903024520.snapshot\\c2fb2a59_0c8a_4359_80eb_7d1e71a25b24\\-672468802\\index.bin");
//            File file = new File("C:\\Users\\tkalk\\IdeaProjects\\apache-ignite\\work\\db\\copy\\cacheGroup-defaultGroup\\index.bin");

            try (FileChannel c = new RandomAccessFile(file, "r").getChannel()) {
                int h = 0;
//                int h = PAGE_SIZE;

//                for (int i = 30; i < 50; i++) {
                for (int i = 0; i < file.length() / PAGE_SIZE; i++) {
                    buf.rewind();

                    c.read(buf, h + (i * PAGE_SIZE));
                    buf.rewind();

                    long pageId = PageIO.getPageId(addr);
                    int pageIdx = PageIdUtils.pageIndex(pageId);
                    int type = PageIO.getType(addr);

                    System.out.println("pageId0=" + pageId);
                    System.out.println("pageIdx0=" + pageIdx);
                    System.out.println("type0=" + type);
                    System.out.println();
                }
            }
            finally {
                GridUnsafe.freeBuffer(buf);
            }
        }
        else {
            checkCorruptedIdxWithCheckParts(workDir, false);
//        checkCorruptedIdxWithCheckParts(fullSnapshotDir, true);
            checkCorruptedIdxWithCheckParts(incSnapshotDir, true);
        }
    }

    /**
     * Test for finding corrupted pages in partition for normal pds and snapshots.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedPart() throws Exception {
        checkCorruptedPart(workDir, false);
//        checkCorruptedPart(fullSnapshotDir, true);
        checkCorruptedPart(incSnapshotDir, true);
    }

    /**
     * Test for finding corrupted pages in index and partition for normal pds and snapshots.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorruptedIdxAndPart() throws Exception {
        checkCorruptedIdxAndPart(workDir, false);
//        checkCorruptedIdxAndPart(fullSnapshotDir, true);
        checkCorruptedIdxAndPart(incSnapshotDir, true);
    }

    /**
     * Test checks correctness of index of {@link #QUERY_CACHE_GROUP_NAME} for normal pds and snapshots.
     *
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testQryCacheGroup() throws IgniteCheckedException {
        checkQryCacheGroup(workDir, false);
//        checkQryCacheGroup(fullSnapshotDir, true);
        checkQryCacheGroup(incSnapshotDir, true);
    }

    /**
     * Checks whether corrupted pages are found in index and partition.
     *
     * @param workDir Work directory.
     * @param snapshot Snapshot directory or not.
     * @throws Exception If failed.
     */
    private void checkCorruptedIdxAndPart(File workDir, boolean snapshot) throws Exception {
        corruptFile(workDir, INDEX_PARTITION, 7, snapshot);
        if (snapshot)corruptFile(fullSnapshotDir, INDEX_PARTITION, 7, snapshot);
        corruptFile(workDir, 0, 5, snapshot);
        if (snapshot)corruptFile(fullSnapshotDir, 0, 5, snapshot);

        try {
            String output = runIndexReader(workDir, CACHE_GROUP_NAME, null, false, snapshot);

            checkOutput(output, 19, -1, 0, 2);

            for (int i = 0; i < CREATED_TABLES_CNT; i++)
                checkIdxs(output, TableInfo.generate(i), true);
        }
        finally {
            restoreFile(workDir, INDEX_PARTITION, snapshot);
            if (snapshot)restoreFile(fullSnapshotDir, INDEX_PARTITION, snapshot);
            restoreFile(workDir, 0, snapshot);
            if (snapshot)restoreFile(fullSnapshotDir, 0, snapshot);
        }
    }

    /**
     * Checks whether corrupted pages are found in partition.
     *
     * @param workDir Work directory.
     * @param snapshot Snapshot directory or not.
     * @throws Exception If failed.
     */
    private void checkCorruptedPart(File workDir, boolean snapshot) throws Exception {
        corruptFile(workDir, 0, 7, snapshot);

        try {
            String output = runIndexReader(workDir, CACHE_GROUP_NAME, null, false, snapshot);

            checkOutput(output, 19, -1, 0, 0);

            for (int i = 0; i < CREATED_TABLES_CNT; i++)
                checkIdxs(output, TableInfo.generate(i), true);
        }
        finally {
            restoreFile(workDir, 0, snapshot);
        }
    }

    /**
     * Checking for corrupted pages in index and checking for consistency in partitions.
     *
     * @param workDir Work directory.
     * @param snapshot Snapshot directory or not.
     * @throws Exception If failed.
     */
    private void checkCorruptedIdxWithCheckParts(File workDir, boolean snapshot) throws Exception {
        for (int i = snapshot ? 31 : 30; i < (snapshot ? 35 : 50); i++) {
            corruptFile(workDir, INDEX_PARTITION, i, snapshot);
            if (snapshot) corruptFile(fullSnapshotDir, INDEX_PARTITION, i, snapshot);
        }

        try {
            String output = runIndexReader(workDir, CACHE_GROUP_NAME, null, true, snapshot);

            // Pattern with errors count > 9
            Pattern ptrn = compile(
                "Partition check finished, total errors: [0-9]{2,5}, total problem partitions: [0-9]{2,5}"
            );

            assertTrue(output, ptrn.matcher(output).find());

            assertContains(log, output, "Total errors during lists scan: 0");
        }
        finally {
            restoreFile(workDir, INDEX_PARTITION, snapshot);
            if (snapshot) restoreFile(fullSnapshotDir, INDEX_PARTITION, snapshot);
        }
    }

    /**
     * Checks whether corrupted pages are found in index.
     *
     * @param workDir Work directory.
     * @param snapshot Snapshot directory or not.
     * @throws Exception If failed.
     */
    private void checkCorruptedIdx(File workDir, boolean snapshot) throws Exception {
        corruptFile(workDir, INDEX_PARTITION, 5, snapshot);
        if (snapshot)corruptFile(fullSnapshotDir, INDEX_PARTITION, 5, snapshot);

        try {
            String output = runIndexReader(workDir, CACHE_GROUP_NAME, null, false, snapshot);

            // 1 corrupted page detected while traversing, and 1 index size inconsistency error.
            int travErrCnt = 2;

            // 2 errors while sequential scan: 1 page with unknown IO type, and 1 correct, but orphan innerIO page.
            int seqErrCnt = 2;

            checkOutput(output, 19, travErrCnt, 0, seqErrCnt);

            for (int i = 0; i < CREATED_TABLES_CNT; i++)
                checkIdxs(output, TableInfo.generate(i), true);
        }
        finally {
            restoreFile(workDir, INDEX_PARTITION, snapshot);
            if (snapshot)restoreFile(fullSnapshotDir, INDEX_PARTITION, snapshot);
        }
    }

    /**
     * Checking correctness of index for {@link #QUERY_CACHE_GROUP_NAME}.
     *
     * @param workDir Work directory.
     * @param snapshot Snapshot directory or not.
     * @throws IgniteCheckedException If failed.
     */
    private void checkQryCacheGroup(File workDir, boolean snapshot) throws IgniteCheckedException {
        String output = runIndexReader(workDir, QUERY_CACHE_GROUP_NAME, null, false, snapshot);

        checkOutput(output, 5, 0, 0, 0);
    }

    /**
     * Checking correctness of index.
     *
     * @param workDir Work directory.
     * @param snapshot Snapshot directory or not.
     * @throws IgniteCheckedException If failed.
     */
    private void checkCorrectIdx(File workDir, boolean snapshot) throws IgniteCheckedException {
        String output = runIndexReader(workDir, CACHE_GROUP_NAME, null, false, snapshot);

        checkOutput(output, 19, 0, 0, 0);

        for (int i = 0; i < CREATED_TABLES_CNT; i++)
            checkIdxs(output, TableInfo.generate(i), false);
    }

    /**
     * Checking correctness of index and consistency of partitions with it.
     *
     * @param workDir Work directory.
     * @param snapshot Snapshot directory or not.
     * @throws IgniteCheckedException If failed.
     */
    private void checkCorrectIdxWithCheckParts(File workDir, boolean snapshot) throws IgniteCheckedException {
        String output = runIndexReader(workDir, CACHE_GROUP_NAME, null, true, snapshot);

        checkOutput(output, 19, 0, 0, 0);

        for (int i = 0; i < CREATED_TABLES_CNT; i++)
            checkIdxs(output, TableInfo.generate(i), false);

        assertContains(log, output, "Partitions check detected no errors.");
        assertContains(log, output, "Partition check finished, total errors: 0, total problem partitions: 0");
    }

    /**
     * Checking that specific indexes being checked are correct.
     *
     * @param workDir Work directory.
     * @param snapshot Snapshot directory or not.
     * @throws IgniteCheckedException If failed.
     */
    private void checkCorrectIdxWithFilter(File workDir, boolean snapshot) throws IgniteCheckedException {
        String[] idxsToCheck = {"2654_-1177891018_T2_F1_IDX##H2Tree%0", "2654_-1177891018_T2_F2_IDX##H2Tree%0"};

        String output = runIndexReader(workDir, CACHE_GROUP_NAME, idxsToCheck, false, snapshot);

        checkOutput(output, 3, 0, 0, -1);

        Set<String> idxSet = new HashSet<>(asList(idxsToCheck));

        for (int i = 0; i < CREATED_TABLES_CNT; i++) {
            TableInfo info = TableInfo.generate(i);

            List<IgnitePair<String>> fields = fields(info.fieldsCnt);
            List<IgnitePair<String>> idxs = idxs(info.tblName, fields);

            int entriesCnt = info.rec - info.del;

            idxs.stream().map(IgniteBiTuple::get1)
                .filter(idxSet::contains)
                .forEach(idx -> {
                    checkIdx(output, RECURSIVE_TRAVERSE_NAME, idx.toUpperCase(), entriesCnt, false);
                    checkIdx(output, HORIZONTAL_SCAN_NAME, idx.toUpperCase(), entriesCnt, false);
                });
        }
    }

    /**
     * Validating index of an empty group.
     *
     * @param workDir Work directory.
     * @param snapshot Snapshot directory or not.
     * @throws IgniteCheckedException If failed.
     */
    private void checkEmpty(File workDir, boolean snapshot) throws IgniteCheckedException {
        // Check output for empty cache group.
        String output = runIndexReader(workDir, EMPTY_CACHE_GROUP_NAME, null, false, snapshot);

        checkOutput(output, 1, 0, 0, 0);

        // Create an empty directory and try to check it.
        String newCleanGrp = "noCache";

        File cleanDir = new File(workDir, dataDir(newCleanGrp, snapshot));

        try {
            cleanDir.mkdir();

            GridTestUtils.assertThrows(
                log,
                () -> runIndexReader(workDir, newCleanGrp, null, false, snapshot),
                IgniteCheckedException.class,
                null
            );
        }
        finally {
            U.delete(cleanDir);
        }
    }

    /**
     *
     */
    private static class TableInfo {
        /** Table name. */
        final String tblName;

        /** Fields count. */
        final int fieldsCnt;

        /** Count of records that should be inserted. */
        final int rec;

        /** Count of records that should be deleted after insert.*/
        final int del;

        /** */
        public TableInfo(String tblName, int fieldsCnt, int rec, int del) {
            this.tblName = tblName;
            this.fieldsCnt = fieldsCnt;
            this.rec = rec;
            this.del = del;
        }

        /**
         * Generates some table info for given int.
         * @param i Some integer.
         * @return Table info.
         */
        public static TableInfo generate(int i) {
            return new TableInfo("T" + i, 3 + (i % 3), 1700 - (i % 3) * 500, (i % 3) * 250);
        }
    }

    /**
     *
     */
    private static class TestClass1 {
        /** */
        private final Integer f;

        /** */
        private final String s;

        /** */
        public TestClass1(Integer f, String s) {
            this.f = f;
            this.s = s;
        }
    }

    /**
     *
     */
    private static class TestClass2 extends TestClass1 {
        /** */
        public TestClass2(Integer f, String s) {
            super(f, s);
        }
    }
}
