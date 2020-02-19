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
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class IgniteIndexReaderTest {
    /** Page size. */
    private static final int PAGE_SIZE = 4096;

    /** Partitions count. */
    private static final int PART_CNT = 1024;

    /** Version of file page stores. */
    private static final int PAGE_STORE_VER = 2;

    /** Cache name. */
    private static final String CACHE_NAME = "default";

    /** Cache group name. */
    private static final String CACHE_GROUP_NAME = "defaultGroup";

    /** Cache group without indexes. */
    private static final String EMPTY_CACHE_NAME = "empty";

    /** Cache group without indexes. */
    private static final String EMPTY_CACHE_GROUP_NAME = "emptyGroup";

    /** Count of tables that will be created for test. */
    private static final int CREATED_TABLES_CNT = 3;

    /** Line delimiter. */
    private static final String LINE_DELIM = System.lineSeparator();

    /** Common part of regexp for single index output validation. */
    private static final String CHECK_IDX_PTRN_COMMON =
        "Index tree: I \\[idxName=[\\-_0-9]{1,20}_%s##H2Tree.0, pageId=[0-9a-f]{16}\\]" +
            LINE_DELIM + "-- Page stat:" +
            LINE_DELIM + "([0-9a-zA-Z]{1,50}: [0-9]{1,5}" +
            LINE_DELIM + "){%s,1000}-- Count of items found in leaf pages: %s" +
            LINE_DELIM;

    /** Regexp to validate output of correct index. */
    private static final String CHECK_IDX_PTRN_CORRECT =
        CHECK_IDX_PTRN_COMMON + "No errors occurred while traversing.";

    /** Regexp to validate output of corrupted index. */
    private static final String CHECK_IDX_PTRN_WITH_ERRORS =
        CHECK_IDX_PTRN_COMMON + "-- Errors:" +
            LINE_DELIM + "Page id=[0-9]{1,30}, exceptions:" +
            LINE_DELIM + "class.*?Exception.*";

    /** Work directory, containing cache group directories. */
    private static File workDir;

    /** */
    @BeforeClass
    public static void before() throws Exception {
        cleanPersistenceDir();

        workDir = prepareIndex();
    }

    /** */
    @AfterClass
    public static void after() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * Cleans persistent directory.
     * @throws Exception If failed.
     */
    protected static void cleanPersistenceDir() throws Exception {
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }

    /**
     * Generates a grid configuration.
     * @return Ignite configuration.
     */
    private static IgniteConfiguration getConfiguration() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setPageSize(PAGE_SIZE)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(10 * 1024L * 1024L)
                        .setMaxSize(50 * 1024L * 1024L)
                )
            )
            .setCacheConfiguration(
                new CacheConfiguration(CACHE_NAME)
                    .setGroupName(CACHE_GROUP_NAME)
                    .setAffinity(new RendezvousAffinityFunction(false, PART_CNT))
                    .setSqlSchema("PUBLIC"),
                new CacheConfiguration(EMPTY_CACHE_NAME)
                    .setGroupName(EMPTY_CACHE_GROUP_NAME)
            );

        return cfg;
    }

    /**
     * Runs a grid to prepare directory with index and data partitions.
     * @return Work directory.
     */
    private static File prepareIndex() {
        IgniteEx ignite = (IgniteEx)Ignition.start(getConfiguration());

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < CREATED_TABLES_CNT; i++)
            createAndFillTable(cache, TableInfo.generate(i));

        forceCheckpoint(ignite);

        IgniteInternalCache<Integer, Integer> cacheEx = ignite.cachex(CACHE_NAME);

        File cacheWorkDir =
            ((FilePageStoreManager)cacheEx.context().shared().pageStore()).cacheWorkDir(cacheEx.configuration());

        Ignition.stop(ignite.name(), true);

        return cacheWorkDir.getParentFile();
    }

    /**
     * Corrupts partition file.
     * @param partId Partition id.
     * @param pageNum Page to corrupt.
     * @throws IOException If failed.
     */
    private void corruptFile(int partId, int pageNum) throws IOException {
        String fileName = partId == INDEX_PARTITION ? INDEX_FILE_NAME : String.format(PART_FILE_TEMPLATE, partId);

        File cacheWorkDir = new File(workDir, "cacheGroup-" + CACHE_GROUP_NAME);

        File file = new File(cacheWorkDir, fileName);

        Files.copy(file.toPath(), new File(cacheWorkDir, fileName + ".backup").toPath());

        try (RandomAccessFile f = new RandomAccessFile(file, "rw")) {
            byte[] trash = new byte[PAGE_SIZE];

            ThreadLocalRandom.current().nextBytes(trash);

            f.seek(pageNum * PAGE_SIZE + FilePageStore.HEADER_SIZE);

            f.write(trash);
        }
    }

    /**
     * Restores corrupted file from backup after corruption.
     * @param partId Partition id.
     * @throws IOException If failed.
     */
    private void restoreFile(int partId) throws IOException {
        String fileName = partId == INDEX_PARTITION ? INDEX_FILE_NAME : String.format(PART_FILE_TEMPLATE, partId);

        File cacheWorkDir = new File(workDir, "cacheGroup-" + CACHE_GROUP_NAME);

        Path backupFilesPath = new File(cacheWorkDir, fileName + ".backup").toPath();

        Files.copy(backupFilesPath, new File(cacheWorkDir, fileName).toPath(), REPLACE_EXISTING);

        Files.delete(backupFilesPath);
    }

    /**
     * Generates fields for sql table.
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
     * @param cache Ignite cache.
     * @param info Table info.
     */
    private static void createAndFillTable(IgniteCache cache, TableInfo info) {
        List<IgnitePair<String>> fields = fields(info.fieldsCnt);
        List<IgnitePair<String>> idxs = idxs(info.tblName, fields);

        String strFields = fields.stream().map(f -> f.get1() + " " + f.get2()).collect(joining(", "));

        query(
            cache,
            "create table " + info.tblName + " (id integer primary key, " + strFields + ") with " +
                "\"CACHE_NAME=" + info.tblName + ", CACHE_GROUP=" + CACHE_GROUP_NAME + "\""
        );

        for (IgnitePair<String> idx : idxs)
            query(cache, String.format("create index %s on %s (%s)", idx.get1(), info.tblName, idx.get2()));

        for (int i = 0; i < info.rec; i++)
            insertQuery(cache, info.tblName, fields, i);

        for (int i = info.rec - info.del; i < info.rec; i++)
            query(cache, "delete from " + info.tblName + " where id = " + i);
    }

    /**
     * Performs an insert query.
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
            paramVals[i] = (i % 2 == 0) ? cntr : String.valueOf(cntr);

        query(cache, q.toString(), paramVals);
    }

    /**
     * Performs a query.
     * @param cache Ignite cache.
     * @param qry Query string.
     * @return Result.
     */
    private static List<List<?>> query(IgniteCache cache, String qry) {
        return cache.query(new SqlFieldsQuery(qry)).getAll();
    }

    /**
     * Performs a query.
     * @param cache Ignite cache.
     * @param qry Query string.
     * @param args Query arguments.
     * @return Result.
     */
    private static List<List<?>> query(IgniteCache cache, String qry, Object... args) {
        return cache.query(new SqlFieldsQuery(qry).setArgs(args)).getAll();
    }

    /**
     * Makes a force checkpoint for given Ignite instance.
     * @param ignite Ignite instance.
     */
    private static void forceCheckpoint(IgniteEx ignite) {
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)(ignite).context()
            .cache().context().database();

        try {
            dbMgr.waitForCheckpoint("test");
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Checks that first string contains the second.
     * @param str String.
     * @param substr String.
     */
    private static void assertContains(String str, String substr) {
        try {
            assertTrue(str != null && str.contains(substr));
        } catch (AssertionError e) {
            throw new AssertionError(String.format("String does not contain substring: '%s'. String: %s", substr, str));
        }
    }

    /**
     * Checks the index reader output.
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
        assertContains(output, "Total trees: " + treesCnt);
        assertContains(output, "Total errors during trees traversal: " + (travErrCnt >= 0 ? travErrCnt : ""));
        assertContains(output, "Total errors during lists scan: " + pageListsErrCnt);
        assertContains(output, "Total errors occurred during sequential scan: " + seqErrCnt);
    }

    /**
     * Checks output info for indexes.
     * @param output Output string.
     * @param info Table info, which is used to calculate index info.
     * @param corruptedIdx Whether some index is corrupted.
     */
    private void checkIdxs(String output, TableInfo info, boolean corruptedIdx) {
        List<IgnitePair<String>> fields = fields(info.fieldsCnt);
        List<IgnitePair<String>> idxs = idxs(info.tblName, fields);

        int entriesCnt = info.rec - info.del;

        idxs.stream().map(IgniteBiTuple::get1)
            .forEach(idx -> checkIdx(output, idx.toUpperCase(), entriesCnt, corruptedIdx));
    }

    /**
     * Checks output info for single index.
     * @param output Output string.
     * @param idx Index name.
     * @param entriesCnt Count of entries that should be present in index.
     * @param canBeCorrupted Whether index can be corrupted.
     */
    private void checkIdx(String output, String idx, int entriesCnt, boolean canBeCorrupted) {
        Pattern ptrnCorrect = Pattern.compile(checkIdxRegex(false, idx, 1, String.valueOf(entriesCnt)));

        Matcher mCorrect = ptrnCorrect.matcher(output);

        if (canBeCorrupted) {
            Pattern ptrnCorrupted = Pattern.compile(checkIdxRegex(true, idx, 0, "[0-9]{1,4}"));

            Matcher mCorrupted = ptrnCorrupted.matcher(output);

            assertTrue("could not find index " + idx + ":\n" + output, mCorrect.find() || mCorrupted.find());
        }
        else
            assertTrue("could not find index " + idx + ":\n" + output, mCorrect.find());
    }

    /**
     * Returns regexp string for index check.
     * @param withErrors Whether errors should be present.
     * @param idxName Index name.
     * @param minimumPageStatSize Minimum size of page stats for index.
     * @param itemsCnt Count of data entries.
     * @return Regexp string.
     */
    private String checkIdxRegex(boolean withErrors, String idxName, int minimumPageStatSize, String itemsCnt) {
        return String.format(
            withErrors ? CHECK_IDX_PTRN_WITH_ERRORS : CHECK_IDX_PTRN_CORRECT,
            idxName,
            minimumPageStatSize,
            itemsCnt
        );
    }

    /**
     * Runs index reader on given cache group.
     * @param workDir Work directory which contains cache group directories.
     * @param cacheGrp Cache group name.
     * @return Index reader output.
     * @throws IgniteCheckedException If failed.
     */
    private String runIndexReader(File workDir, String cacheGrp) throws IgniteCheckedException {
        File dir = new File(workDir, "cacheGroup-" + cacheGrp);

        OutputStream destStream = new StringBuilderOutputStream();

        try (IgniteIndexReader reader = new IgniteIndexReader(dir.getAbsolutePath(), PAGE_SIZE, PART_CNT, PAGE_STORE_VER, destStream)) {
            reader.readIdx();
        }

        return destStream.toString();
    }

    /** */
    @Test
    public void testCorrectIdx() throws IgniteCheckedException {
        String output = runIndexReader(workDir, CACHE_GROUP_NAME);

        checkOutput(output, 19, 0, 0, 0);

        for (int i = 0; i < CREATED_TABLES_CNT; i++)
            checkIdxs(output, TableInfo.generate(i), false);

        // Check output for empty cache group.
        output = runIndexReader(workDir, EMPTY_CACHE_GROUP_NAME);

        checkOutput(output, 1, 0, 0, 0);

        // Create an empty directory and try to check it.
        File cleanDir = new File(workDir, "cacheGroup-noCache");

        cleanDir.mkdir();

        RuntimeException re = null;

        try {
            runIndexReader(workDir, "noCache");
        }
        catch (RuntimeException e) {
            re = e;
        }

        assertNotNull(re);
    }

    /** */
    @Test
    public void testCorruptedIdx() throws IgniteCheckedException, IOException {
        corruptFile(INDEX_PARTITION, 10);

        try {
            String output = runIndexReader(workDir, CACHE_GROUP_NAME);

            // 1 corrupted page detected while traversing.
            int travErrCnt = 1;

            // 2 errors while sequential scan: 1 page with unknown IO type, and 1 correct, but orphan innerIO page.
            int seqErrCnt = 2;

            checkOutput(output, 19, travErrCnt, 0, seqErrCnt);

            for (int i = 0; i < CREATED_TABLES_CNT; i++)
                checkIdxs(output, TableInfo.generate(i), true);
        }
        finally {
            restoreFile(INDEX_PARTITION);
        }
    }

    /** */
    @Test
    public void testCorruptedPart() throws IgniteCheckedException, IOException {
        corruptFile(0, 8);

        try {
            String output = runIndexReader(workDir, CACHE_GROUP_NAME);

            checkOutput(output, 19, -1, 0, 0);

            for (int i = 0; i < CREATED_TABLES_CNT; i++)
                checkIdxs(output, TableInfo.generate(i), true);
        }
        finally {
            restoreFile(0);
        }
    }

    /** */
    @Test
    public void testCorruptedIdxAndPart() throws IgniteCheckedException, IOException {
        corruptFile(INDEX_PARTITION, 10);
        corruptFile(0, 8);

        try {
            String output = runIndexReader(workDir, CACHE_GROUP_NAME);

            checkOutput(output, 19, -1, 0, 2);

            for (int i = 0; i < CREATED_TABLES_CNT; i++)
                checkIdxs(output, TableInfo.generate(i), true);
        }
        finally {
            restoreFile(INDEX_PARTITION);
            restoreFile(0);
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
}
