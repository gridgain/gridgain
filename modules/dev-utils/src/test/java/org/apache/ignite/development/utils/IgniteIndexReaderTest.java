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
import java.io.UncheckedIOException;
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
import org.junit.Before;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class IgniteIndexReaderTest {
    /** */
    private static final int PAGE_SIZE = 4096;

    /** */
    private static final int PART_CNT = 1024;

    /** */
    private static final int PAGE_STORE_VER = 2;

    /** */
    private static final String CACHE_NAME = "default";

    /** Cache group name. */
    private static final String CACHE_GROUP_NAME = "defaultGroup";

    /** */
    private static final int CREATED_TABLES_CNT = 3;

    /** */
    private static final String LINE_DELIM = System.lineSeparator();

    @Before
    public void before() throws Exception {
        cleanPersistenceDir();
    }

    @After
    public void after() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    protected void cleanPersistenceDir() throws Exception {
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }

    /** */
    private static final String CHECK_IDX_PTRN_COMMON =
        "Index tree: I \\[idxName=[\\-_0-9]{1,20}_%s##H2Tree.0, pageId=[0-9a-f]{16}\\]" +
        LINE_DELIM + "-- Page stat:" +
        LINE_DELIM + "([0-9a-zA-Z]{1,50}: [0-9]{1,5}" +
        LINE_DELIM + "){%s,1000}-- Count of items found in leaf pages: %s" +
        LINE_DELIM;

    /** */
    private static final String CHECK_IDX_PTRN_CORRECT =
        CHECK_IDX_PTRN_COMMON + "No errors occurred while traversing.";

    /** */
    private static final String CHECK_IDX_PTRN_WITH_ERRORS =
        CHECK_IDX_PTRN_COMMON + "-- Errors:" +
        LINE_DELIM + "Page id=[0-9]{1,30}, exceptions:" +
        LINE_DELIM + "class.*?Exception.*";

    /** */
    private IgniteConfiguration getConfiguration() {
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
                    .setSqlSchema("PUBLIC")
            );

        return cfg;
    }

    /** */
    private File prepareIndex(boolean withCorruption) {
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

        if (withCorruption) {
            try {
                corruptIndex(cacheWorkDir);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return cacheWorkDir;
    }

    private void corruptIndex(File cacheWorkDir) throws IOException {
        File idxFile = new File(cacheWorkDir, INDEX_FILE_NAME);

        try (RandomAccessFile file = new RandomAccessFile(idxFile, "rw")) {
            byte[] trash = new byte[PAGE_SIZE];

            ThreadLocalRandom.current().nextBytes(trash);

            file.seek(10 * PAGE_SIZE + FilePageStore.HEADER_SIZE);

            file.write(trash);
        }
    }

    private List<IgnitePair<String>> fields(int cnt) {
        List<IgnitePair<String>> res = new LinkedList<>();

        for (int i = 0; i < cnt; i++)
            res.add(new IgnitePair<>("f" + i, i % 2 == 0 ? "integer" : "varchar(100)"));

        return res;
    }

    private List<IgnitePair<String>> idxs(String tblName, List<IgnitePair<String>> fields) {
        List<IgnitePair<String>> res = new LinkedList<>();

        res.addAll(fields.stream().map(f -> new IgnitePair<>(tblName + "_" + f.get1() + "_idx", f.get1())).collect(toList()));

        if (fields.size() > 1) {
            res.add(new IgnitePair<>(
                tblName + "_" + fields.get(0).get1() + "_" + fields.get(1).get1() + "_idx",
                fields.get(0).get1() + "," + fields.get(1).get1()
            ));
        }

        return res;
    }

    /** */
    private void createAndFillTable(IgniteCache cache, TableInfo info) {
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

    private void insertQuery(IgniteCache cache, String tblName, List<IgnitePair<String>> fields, int cntr) {
        GridStringBuilder q = new GridStringBuilder().a("insert into ").a(tblName).a(" (id, ");

        q.a(fields.stream().map(IgniteBiTuple::get1).collect(joining(", ")));
        q.a(") values (");
        q.a(fields.stream().map(f -> "?").collect(joining(", ", "?, ", ")")));

        Object[] paramVals = new Object[fields.size() + 1];

        for (int i = 0; i < fields.size() + 1; i++)
            paramVals[i] = (i % 2 == 0) ? cntr : String.valueOf(cntr);

        query(cache, q.toString(), paramVals);
    }

    /** */
    private List<List<?>> query(IgniteCache cache, String qry) {
        return cache.query(new SqlFieldsQuery(qry)).getAll();
    }

    /** */
    private List<List<?>> query(IgniteCache cache, String qry, Object... args) {
        return cache.query(new SqlFieldsQuery(qry).setArgs(args)).getAll();
    }

    /** */
    private void forceCheckpoint(IgniteEx ignite) {
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)(ignite).context()
            .cache().context().database();

        try {
            dbMgr.waitForCheckpoint("test");
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    private void checkOutput(String output, int treesCnt, int travErrCnt, int pageListsErrCnt, int seqErrCnt) {
        assertTrue(output.contains("Total trees: " + treesCnt));
        assertTrue(output.contains("Total errors during trees traversal: " + travErrCnt));
        assertTrue(output.contains("Total errors during lists scan: " + pageListsErrCnt));
        assertTrue(output.contains("Total errors occurred during sequential scan: " + seqErrCnt));
    }

    private void checkIdxs(String output, TableInfo info, boolean withCorruption) {
        List<IgnitePair<String>> fields = fields(info.fieldsCnt);
        List<IgnitePair<String>> idxs = idxs(info.tblName, fields);

        int entriesCnt = info.rec - info.del;

        idxs.stream().map(IgniteBiTuple::get1)
            .forEach(idx -> checkIdx(output, idx.toUpperCase(), entriesCnt, withCorruption));
    }

    private void checkIdx(String output, String idx, int entriesCnt, boolean canBeCorrupted) {
        Pattern ptrnCorrect = Pattern.compile(checkIdxRegex(false, idx, 1, entriesCnt));

        Matcher mCorrect = ptrnCorrect.matcher(output);

        if (canBeCorrupted) {
            Pattern ptrnCorrupted = Pattern.compile(checkIdxRegex(true, idx, 0, 0));

            Matcher mCorrupted = ptrnCorrupted.matcher(output);

            assertTrue("could not find index " + idx + ":\n" + output, mCorrect.find() || mCorrupted.find());
        }
        else
            assertTrue("could not find index " + idx + ":\n" + output, mCorrect.find());
    }

    private String checkIdxRegex(boolean withErrors, String idxName, int minimumPageStatSize, int itemsCnt) {
        return String.format(
            withErrors ? CHECK_IDX_PTRN_WITH_ERRORS : CHECK_IDX_PTRN_CORRECT,
            idxName,
            minimumPageStatSize,
            itemsCnt
        );
    }

    private void doTest(boolean withCorruption) throws IgniteCheckedException {
        File dir = prepareIndex(withCorruption);

        OutputStream destStream = new StringBuilderOutputStream();

        try (IgniteIndexReader reader = new IgniteIndexReader(dir.getAbsolutePath(), PAGE_SIZE, PART_CNT, PAGE_STORE_VER, destStream)) {
            reader.readIdx();
        }

        String output = destStream.toString();

        // 1 corrupted page detected while traversing.
        int travErrCnt = withCorruption ? 1 : 0;

        // 2 errors while sequential scan: 1 page with unknown IO type, and 1 correct, but orphan innerIO page.
        int seqErrCnt = withCorruption ? 2 : 0;

        checkOutput(output, 19, travErrCnt, 0, seqErrCnt);

        for (int i = 0; i < CREATED_TABLES_CNT; i++)
            checkIdxs(output, TableInfo.generate(i), withCorruption);
    }

    /** */
    @Test
    public void testCorrectIdx() throws IgniteCheckedException {
        doTest(false);
    }

    /** */
    @Test
    public void testCorrupted() throws IgniteCheckedException {
        doTest(true);
    }

    private static class TableInfo {
        final String tblName;
        final int fieldsCnt;
        final int rec;
        final int del;

        public TableInfo(String tblName, int fieldsCnt, int rec, int del) {
            this.tblName = tblName;
            this.fieldsCnt = fieldsCnt;
            this.rec = rec;
            this.del = del;
        }

        public static TableInfo generate(int i) {
            return new TableInfo("T" + i, 3 + (i % 3), 1700 - (i % 3) * 500, (i % 3) * 250);
        }
    }
}
