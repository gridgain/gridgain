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

package org.apache.ignite.internal.processors.cache.index;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.SupportFeaturesUtils;
import org.apache.ignite.internal.processors.cache.AbstractDataTypesCoverageTest.Quoted;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.gridgain.internal.h2.index.Index;
import org.gridgain.internal.h2.table.Column;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;
import static org.apache.ignite.internal.processors.cache.AbstractDataTypesCoverageTest.ByteArrayed;
import static org.apache.ignite.internal.processors.cache.AbstractDataTypesCoverageTest.Dated;
import static org.apache.ignite.internal.processors.cache.AbstractDataTypesCoverageTest.SqlStrConvertedValHolder;
import static org.apache.ignite.internal.processors.cache.AbstractDataTypesCoverageTest.Timed;
import static org.apache.ignite.internal.processors.query.h2.H2TableDescriptor.PK_IDX_NAME;
import static org.apache.ignite.internal.processors.query.h2.database.H2Tree.IGNITE_THROTTLE_INLINE_SIZE_CALCULATION;
import static org.apache.ignite.internal.processors.query.h2.opt.H2TableScanIndex.SCAN_INDEX_NAME_SUFFIX;
import static org.apache.ignite.internal.util.IgniteUtils.MAX_INLINE_SIZE;

/**
 * A set of basic tests for caches with indexes.
 */
public class BasicIndexTest extends AbstractIndexingCommonTest {
    /** Default client name. */
    private static final String CLIENT_NAME = "client";

    /** {@code True} If index need to be created throught static config. */
    private static boolean createIdx = true;

    /** {@code True} If composite index required. */
    private static boolean createCompositeIdx;

    /** {@code True} If cache need to be created throught static config. */
    private static boolean createStaticCache = true;

    /** Default table name. */
    private static final String TEST_TBL_NAME = "PUBLIC.TEST_TABLE";

    /** */
    private Collection<QueryIndex> indexes = Collections.emptyList();

    /** */
    private Integer inlineSize;

    /** */
    private boolean isPersistenceEnabled;

    /** */
    private int gridCount = 1;

    /** Server listening logger. */
    private ListeningTestLogger srvLog;

    /** Client listening logger. */
    private ListeningTestLogger clientLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        assertNotNull(inlineSize);

        for (QueryIndex index : indexes)
            index.setInlineSize(inlineSize);

        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        igniteCfg.setConsistentId(igniteInstanceName);

        if (igniteInstanceName.startsWith(CLIENT_NAME)) {
            igniteCfg.setClientMode(true);

            if (clientLog != null)
                igniteCfg.setGridLogger(clientLog);
        }
        else {
            if (srvLog != null)
                igniteCfg.setGridLogger(srvLog);
        }

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("keyStr", String.class.getName());
        fields.put("keyLong", Long.class.getName());
        fields.put("keyPojo", Pojo.class.getName());
        fields.put("valStr", String.class.getName());
        fields.put("valLong", Long.class.getName());
        fields.put("valPojo", Pojo.class.getName());

        if (!createIdx)
            indexes = Collections.emptyList();

        CacheConfiguration<Key, Val> ccfg = new CacheConfiguration<Key, Val>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setQueryEntities(Collections.singleton(
                new QueryEntity()
                    .setKeyType(Key.class.getName())
                    .setValueType(Val.class.getName())
                    .setFields(fields)
                    .setKeyFields(new HashSet<>(Arrays.asList("keyStr", "keyLong", "keyPojo")))
                    .setIndexes(indexes)
                    .setAliases(Collections.singletonMap(QueryUtils.KEY_FIELD_NAME, "pk_id"))
            ))
            .setSqlIndexMaxInlineSize(inlineSize);

        if (createStaticCache)
            igniteCfg.setCacheConfiguration(ccfg);

        if (isPersistenceEnabled) {
            igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(10 * 1024 * 1024)
                )
            );
        }

        return igniteCfg
            .setFailureHandler(new StopNodeFailureHandler());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        srvLog = clientLog = null;

        createCompositeIdx = false;

        super.afterTest();
    }

    /**
     * @return Grid count used in test.
     */
    protected int gridCount() {
        return gridCount;
    }

    /** */
    @Test
    public void testNoIndexesNoPersistence() throws Exception {
        int[] inlineSizes = inlineSizeVariations();

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            stopAllGrids();
        }
    }

    /** */
    private int[] inlineSizeVariations() {
        int[] baseVariations = {0, 10, 20, 50, 100};

        // Determine if scaling is needed, we are not accurate here
        if (GridTestUtils.SF.apply(baseVariations.length) < baseVariations.length)
            return new int[] {0, 20, 100};

        return baseVariations;
    }

    /** */
    @Test
    public void testAllIndexesNoPersistence() throws Exception {
        indexes = Arrays.asList(
            new QueryIndex("keyStr"),
            new QueryIndex("keyLong"),
            new QueryIndex("keyPojo"),
            new QueryIndex("valStr"),
            new QueryIndex("valLong"),
            new QueryIndex("valPojo")
        );

        int[] inlineSizes = inlineSizeVariations();

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            stopAllGrids();
        }
    }

    /** */
    @Test
    public void testDynamicIndexesNoPersistence() throws Exception {
        int[] inlineSizes = inlineSizeVariations();

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            createDynamicIndexes(
                "keyStr",
                "keyLong",
                "keyPojo",
                "valStr",
                "valLong",
                "valPojo"
            );

            checkAll();

            stopAllGrids();
        }
    }

    /**
     * Checks that fields in primary index have correct order.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorrectPkFldsSequence() throws Exception {
        inlineSize = 10;

        IgniteEx ig0 = startGrid(0);

        GridQueryProcessor qryProc = ig0.context().query();

        IgniteH2Indexing idx = (IgniteH2Indexing)(ig0).context().query().getIndexing();

        String tblName = "T1";

        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE PUBLIC." + tblName + " (F1 VARCHAR, F2 VARCHAR, F3 VARCHAR, " +
            "CONSTRAINT PK PRIMARY KEY (F1, F2))"), true).getAll();

        List<String> expect = Arrays.asList("F1", "F2");

        checkPkFldSequence(tblName, expect, idx);

        tblName = "T2";

        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE PUBLIC." + tblName + " (F1 VARCHAR, F2 VARCHAR, F3 VARCHAR, " +
            "CONSTRAINT PK PRIMARY KEY (F2, F1))"), true).getAll();

        expect = Arrays.asList("F2", "F1");

        checkPkFldSequence(tblName, expect, idx);

        tblName = "T3";

        qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE PUBLIC." + tblName + " (F1 VARCHAR, F2 VARCHAR, F3 VARCHAR, " +
            "CONSTRAINT PK PRIMARY KEY (F3, F2))"), true).getAll();

        expect = Arrays.asList("F3", "F2");

        checkPkFldSequence(tblName, expect, idx);
    }

    /**
     * Checks that fields in primary index have correct order after node restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCorrectPkFldsSequenceAfterRestart() throws Exception {
        inlineSize = 10;

        IgniteEx ig0 = startGrid(getConfiguration("0").setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )
        ));

        ig0.cluster().state(ClusterState.ACTIVE);

        {
            GridQueryProcessor qryProc = ig0.context().query();

            IgniteH2Indexing idx = (IgniteH2Indexing)(ig0).context().query().getIndexing();

            String tblName = "T1";

            qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE PUBLIC." + tblName + " (F1 VARCHAR, F2 VARCHAR, F3 VARCHAR, " +
                "CONSTRAINT PK PRIMARY KEY (F2, F1))"), true).getAll();

            List<String> expect = Arrays.asList("F2", "F1");

            checkPkFldSequence(tblName, expect, idx);
        }

        stopAllGrids();

        ig0 = startGrid(getConfiguration("0").setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )
        ));

        ig0.cluster().state(ClusterState.ACTIVE);

        {
            IgniteH2Indexing idx = (IgniteH2Indexing)(ig0).context().query().getIndexing();

            String tblName = "T1";

            List<String> expect = Arrays.asList("F2", "F1");

            checkPkFldSequence(tblName, expect, idx);
        }
    }

    /**
     * Checks that fields in primary index have incorrect order
     * if grid has a node that doesn't support this feature.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPkFldsSequenceFeatureUnsupported() throws Exception {
        inlineSize = 10;

        List<IgniteEx> nodes = new ArrayList<>();

        nodes.add(startGrid(0));

        String prev = System.getProperty(SupportFeaturesUtils.IGNITE_SPECIFIED_SEQ_PK_KEYS_DISABLED);

        try {
            System.setProperty(SupportFeaturesUtils.IGNITE_SPECIFIED_SEQ_PK_KEYS_DISABLED, "true");

            nodes.add(startGrid(1));
        }
        finally {
            if (prev == null)
                System.clearProperty(SupportFeaturesUtils.IGNITE_SPECIFIED_SEQ_PK_KEYS_DISABLED);
            else
                System.setProperty(SupportFeaturesUtils.IGNITE_SPECIFIED_SEQ_PK_KEYS_DISABLED, prev);
        }

        int i = 0;
        for (IgniteEx ig : nodes) {
            GridQueryProcessor qryProc = ig.context().query();

            IgniteH2Indexing idx = (IgniteH2Indexing)(ig).context().query().getIndexing();

            String tblName = "T" + i++;

            qryProc.querySqlFields(new SqlFieldsQuery("CREATE TABLE PUBLIC." + tblName + " (F1 VARCHAR, F2 VARCHAR, F3 VARCHAR, " +
                "CONSTRAINT PK PRIMARY KEY (F2, F1))"), true).getAll();

            checkPkFldSequence(tblName, Arrays.asList("F1", "F2"), idx);
        }
    }

    /**
     * Fields correctness checker.
     *
     * @param tblName Table name.
     * @param expect Expected fields sequence.
     * @param idx Indexing.
     */
    private void checkPkFldSequence(String tblName, List<String> expect, IgniteH2Indexing idx) {
        Index pkIdx = idx.schemaManager().dataTable("PUBLIC", tblName.toUpperCase()).getIndex(PK_IDX_NAME);

        List<String> actual = Arrays.stream(pkIdx.getColumns()).map(Column::getName).collect(Collectors.toList());

        if (!expect.equals(actual))
            throw new AssertionError("Exp: " + expect + ", but was: " + actual);
    }

    /**
     * Tests mixed dynamic and static caches with indexes creation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicIdxOnStaticCacheWithIdxWithoutPersistence() throws Exception {
        runDynamicIdxOnStaticCacheWithIdx(false);
    }

    /**
     * Tests mixed dynamic and static caches with indexes creation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicIdxOnStaticCacheWithIdxWithPersistence() throws Exception {
        runDynamicIdxOnStaticCacheWithIdx(true);
    }

    /** */
    private void runDynamicIdxOnStaticCacheWithIdx(boolean persistEnabled) throws Exception {
        isPersistenceEnabled = persistEnabled;

        inlineSize = 10;

        createIdx = false;

        indexes = Collections.singletonList(new QueryIndex("valStr"));

        IgniteEx ig0 = startGrid(0);

        createIdx = true;

        startGrid(1);

        if (persistEnabled)
            ig0.cluster().active(true);

        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        populateCache();

        String plan = cache.query(new SqlFieldsQuery("explain select * from Val where valStr between 0 and ?")
            .setArgs(100)).getAll().get(0).get(0).toString();

        assertTrue(plan, plan.contains(SCAN_INDEX_NAME_SUFFIX));

        stopAllGrids();

        if (persistEnabled)
            cleanPersistenceDir();

        createStaticCache = false;

        ig0 = startGrid(0);

        if (persistEnabled)
            ig0.cluster().active(true);

        ig0.getOrCreateCache(DEFAULT_CACHE_NAME);

        populateCache();

        createStaticCache = true;

        try {
            startGrid(1);

            fail("Exception wasn't thrown");
        }
        catch (IgniteCheckedException e) {
            // no op.
        }
    }

    /**
     * Tests dynamic indexes creation with equal fields.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEqualFieldsDynamicIndexesWithoutPersistence() throws Exception {
        runEqualFieldsDynamicIndexes(false);
    }

    /**
     * Tests dynamic indexes creation with equal fields.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEqualFieldsDynamicIndexesWithPersistence() throws Exception {
        runEqualFieldsDynamicIndexes(true);
    }

    /** */
    private void runEqualFieldsDynamicIndexes(boolean persistEnabled) throws Exception {
        isPersistenceEnabled = persistEnabled;

        indexes = Collections.singletonList(new QueryIndex("valStr"));

        inlineSize = 10;

        srvLog = new ListeningTestLogger(false, log);

        clientLog = new ListeningTestLogger(false, log);

        String msg1 = "Index with the given set or subset of columns already exists";

        LogListener lsnr = LogListener.matches(msg1).andMatches(Pattern.compile(".*newIndexName=idx[0-9]")).build();

        LogListener staticCachesLsnr = LogListener.matches(msg1).build();

        srvLog.registerListener(staticCachesLsnr);

        IgniteEx ig0 = startGrid(0);

        if (persistEnabled)
            ig0.cluster().active(true);

        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        populateCache();

        cache.query(new SqlFieldsQuery("create index \"idx0\" on Val(valStr)"));

        assertTrue(staticCachesLsnr.check());

        srvLog.unregisterListener(staticCachesLsnr);

        srvLog.registerListener(lsnr);

        cache.query(new SqlFieldsQuery("create index \"idx1\" on Val(valStr, valLong)"));

        cache.query(new SqlFieldsQuery("create index \"idx2\" on Val(valStr desc, valLong)"));

        assertFalse(lsnr.check());

        cache.query(new SqlFieldsQuery("create index \"idx3\" on Val(valStr, valLong)"));

        cache.query(new SqlFieldsQuery("create index \"idx4\" on Val(valLong)"));

        String plan = cache.query(new SqlFieldsQuery("explain select min(_key), max(_key) from Val")).getAll()
            .get(0).get(0).toString().toUpperCase();

        assertTrue(plan, plan.contains(PK_IDX_NAME.toUpperCase()));

        assertTrue(lsnr.check());

        srvLog.unregisterListener(lsnr);

        IgniteEx client = startGrid(CLIENT_NAME);

        cache = client.cache(DEFAULT_CACHE_NAME);

        LogListener lsnrIdx5 = LogListener.matches(msg1).andMatches("newIndexName=idx5").build();

        srvLog.registerListener(lsnrIdx5);

        cache.query(new SqlFieldsQuery("create index \"idx5\" on Val(valStr desc, valLong)"));

        assertTrue(lsnrIdx5.check());

        LogListener lsnrIdx7 = LogListener.matches(msg1).andMatches("newIndexName=idx7").build();

        srvLog.registerListener(lsnrIdx7);

        cache.query(new SqlFieldsQuery("create index \"idx6\" on Val(valLong)"));

        cache.query(new SqlFieldsQuery("create index \"idx7\" on Val(keyStr, keyLong, keyPojo, valLong)"));

        assertFalse(lsnrIdx7.check());
    }

    /**
     * Check that specified index is used in query plan with specified where clause tables.
     *
     * @param qryProc Query processor to run queries on.
     * @param idxName Index name to test.
     * @param tblName Table to query.
     * @param reqFlds Fields to add into where clause.
     */
    private void checkIdxIsUsed(GridQueryProcessor qryProc, String idxName, String tblName, String... reqFlds) {
       checkIdxUsage(qryProc, idxName, null, tblName, reqFlds);
    }

    /**
     * Check that specified index is used in query plan with specified where clause tables.
     *
     * @param qryProc Query processor to run queries on.
     * @param idxName Index name to test.
     * @param tblName Table to query.
     * @param reqFlds Fields to add into where clause.
     */
    private void checkIdxIsNotUsed(GridQueryProcessor qryProc, String idxName, String tblName, String... reqFlds) {
        checkIdxUsage(qryProc, null, idxName, tblName, reqFlds);
    }

    /** */
    private void checkIdxUsage(GridQueryProcessor qryProc, String reqIdxName, String forbIdxname, String tblName, String... reqFlds) {
        String sql = "explain select * from " + tblName + " where ";

        for (int i = 0; i < reqFlds.length; ++i)
            sql += reqFlds[i] + " > 0 " + ((i < reqFlds.length - 1) ? " and " : "");

        String plan = qryProc.querySqlFields(new SqlFieldsQuery(sql), true)
                .getAll().get(0).get(0).toString().toUpperCase();

        if (reqIdxName != null)
            assertTrue(String.format("Plan \n%s\ndoesn't contain index %s", plan, reqIdxName),
                    plan.contains(reqIdxName.toUpperCase()));

        if (forbIdxname != null)
            assertTrue(String.format("Plan \n%s\ncontains index %s", plan, forbIdxname),
                    !plan.contains(forbIdxname.toUpperCase()));
    }

    /** */
    private boolean checkIdxAlreadyExistLog(GridQueryProcessor qryProc, String idxName, String tblName, String... reqFlds) {
        String msg0 = "Index with the given set or subset of columns already exists";

        String sql = "create index \"" + idxName + "\" on " + tblName + "(";

        for (int i = 0; i < reqFlds.length; ++i)
            sql += reqFlds[i] + ((i < reqFlds.length - 1) ? ", " : ")");

        LogListener lsnrIdx = LogListener.matches(msg0).andMatches(idxName).build();

        srvLog.registerListener(lsnrIdx);

        qryProc.querySqlFields(new SqlFieldsQuery(sql), true).getAll();

        srvLog.unregisterListener(lsnrIdx);

        return lsnrIdx.check();
    }

    /**
     * Create and fills the table.
     *
     * @param qryProc Query processor.
     * @param tblName Table name.
     * @param consPkFldsNum Number of fields to use in pk, 0 - default pk, positive\negative value shows fields iteration direction.
     */
    private void populateTable(GridQueryProcessor qryProc, String tblName, int consPkFldsNum, String... reqFlds) {
        assert consPkFldsNum <= reqFlds.length;

        String sql = "CREATE TABLE " + tblName + " (";

        String sqlIns = "INSERT INTO " + tblName + " (";

        for (int i = 0; i < reqFlds.length; ++i) {
            sql += reqFlds[i] + " VARCHAR" + (consPkFldsNum == 0 && i == 0 ? " PRIMARY KEY, " : ", ");

            sqlIns += reqFlds[i] + ((i < reqFlds.length - 1) ? ", " : ") values (");
        }

        if (consPkFldsNum != 0) {
            sql += " CONSTRAINT PK_PERSON PRIMARY KEY (";

            if (consPkFldsNum > 0) {
                for (int i = 0; i < consPkFldsNum; ++i)
                    sql += reqFlds[i] + ((i < consPkFldsNum - 1) ? ", " : "))");
            }
            else {
                for (int i = -consPkFldsNum - 1; i >= 0; --i)
                    sql += reqFlds[i] + ((i > 0) ? ", " : "))");
            }
        }
        else
            sql += ")";

        qryProc.querySqlFields(new SqlFieldsQuery(sql), true);

        for (int i = 0; i < 10; ++i) {
            String s0 = sqlIns;

            for (int f = 0; f < reqFlds.length; ++f)
                s0 += (i + f) + ((f < reqFlds.length - 1) ? ", " : ")");

            qryProc.querySqlFields(new SqlFieldsQuery(s0), true).getAll();
        }
    }

    /**
     * Checks index usage instead of full scan.
     * @param res Result set.
     * @return {@code True} if index usage found.
     */
    private boolean checkIdxUsage(List<List<?>> res, String idx) {
        String plan = res.get(0).get(0).toString();

        return idx != null ? plan.contains(idx) : !plan.contains(SCAN_INDEX_NAME_SUFFIX);
    }

    /**
     *  Checks index usage with correct pk fields enumeration.
     */
    @Test
    public void testCorrectFieldsSequenceInPk() throws Exception {
        inlineSize = 10;

        srvLog = new ListeningTestLogger(false, log);

        IgniteEx ig0 = startGrid(0);

        GridQueryProcessor qryProc = ig0.context().query();

        populateTable(qryProc, TEST_TBL_NAME, -2, "FIRST_NAME", "LAST_NAME",
            "ADDRESS", "LANG");

        assertFalse(checkIdxAlreadyExistLog(
            qryProc, "idx1", TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME"));

        assertTrue(checkIdxAlreadyExistLog(
            qryProc, "idx2", TEST_TBL_NAME, "LAST_NAME", "FIRST_NAME"));
    }

    /**
     *  Checks index usage for full coverage.
     */
    @Test
    public void testAllTableFieldsCoveredByIdx() throws Exception {
        inlineSize = 10;

        srvLog = new ListeningTestLogger(false, log);

        IgniteEx ig0 = startGrid(0);

        GridQueryProcessor qryProc = ig0.context().query();

        populateTable(qryProc, TEST_TBL_NAME, 2, "FIRST_NAME", "LAST_NAME",
            "ADDRESS", "LANG");

        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "LANG");
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "LAST_NAME");
        checkIdxIsUsed(qryProc, PK_IDX_NAME, TEST_TBL_NAME, "FIRST_NAME");
        checkIdxIsUsed(qryProc, PK_IDX_NAME, TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "LANG", "ADDRESS");

        assertTrue(checkIdxAlreadyExistLog(
            qryProc, "idx1", TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME"));

        String sqlIdx2 = String.format("create index \"idx2\" on %s(LANG, ADDRESS)", TEST_TBL_NAME);

        qryProc.querySqlFields(new SqlFieldsQuery(sqlIdx2), true).getAll();

        checkIdxIsUsed(qryProc, "idx2", TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "LANG", "ADDRESS");

        checkIdxIsUsed(qryProc, null, TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "ADDRESS", "LANG");
    }

    /**
     *  Checks index usage for full coverage.
     */
    @Test
    public void testConditionsWithoutIndexes() throws Exception {
        inlineSize = 10;

        srvLog = new ListeningTestLogger(false, log);

        IgniteEx ig0 = startGrid(0);

        GridQueryProcessor qryProc = ig0.context().query();

        populateTable(qryProc, TEST_TBL_NAME, 2, "FIRST_NAME", "LAST_NAME", "ADDRESS", "LANG");

        String sqlIdx = String.format("create index \"idx1\" on %s(LANG, ADDRESS)", TEST_TBL_NAME);

        qryProc.querySqlFields(new SqlFieldsQuery(sqlIdx), true).getAll();

        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "LAST_NAME");
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "ADDRESS");
        checkIdxIsUsed(qryProc,"idx1", TEST_TBL_NAME, "LANG");

        // first idx fields not belongs to request fields.
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "ADDRESS", "LAST_NAME");
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "ADDRESS", "ADDRESS");
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "LAST_NAME", "ADDRESS");
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "ADDRESS");
    }

    /**
     *  Checks index usage for partial coverage.
     *  Last field not participate in any index.
     */
    @Test
    public void testPartialTableFieldsCoveredByIdx() throws Exception {
        inlineSize = 10;

        srvLog = new ListeningTestLogger(false, log);

        IgniteEx ig0 = startGrid(0);

        GridQueryProcessor qryProc = ig0.context().query();

        populateTable(qryProc, TEST_TBL_NAME, 2, "FIRST_NAME", "LAST_NAME",
            "ADDRESS", "LANG", "GENDER");

        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "LANG");
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "LAST_NAME");
        checkIdxIsUsed(qryProc, PK_IDX_NAME, TEST_TBL_NAME, "FIRST_NAME");
        checkIdxIsUsed(qryProc, PK_IDX_NAME, TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "LANG", "ADDRESS");

        assertTrue(checkIdxAlreadyExistLog(qryProc, "idx1", TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME"));

        String sqlIdx2 = String.format("create index \"idx2\" on %s(LANG, ADDRESS)", TEST_TBL_NAME);

        qryProc.querySqlFields(new SqlFieldsQuery(sqlIdx2), true).getAll();

        // PK_IDX_NAME used.
        checkIdxIsUsed(qryProc, PK_IDX_NAME, TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "LANG", "ADDRESS");
        checkIdxIsUsed(qryProc, PK_IDX_NAME, TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "ADDRESS", "LANG");

        // first idx fields not belongs to request fields.
        checkIdxIsNotUsed(qryProc, "idx2", TEST_TBL_NAME, "ADDRESS", "LAST_NAME");

        assertFalse(checkIdxAlreadyExistLog(qryProc, "idx3", TEST_TBL_NAME, "ADDRESS", "LANG"));

        assertTrue(checkIdxAlreadyExistLog(
                qryProc, "idx4", TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "ADDRESS", "LANG"));

        assertTrue(checkIdxAlreadyExistLog(qryProc, "idx5", TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "LANG", "ADDRESS"));

        String msg0 = "Index with the given set or subset of columns already exists";
        LogListener lsnrIdx4 = LogListener.matches(msg0).andMatches(PK_IDX_NAME).build();

        srvLog.registerListener(lsnrIdx4);

        String sqlIdx5 = String.format("create index \"idx6\" on %s(FIRST_NAME, LAST_NAME, LANG, ADDRESS)", TEST_TBL_NAME);
        String cacheName = QueryUtils.createTableCacheName("PUBLIC", "TEST_TABLE");
        IgniteCache<Object, Object> jcache = ig0.cache(cacheName);
        jcache.query(new SqlFieldsQuery(sqlIdx5)).getAll();

        assertTrue(lsnrIdx4.check());
    }

    /**
     * Check three fields in pk index.
     */
    @Test
    public void testCheckThreeFieldsInPk() throws Exception {
        inlineSize = 10;

        srvLog = new ListeningTestLogger(log);

        IgniteEx ig0 = startGrid(0);

        GridQueryProcessor qryProc = ig0.context().query();

        populateTable(qryProc, TEST_TBL_NAME, 3, "c1", "c2", "c3", "c4", "c5", "c6");

        checkIdxIsUsed(qryProc, PK_IDX_NAME, TEST_TBL_NAME, "c1");
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "c2");
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "c3");
    }

    /**
     * Test composite indices with PK field in first place.
     *
     * There is no sense to create such indices:
     * 1. PK index will be enough for equality condition on PK field.
     * 2. None of these indices will be used for non-equality condition on PK field.
     */
    @Test
    public void testCreateIdxWithDifferentIdxFldsSeq() throws Exception {
        inlineSize = 10;

        srvLog = new ListeningTestLogger(false, log);

        IgniteEx ig0 = startGrid(0);

        IgniteEx client = startGrid(CLIENT_NAME);

        GridQueryProcessor qryProc = ig0.context().query();

        populateTable(qryProc, TEST_TBL_NAME, 1, "c1", "c2", "c3", "c4", "c5");

        assertFalse(checkIdxAlreadyExistLog(
            qryProc, "idx1", TEST_TBL_NAME, "c1", "c2", "c3", "c4", "c5"));

        assertFalse(checkIdxAlreadyExistLog(
            qryProc, "idx2", TEST_TBL_NAME, "c1", "c3", "c4", "c5"));

        assertTrue(checkIdxAlreadyExistLog(
            qryProc, "idx3", TEST_TBL_NAME, "c1", "c2"));

        assertTrue(checkIdxAlreadyExistLog(
            qryProc, "idx4", TEST_TBL_NAME, "c1", "c3"));

        assertFalse(checkIdxAlreadyExistLog(
            qryProc, "idx5", TEST_TBL_NAME, "c1", "c4", "c5"));

        GridQueryProcessor qryProcCl = client.context().query();

        assertTrue(checkIdxAlreadyExistLog(
            qryProcCl, "idx6", TEST_TBL_NAME, "c1", "c2"));
    }

    /**
     * Tests IN with EQUALS index usage .
     */
    @Test
    public void testInWithEqualsIdxUsage() throws Exception {
        inlineSize = 10;

        isPersistenceEnabled = false;

        IgniteEx ignite = startGrid(0);

        GridQueryProcessor qryProc = ignite.context().query();

        checkInWithEqualsIdxUsageForDifferentTypes(qryProc);

        // Same checks for composite index.
        createCompositeIdx = true;
        checkInWithEqualsIdxUsageForDifferentTypes(qryProc);
    }

    private void checkInWithEqualsIdxUsageForDifferentTypes(final GridQueryProcessor qryProc) {
        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.INT,
            "val * 3",
            1, 2, -3, null);

        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.BIGINT,
            null,
            0L, Long.MAX_VALUE, Long.MIN_VALUE, null);

        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.VARCHAR,
            "_val",
            new Quoted(""),
            new Quoted("whatever"),
            new Quoted("CamelCase"),
            null);

        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.DATE,
            "_key",
            new Dated(Date.valueOf("2001-09-11")),
            new Dated(Date.valueOf("1806-08-12")),
            new Dated(Date.valueOf("2051-11-21")),
            null);

        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.TIME,
            "fld, val",
            new Timed(Time.valueOf("00:00:01")),
            new Timed(Time.valueOf("12:00:00")),
            new Timed(Time.valueOf("23:59:59")),
            null);

        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.TIMESTAMP,
            "val, fld",
            new Dated(Timestamp.valueOf("2019-01-01 00:00:00")),
            new Dated(Timestamp.valueOf("2051-12-31 23:59:59")),
            new Dated(Timestamp.valueOf("1806-08-12 12:00:00")),
            null);

        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.DOUBLE,
            "*, _key",
            -1.0d, 1e-7d, new Quoted(Double.NEGATIVE_INFINITY), null);

        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.UUID,
            "val",
            new Quoted("d9bc480e-1107-11ea-8d71-362b9e155667"),
            new Quoted(UUID.fromString("d9bc4354-1107-11ea-8d71-362b9e155667")),
            new Quoted(UUID.randomUUID()),
            null);

        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.DECIMAL,
            "ROUND(val + 0.05, 1)",
            "10.2",
            new BigDecimal("10.01"),
            new BigDecimal(123.123),
            null);

        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.BINARY,
            "BIT_LENGTH(val)",
            new ByteArrayed(new byte[] {}),
            new ByteArrayed(new byte[] {0, 1}),
            new ByteArrayed(new byte[] {1, 2, 3}),
            null);

        checkInWithEqualsIdxUsageForType(qryProc, SqlDataType.OTHER,
            null,
            new UserObject(new Pojo(1)),
            new UserObject(new Pojo(2)),
            new UserObject(new Pojo(-3)),
            null);
    }

    /**
     * Tests that it's forbidden to create index with duplicated columns from SQL.
     */
    @Test
    public void testFailToCreateSqlIndexWithDuplicatedColumn() throws Exception {
        inlineSize = 10;

        IgniteEx ig0 = startGrid(0);

        GridQueryProcessor qryProc = ig0.context().query();

        populateTable(qryProc, TEST_TBL_NAME, 1, "ID", "NAME");

        GridTestUtils.assertThrows(log, () -> {
            String sqlIdx1 = String.format("create index \"idx1\" on %s(NAME, NAME)", TEST_TBL_NAME);

            qryProc.querySqlFields(new SqlFieldsQuery(sqlIdx1), true).getAll();

            return null;
        }, IgniteSQLException.class, "Already defined column in index: NAME ASC");

        GridTestUtils.assertThrows(log, () -> {
            String sqlIdx1 = String.format("create index \"idx1\" on %s(NAME ASC, NAME DESC)", TEST_TBL_NAME);

            qryProc.querySqlFields(new SqlFieldsQuery(sqlIdx1), true).getAll();

            return null;
        }, IgniteSQLException.class, "Already defined column in index: NAME ASC");

        GridTestUtils.assertThrows(log, () -> {
            String sqlIdx1 = String.format("create index \"idx1\" on %s(NAME DESC, ID, NAME)", TEST_TBL_NAME);

            qryProc.querySqlFields(new SqlFieldsQuery(sqlIdx1), true).getAll();

            return null;
        }, IgniteSQLException.class, "Already defined column in index: NAME DESC");

        GridTestUtils.assertThrows(log, () -> {
            String sqlIdx1 = String.format("create index \"idx1\" on %s(ID, id)", TEST_TBL_NAME);

            qryProc.querySqlFields(new SqlFieldsQuery(sqlIdx1), true).getAll();

            return null;
        }, IgniteSQLException.class, "Already defined column in index: ID");
    }

    /**
     * Check IDX usage for different cases.
     *
     * @param qryProc Query processor.
     * @param dataType SQL data type.
     * @param proj Projection.
     * @param values Values to put.
     */
    private void checkInWithEqualsIdxUsageForType(
        final GridQueryProcessor qryProc,
        SqlDataType dataType,
        @Nullable String proj,
        @NotNull Object... values) {
        assert values.length >= 4;

        if (proj == null)
            proj = "*";

        qryProc.querySqlFields(new SqlFieldsQuery(
            "CREATE TABLE " + TEST_TBL_NAME +
                "(id LONG PRIMARY KEY," +
                " fld " + dataType + ", " +
                " val " + dataType + ")"), true);

        final String idxName = "IDX_VAL";

        qryProc.querySqlFields(new SqlFieldsQuery(
            "CREATE INDEX \"" + idxName + "\" ON " + TEST_TBL_NAME +
                (createCompositeIdx ? "(val, fld)" : "(val)")), true);

        for (int i = 0; i < values.length; i++) {
            Object valToPut = toObjVal(values[i]);

            // INSERT
            qryProc.querySqlFields(new SqlFieldsQuery("INSERT INTO " + TEST_TBL_NAME +
                "(id, fld, val) VALUES (?1, ?2, ?3)").setArgs(i, valToPut, valToPut), true);
        }

        try {
            final int rnd = ThreadLocalRandom.current().nextInt(values.length);
            final Object val1 = values[(rnd) % values.length];
            final Object val2 = values[(rnd + 1) % values.length];
            final Object val3 = values[(rnd + 2) % values.length];
            final Object val4 = values[(rnd + 3) % values.length];

            final String qry = "select " + proj + " from " + TEST_TBL_NAME + " ";

            {
                final String sql = qry + "where val in (?1, ?2) and (fld = ?1 or fld = ?2)";

                List<List<?>> res = qryProc.querySqlFields(new SqlFieldsQuery("explain " + sql)
                    .setArgs(toObjVal(val1), toObjVal(val2)), true).getAll();

                assertTrue(checkIdxUsage(res, idxName));

                qryProc.querySqlFields(new SqlFieldsQuery(sql)
                    .setArgs(toObjVal(val1), toObjVal(val2)), true).getAll();
            }

            {
                final String sql = qry + "where val in (select fld from " +
                    TEST_TBL_NAME + " where fld in(?1, ?2)) and (fld = ?1 or fld = ?2)";

                List<List<?>> res = qryProc.querySqlFields(new SqlFieldsQuery("explain " + sql)
                    .setArgs(toObjVal(val1), toObjVal(val2)), true).getAll();

                assertTrue(checkIdxUsage(res, idxName));

                qryProc.querySqlFields(new SqlFieldsQuery(sql)
                    .setArgs(toObjVal(val1), toObjVal(val2)), true).getAll();
            }

            {
                final String sql = qry + "where val in (?1, ?2) and (fld = ?3 or fld = ?3)";

                List<List<?>> res = qryProc.querySqlFields(new SqlFieldsQuery("explain " + sql)
                    .setArgs(toObjVal(val1), toObjVal(val2), toObjVal(val3)), true).getAll();

                assertTrue(checkIdxUsage(res, idxName));

                qryProc.querySqlFields(new SqlFieldsQuery(sql)
                    .setArgs(toObjVal(val1), toObjVal(val2), toObjVal(val3)), true).getAll();
            }

            {
                final String sql = qry + "where val in (?1, ?2) and fld = ?3";

                List<List<?>> res = qryProc.querySqlFields(new SqlFieldsQuery("explain " + sql)
                    .setArgs(toObjVal(val1), toObjVal(val2), toObjVal(val3)), true).getAll();

                assertTrue(checkIdxUsage(res, idxName));

                qryProc.querySqlFields(new SqlFieldsQuery(sql)
                    .setArgs(toObjVal(val1), toObjVal(val2), toObjVal(val3)), true).getAll();
            }

            {
                final String sql = qry + "where val in (" + toStringVal(val1) + ", "
                    + toStringVal(val2) + ") and " + "fld = " + toStringVal(val3);

                List<List<?>> res = qryProc.querySqlFields(
                    new SqlFieldsQuery("explain " + sql),true).getAll();

                assertTrue(checkIdxUsage(res, idxName));

                qryProc.querySqlFields(new SqlFieldsQuery(sql), true).getAll();
            }

            { //Check OR -> IN optimization is applied.
                final String sql = qry + "where (val = ?1 OR val = ?2) and fld = " +
                    toStringVal(val3);

                List<List<?>> res = qryProc.querySqlFields(new SqlFieldsQuery("explain " + sql)
                    .setArgs(toObjVal(val1), toObjVal(val2)), true).getAll();

                assertTrue(checkIdxUsage(res, idxName));

                qryProc.querySqlFields(new SqlFieldsQuery(sql)
                    .setArgs(toObjVal(val1), toObjVal(val2)), true).getAll();
            }

            {
                final String sql = qry + "where val in (?1, ?2) and " +
                    "(fld = ?3 or fld = ?4) ORDER BY fld";

                List<List<?>> res = qryProc.querySqlFields(new SqlFieldsQuery("explain " + sql)
                    .setArgs(toObjVal(val1), toObjVal(val2), toObjVal(val3),
                        toObjVal(val4)), true).getAll();

                assertTrue(checkIdxUsage(res, idxName));

                qryProc.querySqlFields(new SqlFieldsQuery(sql)
                    .setArgs(toObjVal(val1), toObjVal(val2), toObjVal(val3),
                        toObjVal(val4)), true).getAll();
            }

            {
                final String sql = qry + "where val in (?1, ?2) and (fld = ?3 or fld = ?1) ORDER BY fld";

                List<List<?>> res = qryProc.querySqlFields(new SqlFieldsQuery("explain " + sql)
                    .setArgs(toObjVal(val1), toObjVal(val2), toObjVal(val3)), true).getAll();

                assertTrue(checkIdxUsage(res, idxName));

                qryProc.querySqlFields(new SqlFieldsQuery(sql)
                    .setArgs(toObjVal(val1), toObjVal(val2), toObjVal(val3)), true).getAll();
            }

            {
                final String sql = qry + "where val in (?1, ?2) and (fld = ?1 or fld = ?2) ORDER BY fld";

                List<List<?>> res = qryProc.querySqlFields(new SqlFieldsQuery("explain " + sql)
                    .setArgs(toObjVal(val1), toObjVal(val2)), true).getAll();

                assertTrue(checkIdxUsage(res, idxName));

                qryProc.querySqlFields(new SqlFieldsQuery(sql)
                    .setArgs(toObjVal(val1), toObjVal(val2)), true).getAll();
            }

            {
                final String sql = qry + "where val in (" + toStringVal(val2) + ", " +
                    toStringVal(val1) + ") and (fld = " + toStringVal(val1) +
                    " or fld = " + toStringVal(val2) + ") ORDER BY fld";

                List<List<?>> res = qryProc.querySqlFields(
                    new SqlFieldsQuery("explain " + sql), true).getAll();

                assertTrue(checkIdxUsage(res, idxName));

                qryProc.querySqlFields(new SqlFieldsQuery(sql), true).getAll();
            }
        }
        finally {
            qryProc.querySqlFields(new SqlFieldsQuery("DROP TABLE " + TEST_TBL_NAME + ";"), true);
        }
    }

    /**
     * Convert to string value for SQL injection if needed.
     * @param val Value.
     * @return Value to inject to SQL query.
     */
    private <T> String toStringVal(T val) {
        return val instanceof SqlStrConvertedValHolder ?
            ((SqlStrConvertedValHolder)val).sqlStrVal() :
            String.valueOf(val);
    }

    /**
     * Convert to object value if needed.
     * @param val Value.
     * @return Value for usage as prepared statement param.
     */
    private <T> Object toObjVal(T val) {
        return val instanceof SqlStrConvertedValHolder ?
            ((SqlStrConvertedValHolder)val).originalVal() :
            val;
    }

    /**
     * Tests different fields sequence in indexes.
     * Last field not participate in any index.
     */
    @Test
    public void testIndexWithDifferentFldsReqPartialFldsInIdx() throws Exception {
        inlineSize = 10;

        IgniteEx ig0 = startGrid(0);

        GridQueryProcessor qryProc = ig0.context().query();

        populateTable(qryProc, TEST_TBL_NAME, 2, "FIRST_NAME", "LAST_NAME",
            "ADDRESS", "LANG", "GENDER");

        String sqlIdx1 = String.format("create index \"idx1\" on %s(LANG, LAST_NAME, ADDRESS, FIRST_NAME)", TEST_TBL_NAME);

        qryProc.querySqlFields(new SqlFieldsQuery(sqlIdx1), true).getAll();

        checkIdxIsUsed(qryProc, PK_IDX_NAME, TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "LANG");
        checkIdxIsUsed(qryProc, PK_IDX_NAME, TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "ADDRESS");
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "LAST_NAME", "ADDRESS");
    }

    /**
     * Tests different fields sequence in indexes.
     * All fields covered by indexes.
     */
    @Test
    public void testIndexWithDifferentFldsReqAllFldsInIdx() throws Exception {
        inlineSize = 10;

        IgniteEx ig0 = startGrid(0);

        GridQueryProcessor qryProc = ig0.context().query();

        populateTable(qryProc, TEST_TBL_NAME, 2, "FIRST_NAME", "LAST_NAME",
            "ADDRESS", "LANG");

        String sqlIdx1 = String.format("create index \"idx1\" on %s(LANG, LAST_NAME, ADDRESS, FIRST_NAME)", TEST_TBL_NAME);

        qryProc.querySqlFields(new SqlFieldsQuery(sqlIdx1), true).getAll();

        checkIdxIsUsed(qryProc, "idx1", TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "LANG");
        checkIdxIsUsed(qryProc, PK_IDX_NAME, TEST_TBL_NAME, "FIRST_NAME", "LAST_NAME", "ADDRESS");
        checkIdxIsUsed(qryProc, SCAN_INDEX_NAME_SUFFIX, TEST_TBL_NAME, "LAST_NAME", "ADDRESS");
    }

    /** */
    @Test
    public void testNoIndexesWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        int[] inlineSizes = inlineSizeVariations();

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            stopAllGrids();

            startGridsMultiThreaded(gridCount());

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    @Test
    public void testAllIndexesWithPersistence() throws Exception {
        indexes = Arrays.asList(
            new QueryIndex("keyStr"),
            new QueryIndex("keyLong"),
            new QueryIndex("keyPojo"),
            new QueryIndex("valStr"),
            new QueryIndex("valLong"),
            new QueryIndex("valPojo")
        );

        isPersistenceEnabled = true;

        int[] inlineSizes = inlineSizeVariations();

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            stopAllGrids();

            startGridsMultiThreaded(gridCount());

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_THROTTLE_INLINE_SIZE_CALCULATION, value = "1")
    public void testInlineSizeChange() throws Exception {
        isPersistenceEnabled = true;

        indexes = Collections.singletonList(new QueryIndex("valStr"));

        inlineSize = 33;

        srvLog = new ListeningTestLogger(false, log);

        String msg1 = "curSize=1";
        String msg2 = "curSize=2";
        String msg3 = "curSize=3";

        LogListener lstn1 = LogListener.matches(msg1).build();
        LogListener lstn2 = LogListener.matches(msg2).build();
        LogListener lstn3 = LogListener.matches(msg3).build();

        srvLog.registerListener(lstn1);
        srvLog.registerListener(lstn2);
        srvLog.registerListener(lstn3);

        IgniteEx ig0 = startGrid(0);

        ig0.cluster().active(true);

        populateCache();

        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        execSql(cache, "create index \"idx1\" on Val(valLong) INLINE_SIZE 1 PARALLEL 28");

        List<List<?>> res = execSql(cache, "explain select * from Val where valLong > ?", 10);

        assertTrue(((String)res.get(0).get(0)).toUpperCase().contains((DEFAULT_CACHE_NAME + ".idx1").toUpperCase()));

        assertTrue(lstn1.check());

        execSql(cache, "drop index \"idx1\"");
        execSql(cache, "create index \"idx1\" on Val(valLong) INLINE_SIZE 2 PARALLEL 28");
        execSql(cache, "explain select * from Val where valLong > ?", 10);

        assertTrue(lstn2.check());

        execSql(cache, "drop index \"idx1\"");

        stopAllGrids();

        ig0 = startGrid(0);

        ig0.cluster().active(true);

        cache = ig0.cache(DEFAULT_CACHE_NAME);

        execSql(cache, "create index \"idx1\" on Val(valLong) INLINE_SIZE 3 PARALLEL 28");
        execSql(cache, "explain select * from Val where valLong > ?", 10);

        assertTrue(lstn3.check());
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     * @param args Args.
     */
    private List<List<?>> execSql(IgniteCache<?, ?> cache, String qry, Object... args) {
        return cache.query(new SqlFieldsQuery(qry).setArgs(args)).getAll();
    }

    /** */
    @Test
    public void testDynamicIndexesWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        int[] inlineSizes = inlineSizeVariations();

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            createDynamicIndexes(
                "keyStr",
                "keyLong",
                "keyPojo",
                "valStr",
                "valLong",
                "valPojo"
            );

            checkAll();

            stopAllGrids();

            startGridsMultiThreaded(gridCount());

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    @Test
    public void testDynamicIndexesDropWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        int[] inlineSizes = inlineSizeVariations();

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            String[] cols = {
                "keyStr",
                "keyLong",
                "keyPojo",
                "valStr",
                "valLong",
                "valPojo"
            };

            createDynamicIndexes(cols);

            checkAll();

            dropDynamicIndexes(cols);

            checkAll();

            stopAllGrids();

            startGridsMultiThreaded(gridCount());

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    @Test
    public void testNoIndexesWithPersistenceIndexRebuild() throws Exception {
        isPersistenceEnabled = true;

        int[] inlineSizes = inlineSizeVariations();

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            List<Path> idxPaths = getIndexBinPaths(DEFAULT_CACHE_NAME);

            // Shutdown gracefully to ensure there is a checkpoint with index.bin.
            // Otherwise index.bin rebuilding may not work.
            grid(0).cluster().active(false);

            stopAllGrids();

            idxPaths.forEach(idxPath -> assertTrue(U.delete(idxPath)));

            startGridsMultiThreaded(gridCount());

            grid(0).cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    @Test
    public void testAllIndexesWithPersistenceIndexRebuild() throws Exception {
        indexes = Arrays.asList(
            new QueryIndex("keyStr"),
            new QueryIndex("keyLong"),
            new QueryIndex("keyPojo"),
            new QueryIndex("valStr"),
            new QueryIndex("valLong"),
            new QueryIndex("valPojo")
        );

        isPersistenceEnabled = true;

        int[] inlineSizes = inlineSizeVariations();

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            List<Path> idxPaths = getIndexBinPaths(DEFAULT_CACHE_NAME);

            // Shutdown gracefully to ensure there is a checkpoint with index.bin.
            // Otherwise index.bin rebuilding may not work.
            grid(0).cluster().active(false);

            stopAllGrids();

            idxPaths.forEach(idxPath -> assertTrue(U.delete(idxPath)));

            startGridsMultiThreaded(gridCount());

            grid(0).cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    @Test
    public void testDynamicIndexesWithPersistenceIndexRebuild() throws Exception {
        isPersistenceEnabled = true;

        int[] inlineSizes = inlineSizeVariations();

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            createDynamicIndexes(
                "keyStr",
                "keyLong",
                "keyPojo",
                "valStr",
                "valLong",
                "valPojo"
            );

            checkAll();

            List<Path> idxPaths = getIndexBinPaths(DEFAULT_CACHE_NAME);

            // Shutdown gracefully to ensure there is a checkpoint with index.bin.
            // Otherwise index.bin rebuilding may not work.
            grid(0).cluster().active(false);

            stopAllGrids();

            idxPaths.forEach(idxPath -> assertTrue(U.delete(idxPath)));

            startGridsMultiThreaded(gridCount());

            grid(0).cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIndexSelectionIsNotAffectedByCreationOrderScenario1() throws Exception {
        checkRightIndexChosen("IDX1", "IDX2",
            "CREATE INDEX IDX1  on TEST_TBL_NAME (FIRST_NAME, LAST_NAME)",
            "CREATE INDEX IDX2 on TEST_TBL_NAME (LAST_NAME, FIRST_NAME)",
            "SELECT * FROM TEST_TBL_NAME WHERE LAST_NAME = 2 and FIRST_NAME >= 1 and FIRST_NAME <= 5 and ADDRESS in (1, 2, 3);");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIndexSelectionIsNotAffectedByCreationOrderScenario2() throws Exception {
        checkRightIndexChosen("IDX1", "IDX2",
            "CREATE INDEX IDX1 on TEST_TBL_NAME (LAST_NAME, FIRST_NAME)",
            "CREATE INDEX IDX2 on TEST_TBL_NAME (FIRST_NAME, LAST_NAME)",
            "SELECT * FROM TEST_TBL_NAME WHERE LAST_NAME >= 2 and FIRST_NAME = 1 and FIRST_NAME <= 5 and ADDRESS in (1, 2, 3)");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIndexSelectionIsNotAffectedByCreationOrderScenario3() throws Exception {
        checkRightIndexChosen("IDX1", "IDX2",
            "CREATE INDEX IDX1 on TEST_TBL_NAME (FIRST_NAME)",
            "CREATE INDEX IDX2 on TEST_TBL_NAME (LAST_NAME, FIRST_NAME)",
            "SELECT * FROM TEST_TBL_NAME WHERE LAST_NAME = 2 and FIRST_NAME >= 1 and FIRST_NAME <= 5 and ADDRESS in (1, 2, 3)");
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-25338")
    @Test
    public void testIndexSelectionIsNotAffectedByCreationOrderScenario4() throws Exception {
        checkRightIndexChosen("IDX1", "IDX2",
            "CREATE INDEX IDX1 on TEST_TBL_NAME (LAST_NAME, ADDRESS)",
            "CREATE INDEX IDX2 on TEST_TBL_NAME (LAST_NAME, FIRST_NAME)",
            "SELECT * FROM TEST_TBL_NAME WHERE LAST_NAME = 2 and FIRST_NAME >= 1 and FIRST_NAME <= 5 and ADDRESS in (1, 2, 3)");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIndexSelectionIsNotAffectedByCreationOrderScenario5() throws Exception {
        checkRightIndexChosen("IDX1", "IDX2",
            "CREATE INDEX IDX1 on TEST_TBL_NAME (LAST_NAME, FIRST_NAME)",
            "CREATE INDEX IDX2 on TEST_TBL_NAME (LAST_NAME, ADDRESS)",
            "SELECT * FROM TEST_TBL_NAME WHERE LAST_NAME = 2 and ADDRESS >= 1");
    }

    /**
     */
    @Test
    public void testStopNodeOnSqlQueryWithIncompatibleType() throws Exception {
        inlineSize = 10;

        startGrid();

        sql("CREATE TABLE TEST (ID INT PRIMARY KEY, val_int INT, VAL_OBJ OTHER)");
        sql("CREATE INDEX TEST_VAL_INT ON TEST(VAL_INT)");
        sql("CREATE INDEX TEST_VAL_OBJ ON TEST(VAL_OBJ)");

        sql("INSERT INTO TEST VALUES (0, 0, ?)", Instant.now());

        GridTestUtils.assertThrows(log, () -> {
            sql("SELECT * FROM TEST WHERE VAL_OBJ < CURRENT_TIMESTAMP()").getAll();

            return null;
        }, CacheException.class, null);

        GridTestUtils.assertThrows(log, () -> {
            sql("SELECT * FROM TEST WHERE VAL_INT < CURRENT_TIMESTAMP()").getAll();

            return null;
        }, CacheException.class, null);

        assertFalse(grid().context().isStopping());
    }

    /** */
    @Test
    public void testOpenRangePredicateOnCompoundPk() throws Exception {
        inlineSize = 10;

        startGrid();

        sql("create table test (id1 int, id2 int, val int, constraint pk primary key (id1, id2))");

        for (int i = 1; i <= 5; i++)
            sql("insert into test (id1, id2, val) values (?, ?, ?)", 0, i, i);

        assertEquals(5, sql("select * from test where id1 = 0 and id2 > 0").getAll().size());
    }

    /**
     * Checks index creation does not affect used index.
     *
     * @param wrongIdx1Name Wrong index name.
     * @param properIdx2Name Proper index name.
     * @param idx1Cmd Index 1 (wrong) creation command.
     * @param idx2Cmd Index 1 (proper) creation command.
     * @param qry Query to test index selection.
     * @throws Exception If failed.
     */
    private void checkRightIndexChosen(String wrongIdx1Name,
        String properIdx2Name,
        String idx1Cmd,
        String idx2Cmd,
        String qry)
        throws Exception {
        inlineSize = 10;

        wrongIdx1Name = wrongIdx1Name.toUpperCase();
        properIdx2Name = properIdx2Name.toUpperCase();

        // Check preconditions are correct.
        assertTrue(idx1Cmd, idx1Cmd.toUpperCase().contains(wrongIdx1Name));
        assertTrue(idx2Cmd, idx2Cmd.toUpperCase().contains(properIdx2Name));

        // Empty grid is started.
        IgniteEx ig0 = startGrids(gridCount());

        GridQueryProcessor qryProc = ig0.context().query();

        // SQL Table with fields FIRST_NAME, LAST_NAME, ADDRESS, LANG is created.
        // Some initial data is populated to the table.
        populateTable(qryProc, "TEST_TBL_NAME", 1, "ID","FIRST_NAME", "LAST_NAME",
            "ADDRESS", "LANG");

        // Create index idx2.
        qryProc.querySqlFields(new SqlFieldsQuery(idx2Cmd), true).getAll();

        // Create index idx1.
        qryProc.querySqlFields(new SqlFieldsQuery(idx1Cmd), true).getAll();

        // Execute EXPLAIN SELECT sql.
        String plan = qryProc.querySqlFields(new SqlFieldsQuery("explain " + qry), true)
            .getAll().get(0).get(0).toString().toUpperCase();

        // Ensure proper idx2 is used in execution plan.
        assertTrue("plan=" + plan, plan.contains(properIdx2Name));
        assertFalse("plan=" + plan, plan.contains(wrongIdx1Name));

        // Drop indices idx1 and idx2.
        qryProc.querySqlFields(new SqlFieldsQuery("DROP INDEX " + wrongIdx1Name), true).getAll();
        qryProc.querySqlFields(new SqlFieldsQuery("DROP INDEX " + properIdx2Name), true).getAll();

        // Create index idx1.
        qryProc.querySqlFields(new SqlFieldsQuery(idx1Cmd), true).getAll();

        // Create index idx2.
        qryProc.querySqlFields(new SqlFieldsQuery(idx2Cmd), true).getAll();

        // Execute EXPLAIN SELECT sql.
        plan = qryProc.querySqlFields(new SqlFieldsQuery("explain " + qry), true)
            .getAll().get(0).get(0).toString().toUpperCase();

        // Ensure proper idx2 is used in execution plan.
        assertTrue("plan=" + plan, plan.contains(properIdx2Name));
        assertFalse("plan=" + plan, plan.contains(wrongIdx1Name));

        // Tables and indices are dropped.
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testCreateLuceneIndex() throws Exception {
        inlineSize = 10;

        startGrid();

        sql("create table test0(id1 int primary key, val varchar) " +
            "WITH \"WRAP_VALUE=false\"");

        IgniteH2Indexing idx = ((IgniteH2Indexing)grid().context().query().getIndexing());

        H2TableDescriptor tblDesc0 = idx.schemaManager().dataTable("PUBLIC", "TEST0")
            .rowDescriptor().tableDescriptor();

        assertNotNull(GridTestUtils.getFieldValue(tblDesc0, "luceneIdx"));

        idx.distributedConfiguration().disableCreateLuceneIndexForStringValueType(true).get();

        sql("create table test1(id1 int primary key, val varchar) " +
            "WITH \"WRAP_VALUE=false\"");

        H2TableDescriptor tblDesc1 = idx.schemaManager().dataTable("PUBLIC", "TEST1")
            .rowDescriptor().tableDescriptor();

        assertNull(GridTestUtils.getFieldValue(tblDesc1, "luceneIdx"));
    }

    /**
     * Checks that part of the composite key assembled in BinaryObjectBuilder can pass the validation correctly
     if you specify Object type when creating the index.
     */
    @Test
    public void testCacheSecondaryCompositeIndex() throws Exception {
        inlineSize = 70;

        startGrid();

        String cacheName = "TEST";

        grid().createCache(new CacheConfiguration<>()
                .setName(cacheName)
                .setSqlSchema(cacheName)
                .setAffinity(new RendezvousAffinityFunction(false, 4))
                .setQueryEntities(Collections.singleton(new QueryEntity()
                        .setTableName(cacheName)
                        .setKeyType(Integer.class.getName())
                        .setKeyFieldName("id")
                        .setValueType("TEST_VAL_SECONDARY_COMPOSITE")
                        .addQueryField("id", Integer.class.getName(), null)
                        .addQueryField("val_obj", Object.class.getName(), null)
                        .setIndexes(Arrays.asList(
                                        new QueryIndex(Arrays.asList("val_obj"), QueryIndexType.SORTED)
                                                .setInlineSize(inlineSize)
                                )
                        ))));

        BinaryObjectBuilder bob = grid().binary().builder("TEST_VAL_SECONDARY_COMPOSITE");
        BinaryObjectBuilder bobInner = grid().binary().builder("inner");

        bobInner.setField("inner_k", 0);
        bobInner.setField("inner_uuid", UUID.randomUUID());

        bob.setField("val_obj", bobInner.build());

        grid().cache(cacheName).put(0, bob.build());
    }

    /** */
    @Test
    public void testCreateSystemIndexWithSpecifiedInlineSizeByDdl() throws Exception {
        srvLog = new ListeningTestLogger(false, log);

        inlineSize = 10;

        final int pkInlineSize = 22;
        final int affInlineSize = 23;

        IgniteEx ign = startGrid();

        sql("CREATE TABLE TEST (ID VARCHAR, ID_AFF INT, VAL INT, "
                + "PRIMARY KEY (ID, ID_AFF)) WITH"
                + "\""
                + "AFFINITY_KEY=ID_AFF,"
                + "PK_INLINE_SIZE=" + pkInlineSize + ","
                + "AFFINITY_INDEX_INLINE_SIZE=" + affInlineSize
                + "\""
        );

        GridH2Table tbl = ((IgniteH2Indexing)ign.context().query().getIndexing()).schemaManager().dataTable("PUBLIC", "TEST");

        assertEquals(pkInlineSize, ((H2TreeIndex)tbl.getIndex("_key_PK")).inlineSize());
        assertEquals(affInlineSize, ((H2TreeIndex)tbl.getIndex("AFFINITY_KEY")).inlineSize());

        // Check the warning log message
        LogListener lsnr = LogListener
            .matches("Indexed columns of a row cannot be fully inlined")
            .andMatches("for sorted indexes on primary key and affinity field use 'PK_INLINE_SIZE' and 'AFFINITY_INDEX_INLINE_SIZE' properties for CREATE TABLE command").build();

        srvLog.registerListener(lsnr);

        final String key_prefix = "ID____________________________________________________________________";

        for (int i = 0; i < 1000; ++i) {
            sql("INSERT INTO TEST VALUES (?, ?, ?)",
                key_prefix + i,
                i,
                i);
        }

        assertTrue(lsnr.check());
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = Integer.MAX_VALUE + "")
    public void testMaxInlineSizeIsUsedWhenExceeded() throws Exception {
        inlineSize = -1;

        IgniteEx ign = startGrid();

        int idxColSize = MAX_INLINE_SIZE + 1;

        sql("CREATE TABLE TEST (ID VARCHAR(" + idxColSize + "), ID_AFF VARCHAR, PRIMARY KEY (ID))");

        GridH2Table tbl = ((IgniteH2Indexing)ign.context().query().getIndexing()).schemaManager().dataTable("PUBLIC", "TEST");

        // Inline size can't be bigger than this constant.
        assertEquals(MAX_INLINE_SIZE, ((H2TreeIndex)tbl.getIndex("_key_PK")).inlineSize());
    }

    /** */
    @Test
    public void testWarnWhenConfiguredInlineSizeExceedsMax() throws Exception {
        inlineSize = -1;
        srvLog = new ListeningTestLogger(false, log);

        startGrid();

        int idxInlineSize = MAX_INLINE_SIZE + 1;

        String warnMsg = "Explicit INLINE_SIZE exceeds maximum size. Ignoring " +
                "[index=SOME_IDX, explicitInlineSize=" + idxInlineSize + ", maxInlineSize=" + MAX_INLINE_SIZE + "]";

        LogListener lsnr = LogListener.matches(warnMsg).build();

        srvLog.registerListener(lsnr);

        sql("CREATE TABLE TEST (ID VARCHAR, ID_AFF INT, VAL INT, PRIMARY KEY (ID, ID_AFF))");
        sql("CREATE INDEX SOME_IDX ON TEST (VAL) inline_size " + idxInlineSize);

        assertTrue(lsnr.check());

        srvLog.unregisterListener(lsnr);
    }

    /** */
    private void checkAll() {
        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        checkRemovePut(cache);

        checkSelectAll(cache);

        checkSelectStringEqual(cache);

        checkSelectLongEqual(cache);

        checkSelectStringRange(cache);

        checkSelectLongRange(cache);
    }

    /** */
    private void populateCache() {
        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Be paranoid and populate first even indexes in ascending order, then odd indexes in descending
        // to check that inserting in the middle works.

        for (int i = 0; i < 100; i += 2)
            cache.put(key(i), val(i));

        for (int i = 99; i > 0; i -= 2)
            cache.put(key(i), val(i));

        for (int i = 99; i > 0; i -= 2)
            assertEquals(val(i), cache.get(key(i)));
    }

    /** */
    private void checkRemovePut(IgniteCache<Key, Val> cache) {
        final int INT = 24;

        assertEquals(val(INT), cache.get(key(INT)));

        cache.remove(key(INT));

        assertNull(cache.get(key(INT)));

        cache.put(key(INT), val(INT));

        assertEquals(val(INT), cache.get(key(INT)));
    }

    /** */
    private void checkSelectAll(IgniteCache<Key, Val> cache) {
        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val")).getAll();

        assertEquals(100, data.size());

        for (List<?> row : data) {
            Key key = (Key) row.get(0);

            Val val = (Val) row.get(1);

            long i = key.keyLong;

            assertEquals(key(i), key);

            assertEquals(val(i), val);
        }
    }

    /** */
    private void checkSelectStringEqual(IgniteCache<Key, Val> cache) {
        final String STR = "foo011";

        final long LONG = 11;

        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val where keyStr = ?")
            .setArgs(STR))
            .getAll();

        assertEquals(1, data.size());

        List<?> row = data.get(0);

        assertEquals(key(LONG), row.get(0));

        assertEquals(val(LONG), row.get(1));
    }

    /** */
    private void checkSelectLongEqual(IgniteCache<Key, Val> cache) {
        final long LONG = 42;

        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val where valLong = ?")
            .setArgs(LONG))
            .getAll();

        assertEquals(1, data.size());

        List<?> row = data.get(0);

        assertEquals(key(LONG), row.get(0));

        assertEquals(val(LONG), row.get(1));
    }

    /** */
    private void checkSelectStringRange(IgniteCache<Key, Val> cache) {
        final String PREFIX = "foo06";

        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val where keyStr like ?")
            .setArgs(PREFIX + "%"))
            .getAll();

        assertEquals(10, data.size());

        for (List<?> row : data) {
            Key key = (Key) row.get(0);

            Val val = (Val) row.get(1);

            long i = key.keyLong;

            assertEquals(key(i), key);

            assertEquals(val(i), val);

            assertTrue(key.keyStr.startsWith(PREFIX));
        }
    }

    /** */
    private void checkSelectLongRange(IgniteCache<Key, Val> cache) {
        final long RANGE_START = 70;

        final long RANGE_END = 80;

        List<List<?>> data = cache.query(
            new SqlFieldsQuery("select _key, _val from Val where valLong >= ? and valLong < ?")
                .setArgs(RANGE_START, RANGE_END))
            .getAll();

        assertEquals(10, data.size());

        for (List<?> row : data) {
            Key key = (Key) row.get(0);

            Val val = (Val) row.get(1);

            long i = key.keyLong;

            assertEquals(key(i), key);

            assertEquals(val(i), val);

            assertTrue(i >= RANGE_START && i < RANGE_END);
        }
    }

    /** */
    private void createDynamicIndexes(String... cols) {
        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (String col : cols) {
            String indexName = col + "_idx";
            String schemaName = DEFAULT_CACHE_NAME;

            cache.query(new SqlFieldsQuery(
                String.format("create index %s on \"%s\".Val(%s) INLINE_SIZE %s;", indexName, schemaName, col, inlineSize)
            )).getAll();
        }

        cache.indexReadyFuture().get();
    }

    /** */
    private void dropDynamicIndexes(String... cols) {
        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (String col : cols) {
            String indexName = col + "_idx";

            cache.query(new SqlFieldsQuery(
                String.format("drop index %s;", indexName)
            )).getAll();
        }

        cache.indexReadyFuture().get();
    }

    /** Key object factory method. */
    private static Key key(long i) {
        return new Key(String.format("foo%03d", i), i, new Pojo(i));
    }

    /** Value object factory method.*/
    private static Val val(long i) {
        return new Val(String.format("bar%03d", i), i, new Pojo(i));
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return sql(grid(), sql, args);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setArgs(args), false);
    }

    /** Key object. */
    private static class Key {
        /** */
        private String keyStr;

        /** */
        private long keyLong;

        /** */
        private Pojo keyPojo;

        /** */
        private Key(String str, long aLong, Pojo pojo) {
            keyStr = str;
            keyLong = aLong;
            keyPojo = pojo;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key) o;

            return keyLong == key.keyLong &&
                Objects.equals(keyStr, key.keyStr) &&
                Objects.equals(keyPojo, key.keyPojo);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(keyStr, keyLong, keyPojo);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Key.class, this);
        }
    }

    /** Value object. */
    private static class Val {
        /** */
        private String valStr;

        /** */
        private long valLong;

        /** */
        private Pojo valPojo;

        /** */
        private Val(String str, long aLong, Pojo pojo) {
            valStr = str;
            valLong = aLong;
            valPojo = pojo;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Val val = (Val) o;

            return valLong == val.valLong &&
                Objects.equals(valStr, val.valStr) &&
                Objects.equals(valPojo, val.valPojo);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(valStr, valLong, valPojo);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Val.class, this);
        }
    }

    /** User object. */
    private static class Pojo {
        /** */
        private long pojoLong;

        /** */
        private Pojo(long pojoLong) {
            this.pojoLong = pojoLong;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Pojo pojo = (Pojo) o;

            return pojoLong == pojo.pojoLong;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(pojoLong);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Pojo.class, this);
        }
    }

    /** User object converter. */
    static class UserObject implements SqlStrConvertedValHolder {
        /** User object. */
        private Object obj;

        /** Constructor. */
        public UserObject(Object obj) {
            this.obj = obj;
        }

        /** {@inheritDoc} */
        @Override public Object originalVal() {
            return obj;
        }

        /** {@inheritDoc} */
        @Override public String sqlStrVal() {
            return null; // UserObject has no string representation.
        }
    }

    /**
     * Supported sql data types with corresponding java mappings.
     * https://apacheignite-sql.readme.io/docs/data-types
     */
    private enum SqlDataType {
        /** */
        INT(Integer.class),

        /** */
        BIGINT(Long.class),

        /** */
        DECIMAL(BigDecimal.class),

        /** */
        DOUBLE(Double.class),

        /** */
        TIME(java.sql.Time.class),

        /** */
        DATE(java.sql.Date.class),

        /** */
        TIMESTAMP(java.sql.Timestamp.class),

        /** */
        VARCHAR(String.class),

        /** */
        OTHER(Pojo.class),

        /** */
        UUID(UUID.class),

        /** */
        BINARY(byte[].class);

        /**
         * Corresponding java type https://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
         */
        private Object javaType;

        /** */
        SqlDataType(Object javaType) {
            this.javaType = javaType;
        }
    }
}
