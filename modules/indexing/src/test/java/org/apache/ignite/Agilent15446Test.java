/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.gridgain.internal.h2.engine.Session;
import org.gridgain.internal.h2.index.Cursor;
import org.gridgain.internal.h2.index.Index;
import org.gridgain.internal.h2.result.Row;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_HOME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;

/**
 * Investigation of incident agilent-15446.
 */
public class Agilent15446Test extends GridCommonAbstractTest {
    /** Name of the cache with the corrupted index: WCS10. */
    private static final String WCSC10_CACHE_NAME = "WCSC10";

    /** Name of the cache with the corrupted index: ATGC16. */
    private static final String ATGC16_CACHE_NAME = "ATGC16";

    /** Index name: _key_PK. */
    private static final String PK_IDX_NAME = "_key_PK";

    /** Index name: _key_PK_hash. */
    private static final String PK_HASH_IDX_NAME = "_key_PK_hash";

    /** Index name: PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES_IDX_ATTRIBUTE_ID. */
    private static final String USER_0_IDX_NAME = "PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES_IDX_ATTRIBUTE_ID";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.nullifyHomeDirectory();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setWorkDirectory(U.getIgniteHome())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setMaxWalArchiveSize(UNLIMITED_WAL_ARCHIVE)
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "Production_Node";
    }

    /**
     * Outputting rows from the {@link #USER_0_IDX_NAME}.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_HOME, value = "C:\\Users\\tkalk\\IdeaProjects\\incubator-ignite\\work0")
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void test0() throws Exception {
        IgniteEx n = startGrid(2);

        n.cluster().state(ACTIVE);

        iterate(index(n, ATGC16_CACHE_NAME, USER_0_IDX_NAME), row -> log.info("c=" + row));
    }

    /**
     * Search for rows that are not in the {@link #PK_IDX_NAME}
     * and are present in {@link #USER_0_IDX_NAME}, {@link #PK_HASH_IDX_NAME}.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_HOME, value = "C:\\Users\\tkalk\\IdeaProjects\\incubator-ignite\\work0")
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void test1() throws Exception {
        IgniteEx n = startGrid(2);

        n.cluster().state(ACTIVE);

        Map<? super Object, Row> rows0 = new HashMap<>();
        Map<? super Object, Row> rows1 = new HashMap<>();

        iterate(
            index(n, ATGC16_CACHE_NAME, USER_0_IDX_NAME),
            row -> rows0.put(key(row), row)
        );

        iterate(
            index(n, ATGC16_CACHE_NAME, PK_HASH_IDX_NAME),
            row -> rows1.put(key(row), row)
        );

        iterate(
            index(n, ATGC16_CACHE_NAME, PK_IDX_NAME),
            row -> {
                rows0.remove(key(row));
                rows1.remove(key(row));
            }
        );

        if (rows0.keySet().equals(rows1.keySet()))
            print(rows0, keyWithLink());
        else {
            log.info("Rows not equals!!!");

            print(rows0, keyWithLink());
            print(rows1, keyWithLink());
        }
    }

    /**
     * An attempt to add rows to an {@link #PK_IDX_NAME} that are not present in it,
     * but are in the {@link #USER_0_IDX_NAME}.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_HOME, value = "C:\\Users\\tkalk\\IdeaProjects\\incubator-ignite\\work0")
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void test11() throws Exception {
        IgniteEx n = startGrid(2);

        n.cluster().state(ACTIVE);

        Map<? super Object, Row> rows = new HashMap<>();

        iterate(
            index(n, ATGC16_CACHE_NAME, USER_0_IDX_NAME),
            row -> rows.put(key(row), row)
        );

        iterate(
            index(n, ATGC16_CACHE_NAME, PK_IDX_NAME),
            row -> rows.remove(key(row))
        );

        enableCheckpoints(n, false);
        disableWal(n, false);

        IgniteCacheDatabaseSharedManager dbMgr = n.context().cache().context().database();

        dbMgr.checkpointReadLock();

        try {
            H2TreeIndex idx = index(n, ATGC16_CACHE_NAME, PK_IDX_NAME);

            for (Row row : rows.values()) {
                H2CacheRow put = idx.put(((H2CacheRow)row));

                if (put != null)
                    log.warning("WOW " + put + ":::" + row);
            }
        }
        finally {
            dbMgr.checkpointReadUnlock();
        }

        iterate(
            index(n, ATGC16_CACHE_NAME, PK_IDX_NAME),
            row -> rows.remove(key(row))
        );

        print(rows, keyWithLink());
    }

    /**
     * Investigation of insert and delete operations for sql table.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_HOME, value = "C:\\Users\\tkalk\\IdeaProjects\\incubator-ignite\\work")
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void test2() throws Exception {
        IgniteEx n = startGrid(2);

        n.cluster().state(ACTIVE);

        String tblName = "PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES";

        n.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS " + tblName + " ( " +
                "ASSET_ID VARCHAR, " +
                "ATTRIBUTE_ID VARCHAR, " +
                "LANGUAGE VARCHAR, " +
                "ATTRIBUTE_VALUE VARCHAR, " +
                "ATTRIBUTES_STRING VARCHAR, " +
                "IS_MULTIPLE_VALUE VARCHAR, " +
                "CREATE_TIME TIMESTAMP, " +
                "MODIFY_TIME TIMESTAMP, " +
                "STATUS VARCHAR, " +
                "CONSTRAINT PK_PUBLIC_PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES PRIMARY KEY (ASSET_ID,ATTRIBUTE_ID,LANGUAGE,ATTRIBUTE_VALUE)\n" +
                ") WITH \"" +
                "template=REPLICATED, " +
                "key_type=ATGKeyType16, " +
                "CACHE_NAME=" + ATGC16_CACHE_NAME + ", " +
                "value_type=ATGValueType16" +
                "\""
            ),
            false
        );

        n.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS " +
                "PIM_ATG_ASSET_DYNAMIC_ATTRIBUTES " +
                "ON " + tblName + " (ASSET_ID, LANGUAGE)"
            ),
            false
        );

        n.context().query().querySqlFields(
            new SqlFieldsQuery("INSERT INTO " + tblName + " " +
                "(ASSET_ID, ATTRIBUTE_ID, LANGUAGE, ATTRIBUTE_VALUE, ATTRIBUTES_STRING, IS_MULTIPLE_VALUE, CREATE_TIME, MODIFY_TIME, STATUS) " +
                "VALUES (?,?,?,?,?,?,?,?,?)"
            ).setArgs("SDS_534770", "Name", "ja", "ELIBRARY_671069", "", "N", currentTimestamp(), currentTimestamp(), "1"),
            false
        );

        n.context().query().querySqlFields(
            new SqlFieldsQuery("DELETE FROM " + tblName + " WHERE " +
                "ASSET_ID = ? AND ATTRIBUTE_ID = ? AND LANGUAGE = ? AND ATTRIBUTE_VALUE = ?"
            ).setArgs("SDS_534770", "Name", "ja", "ELIBRARY_671069"),
            false
        );

        forceCheckpoint();

        log.info("TODO: WAL segment research");
    }

    private long iterate(Index index, IgniteThrowableConsumer<Row> rowConsumer) throws Exception {
        Cursor cursor = index.find((Session)null, null, null);

        long cnt = 0;

        while (cursor.next()) {
            rowConsumer.accept(cursor.get());

            cnt++;
        }

        return cnt;
    }

    private H2Tree[] segments(Object o) {
        return GridTestUtils.getFieldValue(o, "segments");
    }

    private <T extends Index> T index(IgniteEx n, String cacheName, String idxName) {
        IgniteH2Indexing indexing = (IgniteH2Indexing)n.context().query().getIndexing();

        GridH2Table table = indexing.schemaManager().dataTables().stream()
            .filter(t -> t.cacheName().equals(cacheName))
            .findAny()
            .get();

        return (T)table.getIndex(idxName);
    }

    private KeyCacheObject key(Row row) {
        return ((H2CacheRow)row).key();
    }

    private void print(Map<?, Row> rows, BiFunction<? super Object, Row, String> toStrFun) {
        log.info("rows.size=" + rows.size());

        log.info("Rows");
        rows.forEach((k, row) -> log.info(toStrFun.apply(k, row)));
    }

    private BiFunction<? super Object, Row, String> keyWithLink() {
        return (k, row) -> "k=" + k + ", link=" + U.hexLong(((H2CacheRow)row).link());
    }

    private Timestamp currentTimestamp() {
        return new Timestamp(U.currentTimeMillis());
    }
}
