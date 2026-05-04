/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.jetbrains.annotations.Nullable;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.processors.query.QueryUtils.sysSchemaName;

@SuppressWarnings({"resource", "UnusedReturnValue"})
public abstract class SqlSystemViewAbstractAffinityColumnSelfTest extends AbstractIndexingCommonTest {

    /** Cache name */
    protected static final String CACHE_NAME = "users";

    /** Table name */
    protected static final String TABLE_NAME = Val.class.getSimpleName().toUpperCase();

    /** Key type field annotated with {@link AffinityKeyMapped} */
    protected static final String userId = "userId";

    /** Key type field without {@link AffinityKeyMapped} annotation */
    protected static final String groupId = "groupId";

    /** Key type field alias */
    protected static final String qe_alias = "qe_alias";

    /** Key type field alias from annotation */
    protected static final String annotation_alias = "a_alias";

    /** Name of the implicit column representing whole key instance */
    protected static final String _KEY = "_KEY";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration()
                .setHost("127.0.0.1")
                .setPort(11211)
            );
    }

    @Override protected String queryCache() {
        return CACHE_NAME;
    }

    protected <K> IgniteCache<K, Val> setupCache(Consumer<CacheConfiguration<K, Val>> cacheCfgTuning) throws Exception {
        IgniteEx ignite = startGrid(getConfiguration());

        CacheConfiguration<K, Val> cacheCfg = defaultCacheConfiguration();
        cacheCfg.setName(CACHE_NAME);
        cacheCfgTuning.accept(cacheCfg);

        return ignite.getOrCreateCache(cacheCfg);
    }

    protected void assertTableViewHas(String expectedBinaryFieldColumnValue, String expectedAffKeyColumnValue) {
        List<List<String>> expected = rowOf(
            TABLE_NAME,
            expectedBinaryFieldColumnValue,
            _KEY.equals(expectedAffKeyColumnValue) ? null : expectedAffKeyColumnValue
        );

        assertEquals(expected, selectAffinityPropertiesFromTableSysView());
    }

    /**
     * Check that only column that matches provided name has <code>affinity=true</code>
     *
     * @param expectedAffColumn Expected affinity column, null when no column should be marked as affinity.
     *                          Note that even _KEY should not be marked as affinity when
     */
    protected void assertTableColumnsViewHasOnlyThatColumnAsAffinity(@Nullable String expectedAffColumn) {
        List<List<?>> res = selectAffinityFlagFromTableColumnsSysView();

        String affinityCol = null;

        try {
            for (List<?> row : res) {
                String colName = (String)row.get(0);
                Boolean isAffinity = (Boolean)row.get(1);

                if (isAffinity) {
                    assertNotNull("No affinity columns expected but <" + colName + "> found", expectedAffColumn);
                    assertNull("More than 1 affinity columns found", affinityCol);

                    affinityCol = colName;
                }
            }

            assertEquals("No matching found for expected affinity column <" + expectedAffColumn + ">", expectedAffColumn, affinityCol);
        }
        catch (AssertionError original) {
            throw new AssertionError(original.getMessage() +
                "\nCOLUMN_NAME, IS_AFFINITY" +
                "\n========================" +
                res.stream().map(r -> "\n" + r).collect(Collectors.joining()) +
                "\n"
            );
        }
    }

    private List<List<?>> selectAffinityPropertiesFromTableSysView() {
        return execSql0("SELECT TABLE_NAME, BINARY_AFFINITY_FIELD, AFFINITY_KEY_COLUMN " +
            "FROM " + sysSchemaName() + ".TABLES " +
            "WHERE CACHE_NAME = '" + CACHE_NAME + "'"
        );
    }

    private List<List<?>> selectAffinityFlagFromTableColumnsSysView() {
        return execSql0("SELECT COLUMN_NAME, AFFINITY_COLUMN " +
            "FROM " + sysSchemaName() + ".TABLE_COLUMNS " +
            "WHERE TABLE_NAME = '" + TABLE_NAME + "'"
        );
    }

    @SafeVarargs
    private static <T> List<List<T>> rowOf(T... cols) {
        return Collections.singletonList(asList(cols));
    }

    protected static class UnannotatedKey {
        Integer groupId = 0;
    }

    protected static class AffKey {
        @AffinityKeyMapped
        Integer userId = 0;

        Integer groupId = 0;
    }

    protected static class SqlKey {
        @QuerySqlField
        Integer userId = 0;

        Integer groupId = 0;
    }

    protected static class AffSqlKey {
        @AffinityKeyMapped
        @QuerySqlField
        Integer userId = 0;

        Integer groupId = 0;
    }

    protected static class AffSqlKeyWithAlias {
        @AffinityKeyMapped
        @QuerySqlField(name = annotation_alias)
        Integer userId = 0;

        Integer groupId = 0;
    }

    protected static class Val {
        @QuerySqlField
        Integer val = 0;
    }

    /** Utility wrapper that helps to avoid dumping full-class names in JUnit test header. */
    protected enum KeyType {
        UNANNOTATED(UnannotatedKey.class),
        AFF_ONLY(AffKey.class),
        SQL_ONLY(SqlKey.class),
        AFF_AND_SQL(AffSqlKey.class),
        AFF_AND_SQL_WITH_ALIAS(AffSqlKeyWithAlias.class);

        private final Class<?> keyCls;

        KeyType(Class<?> keyCls) {
            this.keyCls = keyCls;
        }

        public Class<?> keyCls() {
            return keyCls;
        }

        public String keyName() {
            return keyCls().getName();
        }
    }
}
