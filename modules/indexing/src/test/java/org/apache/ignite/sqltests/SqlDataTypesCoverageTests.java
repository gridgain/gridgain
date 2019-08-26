/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.sqltests;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheDataTypesCoverageTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class SqlDataTypesCoverageTests extends GridCacheDataTypesCoverageTest {
    /** */
    private static final int NODES_CNT = 3;

    /** {@inheritDoc} */
    @Before
    @Override
    public void init() throws Exception {
        super.init();
    }

//    /** {@inheritDoc} */
//    @Override protected void afterTestsStopped() throws Exception {
//        super.afterTestsStopped();
//    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-boolean
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBooleanDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.BOOLEAN, Boolean.TRUE, Boolean.FALSE);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-int
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIntDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.INT, 0, 1, Integer.MAX_VALUE, Integer.MIN_VALUE);
    }


    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-tinyint
     *
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23065")
    @Test
    public void testTinyIntDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.TINYINT, (byte)0, (byte)1, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-smallint
     *
     * @throws Exception If failed.
     */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-23065")
    @Test
    public void testSmallIntDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.SMALLINT, (short)0, (short)1, Short.MIN_VALUE, Short.MAX_VALUE);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-bigint
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBigIntDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.BIGINT, 0L, 1L, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-decimal
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.DECIMAL,
            new BigDecimal(123.123),
            BigDecimal.ONE,
            BigDecimal.ZERO,
            BigDecimal.valueOf(123456789, 0),
            BigDecimal.valueOf(123456789, 1),
            BigDecimal.valueOf(123456789, 2),
            BigDecimal.valueOf(123456789, 3));
    }

    // TODO: 26.08.19 besides others, it's really strange to see JDBC in exception Caused by: org.h2.jdbc.JdbcSQLSyntaxErrorException: Column "INFINITY" not found; SQL statement:
    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-double
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDoubleDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.DOUBLE,
            Double.MIN_VALUE,
            Double.MAX_VALUE,
            Double.NaN,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            0,
            0.0,
            1,
            1.0,
            1.1);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-real
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRealDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.REAL,
            Float.MIN_VALUE,
            Float.MAX_VALUE,
            Float.NaN,
            Float.NEGATIVE_INFINITY,
            Float.POSITIVE_INFINITY,
            0F,
            0.0F,
            1F,
            1.1F);
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-time
     *
     * @throws Exception If failed.
     */
    @Test
    // TODO: 26.08.19 add parse time, parse date, etc otherwise class org.apache.ignite.internal.processors.query.IgniteSQLException: Failed to parse query. Syntax error in SQL statement "INSERT INTO TABLE0FE7A3(ID, VAL)  VALUES (03:[*]00:00, 03:00:00); "; expected "[, ::, *, /, %, +, -, ||, ~, !~, NOT, LIKE, ILIKE, REGEXP, IS, IN, BETWEEN, AND, OR, ,, )"; SQL statement:
    //INSERT INTO table0fe7a3(id, val)  VALUES (03:00:00, 03:00:00); [42001-199]
    public void testTimeDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.TIME,
            new java.sql.Time(0L),
            java.sql.Time.from(Instant.now()));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-date
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDateDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.DATE,
            new java.sql.Date(123L),
            java.sql.Date.from(Instant.now()));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-timestamp
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTimestampDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.TIMESTAMP,
            new java.sql.Timestamp(123L),
            java.sql.Timestamp.from(Instant.now()));
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-varchar
     *
     * @throws Exception If failed.
     */
    @Test
    // TODO: 26.08.19 We should use versions of strings without qoutes as expected values.
    public void testVarcharDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.VARCHAR,
            "\'\'",
            "\'abcABC\'",
            "\'!@#$%^&*()\'");
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-char
     *
     * @throws Exception If failed.
     */
    @Test
    // TODO: 26.08.19 We should use versions of strings without qoutes as expected values.
    public void testCharDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.CHAR,
            "\'a\'",
            "\'B\'",
            "\'@\'");
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-uuid
     *
     * @throws Exception If failed.
     */
    @Test
    // TODO: 26.08.19 We should use versions of strings without qoutes as expected values.
    public void testUUIDDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.UUID,
            "\'" + UUID.randomUUID().toString() + "\'",
            "\'" +  UUID.randomUUID().toString() + "\'");
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-binary
     *
     * @throws Exception If failed.
     */
    @Test
    // TODO: 26.08.19 Caused by: org.h2.jdbc.JdbcSQLSyntaxErrorException: Syntax error in SQL statement "INSERT INTO TABLEFA3122(ID, VAL)  VALUES ([[*]LJAVA.LANG.BYTE;@7232E370, [LJAVA.LANG.BYTE;@7232E370); "; expected "), DEFAULT, NOT, EXISTS, INTERSECTS"; SQL statement:
    public void testBinaryDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.BINARY,
            new Byte[]{},
            new Byte[] {1, 2, 3},
            new byte[] {3, 2, 1},
            new byte[]{});
    }

    /**
     * https://apacheignite-sql.readme.io/docs/data-types#section-geometry
     *
     * @throws Exception If failed.
     */
    // TODO: 26.08.19
    @Ignore("todo")
    @Test
    public void testGeometryDataType() throws Exception {
        checkBasicSqlOperations(SqlDataType.GEOMETRY, Boolean.TRUE, Boolean.FALSE);
    }

    private void checkBasicSqlOperations(SqlDataType dataType, Serializable... valsToCheck) throws Exception {
        assert valsToCheck.length > 0;

        IgniteEx ignite = grid(new Random().nextInt(NODES_CNT));

        final String tblName = "table" + UUID.randomUUID().toString().substring(0, 6);

        // TODO: 26.08.19 use test params for atomicity, backups etc.
        ignite.context().query().querySqlFields(new SqlFieldsQuery(
            "CREATE TABLE " + tblName +
                "(id " + dataType + " PRIMARY KEY," +
                " val " + dataType + ")" +
                " WITH " + "\"atomicity=transactional_snapshot,backups=3\""), false);

        checkCRUD(ignite, tblName, dataType, valsToCheck);
    }

    private void checkCRUD(IgniteEx ignite, String tblName, SqlDataType dataType, Serializable... valsToCheck) {
        for (int i = 0; i < valsToCheck.length; i++) {

            Object valToCheck = valsToCheck[i];

            // INSERT
            ignite.context().query().querySqlFields(new SqlFieldsQuery("INSERT INTO " + tblName + "(id, val)  VALUES (" + valToCheck + ", " + valToCheck + ");"), false);

            // Check INSERT/SELECT
            List<List<?>> res = ignite.context().query().querySqlFields(new SqlFieldsQuery("SELECT id, val FROM " + tblName + ";"), false).getAll();

            assertEquals(1, res.size());

            assertEquals(2, res.get(0).size());

            // key
            assertTrue(res.get(0).get(0).getClass().equals(dataType.javaType));

            // TODO: 26.08.19 add cloning here, like it was done in base test?
            assertEquals(valToCheck, res.get(0).get(0));

            // val
            assertTrue(res.get(0).get(1).getClass().equals(dataType.javaType));

            assertEquals(valToCheck, res.get(0).get(1));

            Object revertedVal = valsToCheck[valsToCheck.length - 1 - i];
            // UPDATE
            ignite.context().query().querySqlFields(new SqlFieldsQuery("UPDATE " + tblName + " SET val =  " + revertedVal + ";"), false);

            // Check UPDATE/SELECT
            res = ignite.context().query().querySqlFields(new SqlFieldsQuery("SELECT id, val FROM " + tblName + ";"), false).getAll();

            assertEquals(1, res.size());

            assertEquals(2, res.get(0).size());

            // key
            assertTrue(res.get(0).get(0).getClass().equals(dataType.javaType));

            // TODO: 26.08.19 add cloning here, like it was done in base test?
            assertEquals(valToCheck, res.get(0).get(0));

            // val
            assertTrue(res.get(0).get(1).getClass().equals(dataType.javaType));

            assertEquals(revertedVal, res.get(0).get(1));

            // DELETE
            ignite.context().query().querySqlFields(new SqlFieldsQuery("DELETE FROM " + tblName + " where id =  " + valToCheck + ";"), false);

            // Check DELETE/SELECT
            res = ignite.context().query().querySqlFields(new SqlFieldsQuery("SELECT id FROM " + tblName + ";"), false).getAll();

            assertEquals(0, res.size());
        }
    }

    private void checkBasicSqlOperations(Serializable... valsToCheck) throws Exception {
        assert valsToCheck.length > 0;

        IgniteEx ignite = grid(new Random().nextInt(NODES_CNT));

        final String tblName = "table" + UUID.randomUUID().toString().substring(0, 6);

        ignite.context().query().querySqlFields(new SqlFieldsQuery("CREATE TABLE " + tblName
            + "(id BOOLEAN PRIMARY KEY, val BOOLEAN)  WITH " + "\"atomicity=transactional_snapshot,backups=3\""), false);

//        ignite.context().query().querySqlFields(new SqlFieldsQuery("CREATE TABLE table" + UUID.randomUUID().toString().substring(0, 6)
//            + "(id BOOLEAN PRIMARY KEY, val BOOLEAN) WITH atomicity=" + atomicityMode + ",cache_group=group1;"), false);

        ignite.context().query().querySqlFields(new SqlFieldsQuery("CREATE TABLE " + tblName
            + "(id BOOLEAN PRIMARY KEY, val BOOLEAN)  WITH " + "\"atomicity=transactional_snapshot,backups=3\""), false);

        // INSERT
        ignite.context().query().querySqlFields(new SqlFieldsQuery("INSERT INTO " + tblName + "(id, val)  VALUES (" + Boolean.TRUE + ", " + Boolean.TRUE + ");"), false);

        // Check insert: SELECT
        List<List<?>> res = ignite.context().query().querySqlFields(new SqlFieldsQuery("SELECT id FROM " + tblName + ";"), false).getAll();

        assertEquals(1, res.size());

        assertEquals(1, res.get(0).size());

        assertTrue(res.get(0).get(0) instanceof Boolean);

        assertTrue((Boolean) res.get(0).get(0));

        // UPDATE
        ignite.context().query().querySqlFields(new SqlFieldsQuery("UPDATE " + tblName + " SET val =  " + Boolean.FALSE + ";"), false);

        // Check update: SELECT
        res = ignite.context().query().querySqlFields(new SqlFieldsQuery("SELECT id FROM " + tblName + ";"), false).getAll();

        assertEquals(1, res.size());

        assertEquals(1, res.get(0).size());

        assertTrue(res.get(0).get(0) instanceof Boolean);

        assertTrue((Boolean) res.get(0).get(0));

        // DELETE
        ignite.context().query().querySqlFields(new SqlFieldsQuery("DELETE FROM " + tblName + " where id =  " + Boolean.TRUE + ";"), false);

        // Check delete: SELECT
        res = ignite.context().query().querySqlFields(new SqlFieldsQuery("SELECT id FROM " + tblName + ";"), false).getAll();

        assertEquals(0, res.size());





//        node.getOrCreateCache(DEFAULT_CACHE_NAME)
//            .query(new SqlFieldsQuery("CREATE TABLE City (id int primary key, name varchar, population int) WITH " +
//                "\"atomicity=transactional_snapshot,cache_group=group1,template=partitioned,backups=3,cache_name=City\""))
//            .getAll();;



//        execute("CREATE TABLE " + depTabName + " (" +
//            "id LONG PRIMARY KEY," +
//            "idNoidx LONG, " +
//            "name VARCHAR" +
//            ") " +
//            (F.isEmpty(commonParams) ? "" : " WITH \"" + commonParams + "\"") +
//            ";");
//
//
//        FieldsQueryCursor<List<?>> cursor = ((IgniteEx)node).context().query().querySqlFields(qry, false);
//
//        return BaseSqlTest.Result.fromCursor(cursor);
    }

    /**
     * https://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
     */
    private enum SqlDataType {
        BOOLEAN (Boolean.class),
        INT (Integer.class),
        TINYINT (Byte.class),
        SMALLINT (Short.class),
        BIGINT (Long.class),
        DECIMAL (BigDecimal.class),
        DOUBLE (Double.class),
        REAL (Float.class),
        TIME (java.sql.Time.class),
        DATE (java.sql.Date.class),
        TIMESTAMP (java.sql.Timestamp.class),
        VARCHAR (String.class),
        CHAR (String.class),
        UUID (UUID.class),
        BINARY (byte[].class),
        GEOMETRY (Boolean.class);

        private Object javaType;

        SqlDataType(Object javaType) {
            this.javaType = javaType;
        }

        /**
         * @return Java type.
         */
        public Object javaType() {
            return javaType;
        }
    }
}
