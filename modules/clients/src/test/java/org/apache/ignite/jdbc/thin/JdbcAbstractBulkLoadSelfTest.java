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

package org.apache.ignite.jdbc.thin;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.bulkload.BulkLoadCsvFormat;
import org.apache.ignite.internal.processors.bulkload.BulkLoadCsvParser;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.Callable;

import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;
import static org.apache.ignite.jdbc.thin.JdbcThinBulkLoadSelfTest.BASIC_SQL_COPY_STMT;

/**
 * Common COPY tests for JDBC Thin and JDBC driver
 */
public abstract class JdbcAbstractBulkLoadSelfTest extends JdbcThinAbstractDmlStatementSelfTest {

    /** Subdirectory with CSV files. */
    protected static final String CSV_FILE_SUBDIR = "/modules/clients/src/test/resources/";

    /** Default table name. */
    protected static final String TBL_NAME = "Person";

    /** A CSV file with zero records. */
    protected static final String BULKLOAD_EMPTY_CSV_FILE =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload0.csv"))
                    .getAbsolutePath();

    /** A CSV file with one record. */
    protected static final String BULKLOAD_ONE_LINE_CSV_FILE =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload1.csv")).getAbsolutePath();

    /** A CSV file with two records. */
    protected static final String BULKLOAD_TWO_LINES_CSV_FILE =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload2.csv")).getAbsolutePath();

    /** A CSV file in UTF-8. */
    protected static final String BULKLOAD_UTF8_CSV_FILE =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload2_utf8.csv")).getAbsolutePath();

    /** A CSV file in windows-1251. */
    protected static final String BULKLOAD_CP1251_CSV_FILE =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload2_windows1251.csv")).getAbsolutePath();

    /** A CSV file in windows-1251. */
    protected static final String BULKLOAD_RFC4180_COMMA_CSV_FILE =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload_rfc4180_comma.csv")).getAbsolutePath();

    /** A CSV file in windows-1251. */
    protected static final String BULKLOAD_RFC4180_PIPE_CSV_FILE_ =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload_rfc4180_pipe.csv")).getAbsolutePath();

    /** A CSV file with one record and unmatched quote at the start of field. */
    protected static final String BULKLOAD_ONE_LINE_CSV_FILE_UNMATCHED_QUOTE1 =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload1_unmatched1.csv")).getAbsolutePath();

    /** A CSV file with one record and unmatched quote at the end of the line. */
    protected static final String BULKLOAD_ONE_LINE_CSV_FILE_UNMATCHED_QUOTE2 =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload1_unmatched2.csv")).getAbsolutePath();

    /** A CSV file with one record and unmatched quote as the field content. */
    protected static final String BULKLOAD_ONE_LINE_CSV_FILE_UNMATCHED_QUOTE3 =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload1_unmatched3.csv")).getAbsolutePath();

    /** A CSV file with one record and unmatched quote in the unquoted field content. */
    protected static final String BULKLOAD_ONE_LINE_CSV_FILE_UNMATCHED_QUOTE4 =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload1_unmatched4.csv")).getAbsolutePath();

    /** A CSV file with one record and unmatched quote in the quoted field content. */
    protected static final String BULKLOAD_ONE_LINE_CSV_FILE_UNMATCHED_QUOTE5 =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload1_unmatched5.csv")).getAbsolutePath();

    /** A CSV file with one record and unmatched quote in the quoted field content. */
    protected static final String BULKLOAD_THREE_LINE_CSV_FILE_EMPTY_NUMERIC =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload_empty_numeric.csv")).getAbsolutePath();

    /** A CSV file with one record and unmatched quote in the quoted field content. */
    protected static final String BULKLOAD_WITH_NULL_STRING =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload_empty_numeric_with_null_string.csv")).getAbsolutePath();

    /** A CSV file with one record and unmatched quote in the quoted field content. */
    protected static final String BULKLOAD_WITH_TRIM_OFF =
            Objects.requireNonNull(resolveIgnitePath(CSV_FILE_SUBDIR + "bulkload_empty_numeric_with_trim_off.csv")).getAbsolutePath();

    /** JDBC statement. */
    protected Statement stmt;

    protected boolean isLegacyCopyEnabled;


    /**
     * Dead-on-arrival test. Imports two-entry CSV file into a table and checks
     * the created entries using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testBasicStatement() throws SQLException {
        int updatesCnt = stmt.executeUpdate(BASIC_SQL_COPY_STMT);

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Imports zero-entry CSV file into a table and checks that no entries are created
     * using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testEmptyFile() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_EMPTY_CSV_FILE + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

        assertEquals(0, updatesCnt);

        checkCacheContents(TBL_NAME, true, 0);
    }

    /**
     * Imports one-entry CSV file into a table and checks the entry created using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testOneLineFile() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_ONE_LINE_CSV_FILE + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

        assertEquals(1, updatesCnt);

        checkCacheContents(TBL_NAME, true, 1);
    }

    /**
     * Imports three-entry CSV file into a table and checks the entry created using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testThreeLineFileWithEmptyNumericColumn() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_THREE_LINE_CSV_FILE_EMPTY_NUMERIC + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

        assertEquals(3, updatesCnt);

        checkCacheContents(TBL_NAME, true, 3);
    }

    /**
     * Imports three-entry CSV file into a table and checks the entry created using SELECT statement with specified
     * null string and trim mode (ON).
     * This test verifies that specific null string values will be correctly interpreted as null and will be inserted.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testThreeLineFileWithEmptyNumericColumnWithNullString() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_WITH_NULL_STRING + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv nullstring 'a'");

        assertEquals(3, updatesCnt);

        checkCacheContents(TBL_NAME, true, 3);
    }

    /**
     * Verifies exception thrown if CSV row contains unmatched quote in end of the field content.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testOneLineFileForUnmatchedEndQuote() throws SQLException {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from '" + BULKLOAD_ONE_LINE_CSV_FILE_UNMATCHED_QUOTE2 + "' into " + TBL_NAME +
                                " (_key, age, firstName, lastName)" +
                                " format csv");

                return null;
            }
        }, SQLException.class, JdbcThinBulkLoadErrorMessageHelper.getUnmatchedQuoteFieldMessage(isLegacyCopyEnabled));

        checkCacheContents(TBL_NAME, true, 0);
    }

    /**
     * Imports three-entry CSV file into a table and checks the entry created using SELECT statement with trim OFF.
     * This test verifies that values will be inserted and whitespace in the field content is expected in this case.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testThreeLineFileWithEmptyNumericColumnWithTrimOff() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_WITH_TRIM_OFF + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv nullstring 'a' trim off");

        assertEquals(3, updatesCnt);

        checkCacheContents(TBL_NAME, true, 3);
    }

    /**
     * Verifies exception thrown if CSV row contains unmatched quote at the beginning of the field content.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testOneLineFileForUnmatchedStartQuote() throws SQLException {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from '" + BULKLOAD_ONE_LINE_CSV_FILE_UNMATCHED_QUOTE1 + "' into " + TBL_NAME +
                                " (_key, age, firstName, lastName)" +
                                " format csv");

                return null;
            }
        }, SQLException.class, JdbcThinBulkLoadErrorMessageHelper.getUnmatchedQuoteMessage(isLegacyCopyEnabled));

        checkCacheContents(TBL_NAME, true, 0);
    }

    /**
     * Verifies exception thrown if CSV row contains unmatched quote as the only field content.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testOneLineFileForSingleEndQuote() throws SQLException {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from '" + BULKLOAD_ONE_LINE_CSV_FILE_UNMATCHED_QUOTE3 + "' into " + TBL_NAME +
                                " (_key, age, firstName, lastName)" +
                                " format csv");

                return null;
            }
        }, SQLException.class, JdbcThinBulkLoadErrorMessageHelper.getUnmatchedQuoteMessage(isLegacyCopyEnabled));

        checkCacheContents(TBL_NAME, true, 0);
    }

    /**
     * Verifies exception thrown if CSV row contains single unmatched quote as the field content.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testOneLineFileForQuoteInContent() throws SQLException {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from '" + BULKLOAD_ONE_LINE_CSV_FILE_UNMATCHED_QUOTE4 + "' into " + TBL_NAME +
                                " (_key, age, firstName, lastName)" +
                                " format csv");

                return null;
            }
        }, SQLException.class, JdbcThinBulkLoadErrorMessageHelper.getUnmatchedQuoteMessage(isLegacyCopyEnabled));

        checkCacheContents(TBL_NAME, true, 0);
    }

    /**
     * Verifies exception thrown if CSV row contains unmatched quote in the quoted field content.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testOneLineFileForQuoteInQuotedContent() throws SQLException {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from '" + BULKLOAD_ONE_LINE_CSV_FILE_UNMATCHED_QUOTE5 + "' into " + TBL_NAME +
                                " (_key, age, firstName, lastName)" +
                                " format csv");

                return null;
            }
        }, SQLException.class, JdbcThinBulkLoadErrorMessageHelper.getUnmatchedQuoteFieldMessage(isLegacyCopyEnabled));

        checkCacheContents(TBL_NAME, true, 0);
    }

    /**
     * Verifies that error is reported for empty charset name.
     */
    @Test
    public void testEmptyCharset() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from 'any.file' into Person " +
                                "(_key, age, firstName, lastName) " +
                                "format csv charset ''");

                return null;
            }
        }, SQLException.class, "Unknown charset name: ''");
    }

    /**
     * Verifies that error is reported for unsupported charset name.
     */
    @Test
    public void testNotSupportedCharset() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from 'any.file' into Person " +
                                "(_key, age, firstName, lastName) " +
                                "format csv charset 'nonexistent'");

                return null;
            }
        }, SQLException.class, "Charset is not supported: 'nonexistent'");
    }

    /**
     * Verifies that error is reported for unknown charset name.
     */
    @Test
    public void testUnknownCharset() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from 'any.file' into Person " +
                                "(_key, age, firstName, lastName) " +
                                "format csv charset '8^)\'");

                return null;
            }
        }, SQLException.class, "Unknown charset name: '8^)'");
    }

    /**
     * Verifies that ASCII encoding is recognized and imported.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testAsciiCharset() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_TWO_LINES_CSV_FILE + "'" +
                        " into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv charset 'ascii'");

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Imports four-entry CSV file with default delimiter into a table and checks that
     * the entry is created using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testCsvLoadWithDefaultDelimiter() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_RFC4180_COMMA_CSV_FILE + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

        assertEquals(7, updatesCnt);

        checkCacheContents(TBL_NAME, true, 7);
    }

    /**
     * Imports four-entry CSV file with comma delimiter into a table and checks that
     * the entry is created using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testCsvLoadWithCommaDelimiter() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_RFC4180_COMMA_CSV_FILE + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv delimiter ','");

        assertEquals(7, updatesCnt);

        checkCacheContents(TBL_NAME, true, 7, ',');
    }

    /**
     * Imports four-entry CSV file with pipe delimiter into a table and checks that
     * the entry is created using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testCsvLoadWithPipeDelimiter() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_RFC4180_PIPE_CSV_FILE_ + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv delimiter '|'");

        assertEquals(7, updatesCnt);

        checkCacheContents(TBL_NAME, true, 7, '|');
    }

    /**
     * Imports two-entry CSV file with UTF-8 characters into a table and checks
     * the created entries using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testUtf8Charset() throws SQLException {
        checkBulkLoadWithCharset(BULKLOAD_UTF8_CSV_FILE, "utf-8");
    }

    /**
     * Verifies that ASCII encoding is recognized and imported.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testWin1251Charset() throws SQLException {
        checkBulkLoadWithCharset(BULKLOAD_CP1251_CSV_FILE, "windows-1251");
    }

    /**
     * Bulk-loads specified file specifying charset in the command
     * and verifies the entries imported.
     *
     * @param fileName CSV file to load.
     * @param charsetName Charset name to specify in the command.
     * @throws SQLException If failed.
     */
    private void checkBulkLoadWithCharset(String fileName, String charsetName) throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + fileName + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv charset '" + charsetName + "'");

        assertEquals(2, updatesCnt);

        checkNationalCacheContents(TBL_NAME);
    }

    /**
     * Checks that bulk load works when we use packet size of 1 byte and thus
     * create multiple packets per COPY.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testPacketSize_1() throws SQLException {
        int updatesCnt = stmt.executeUpdate(BASIC_SQL_COPY_STMT + " packet_size 1");

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Imports two-entry CSV file with UTF-8 characters into a table and checks
     * the created entries using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDefaultCharset() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_UTF8_CSV_FILE + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv");

        assertEquals(2, updatesCnt);

        checkNationalCacheContents(TBL_NAME);
    }

    /**
     * Checks cache contents after bulk loading data in the above tests:
     * national charset version.
     * <p>
     * Uses SQL SELECT command for querying entries.
     *
     * @param tblName Table name to query.
     * @throws SQLException When one of checks has failed.
     */
    protected void checkNationalCacheContents(String tblName) throws SQLException {
        checkRecodedNationalCacheContents(tblName, null, null);
    }

    /**
     * Checks cache contents after bulk loading data in the tests:
     * normal and erroneously recoded national charset version.
     * <p>
     * Uses SQL SELECT command for querying entries.
     *
     * @param tblName Table name to query.
     * @param csvCharsetName Either null or the charset used in CSV file
     *      Note that the both {@code csvCharsetName} and {@code stmtCharsetName} should be either null or non-null.
     * @param stmtCharsetName Either null or the charset specified in COPY statement.
     * @throws SQLException When one of checks has failed.
     */
    protected void checkRecodedNationalCacheContents(String tblName,
                                                   String csvCharsetName, String stmtCharsetName) throws SQLException {
        assert (csvCharsetName != null) == (stmtCharsetName != null);

        ResultSet rs = stmt.executeQuery("select _key, age, firstName, lastName from " + tblName);

        assert rs != null;

        IgniteClosure<String, String> recoder =
                (csvCharsetName != null)
                        ? new WrongCharsetRecoder(csvCharsetName, stmtCharsetName)
                        : new IgniteClosure<String, String>() {
                    @Override public String apply(String input) {
                        return input;
                    }
                };

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("_key");

            if (id == 123) {
                assertEquals(12, rs.getInt("age"));

                assertEquals(recoder.apply("Имя123 Отчество123"), rs.getString("firstName"));

                assertEquals(recoder.apply("Фамилия123"), rs.getString("lastName"));
            }
            else if (id == 456) {
                assertEquals(45, rs.getInt("age"));

                assertEquals(recoder.apply("Имя456"), rs.getString("firstName"));

                assertEquals(recoder.apply("Фамилия456"), rs.getString("lastName"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(2, cnt);
    }

    /**
     * Recodes an input string as if it was encoded in one charset and was read using
     * another charset using {@link CodingErrorAction#REPLACE} settings for
     * unmappable and malformed characters.
     */
    private static class WrongCharsetRecoder implements IgniteClosure<String, String> {
        /** Charset in which the string we are reading is actually encoded. */
        private final Charset actualCharset;

        /** Charset which we use to read the string. */
        private final Charset appliedCharset;

        /**
         * Creates the recoder.
         *
         * @param actualCharset Charset in which the string we are reading is actually encoded.
         * @param appliedCharset Charset which we use to read the string.
         * @throws UnsupportedCharsetException if the charset name is wrong.
         */
        WrongCharsetRecoder(String actualCharset, String appliedCharset) {
            this.actualCharset = Charset.forName(actualCharset);
            this.appliedCharset = Charset.forName(appliedCharset);
        }

        /**
         * Converts string as it was read using a wrong charset.
         * <p>
         * First the method converts the string into {@link #actualCharset} and puts bytes into a buffer.
         * Then it tries to read these bytes from the buffer using {@link #appliedCharset} and
         * {@link CodingErrorAction#REPLACE} settings for unmappable and malformed characters
         * (NB: these settings implicitly come from {@link Charset#decode(ByteBuffer)} implementation, while
         * being explicitly set in {@link BulkLoadCsvParser#BulkLoadCsvParser(BulkLoadCsvFormat)}).
         *
         * @param input The input string (in Java encoding).
         * @return The converted string.
         */
        @Override public String apply(String input) {
            ByteBuffer encodedBuf = actualCharset.encode(input);

            return appliedCharset.decode(encodedBuf).toString();
        }
    }

    /**
     * Imports two-entry CSV file with UTF-8 characters into a table using packet size of one byte
     * (thus splitting each two-byte UTF-8 character into two packets)
     * and checks the created entries using SELECT statement.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testDefaultCharsetPacketSize1() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_UTF8_CSV_FILE + "' into " + TBL_NAME +
                        " (_key, age, firstName, lastName)" +
                        " format csv packet_size 1");

        assertEquals(2, updatesCnt);

        checkNationalCacheContents(TBL_NAME);
    }

    /**
     * Checks that error is reported for a non-existent file.
     */
    @Test
    public void testWrongFileName() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from 'nonexistent' into Person" +
                                " (_key, age, firstName, lastName)" +
                                " format csv");

                return null;
            }
        }, SQLException.class, JdbcThinBulkLoadErrorMessageHelper.getWrongFileNameMessage(isLegacyCopyEnabled));
    }

    /**
     * Checks that error is reported if the destination table is missing.
     */
    @Test
    public void testMissingTable() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from '" + BULKLOAD_TWO_LINES_CSV_FILE + "' into Peterson" +
                                " (_key, age, firstName, lastName)" +
                                " format csv");

                return null;
            }
        }, SQLException.class, "Table does not exist: PETERSON");
    }

    /**
     * Checks that error is reported when a non-existing column is specified in the SQL command.
     */
    @Test
    public void testWrongColumnName() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from '" + BULKLOAD_TWO_LINES_CSV_FILE + "' into Person" +
                                " (_key, age, firstName, lostName)" +
                                " format csv");

                return null;
            }
        }, SQLException.class, "Column \"LOSTNAME\" not found");
    }

    /**
     * Checks that error is reported if field read from CSV file cannot be converted to the type of the column.
     */
    @Test
    public void testWrongColumnType() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                        "copy from '" + BULKLOAD_TWO_LINES_CSV_FILE + "' into Person" +
                                " (_key, firstName, age, lastName)" +
                                " format csv");

                return null;
            }
        }, SQLException.class, "Value conversion failed [column=AGE, from=java.lang.String, to=java.lang.Integer]");
    }

    /**
     * Checks that if even a subset of fields is imported, the imported fields are set correctly.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testFieldsSubset() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
                "copy from '" + BULKLOAD_TWO_LINES_CSV_FILE + "'" +
                        " into " + TBL_NAME +
                        " (_key, age, firstName)" +
                        " format csv");

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, false, 2);
    }

    /**
     * Checks that bulk load works when we create table using 'CREATE TABLE' command.
     * <p>
     * The majority of the tests in this class use {@link CacheConfiguration#setIndexedTypes(Class[])}
     * to create the table.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testCreateAndBulkLoadTable() throws SQLException {
        String tblName = QueryUtils.DFLT_SCHEMA + ".\"PersonTbl\"";

        execute(conn, "create table " + tblName +
                " (id int primary key, age int, firstName varchar(30), lastName varchar(30))");

        try {
            int updatesCnt = stmt.executeUpdate(
                    "copy from '" + BULKLOAD_TWO_LINES_CSV_FILE + "' into " + tblName +
                            "(_key, age, firstName, lastName)" +
                            " format csv");

            assertEquals(2, updatesCnt);

            checkCacheContents(tblName, true, 2);
        }
        finally {
            execute(conn, "drop table " + tblName);
        }
    }

    /**
     * Verifies that COPY command is rejected by Statement.executeQuery().
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testExecuteQuery() throws SQLException {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeQuery(BASIC_SQL_COPY_STMT);

                return null;
            }
        }, SQLException.class, "Given statement type does not match that declared by JDBC driver");
    }

    /**
     * Verifies that COPY command works in Statement.execute().
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testExecute() throws SQLException {
        boolean isRowSet = stmt.execute(BASIC_SQL_COPY_STMT);

        assertFalse(isRowSet);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Verifies that COPY command can be called with PreparedStatement.executeUpdate().
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testPreparedStatementWithExecuteUpdate() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(BASIC_SQL_COPY_STMT);

        int updatesCnt = pstmt.executeUpdate();

        assertEquals(2, updatesCnt);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Verifies that COPY command reports an error when used with PreparedStatement parameter.
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testPreparedStatementWithParameter() throws SQLException {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                PreparedStatement pstmt = conn.prepareStatement(
                        "copy from '" + BULKLOAD_TWO_LINES_CSV_FILE + "' into " + TBL_NAME +
                                " (_key, age, firstName, lastName)" +
                                " format ?");

                pstmt.setString(1, "csv");

                pstmt.executeUpdate();

                return null;
            }
        }, SQLException.class, "Unexpected token: \"?\" (expected: \"[identifier]\"");
    }

    /**
     * Verifies that COPY command can be called with PreparedStatement.execute().
     *
     * @throws SQLException If failed.
     */
    @Test
    public void testPreparedStatementWithExecute() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(BASIC_SQL_COPY_STMT);

        boolean isRowSet = pstmt.execute();

        assertFalse(isRowSet);

        checkCacheContents(TBL_NAME, true, 2);
    }

    /**
     * Verifies that COPY command is rejected by PreparedStatement.executeQuery().
     */
    @Test
    public void testPreparedStatementWithExecuteQuery() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                PreparedStatement pstmt = conn.prepareStatement(BASIC_SQL_COPY_STMT);

                pstmt.executeQuery();

                return null;
            }
        }, SQLException.class, "Given statement type does not match that declared by JDBC driver");
    }

    /**
     * Checks cache contents after bulk loading data in the above tests: ASCII version.
     * <p>
     * Uses SQL SELECT command for querying entries.
     *
     * @param tblName Table name to query.
     * @param checkLastName Check 'lastName' column (not imported in some tests).
     * @param recCnt Number of records to expect.
     * @throws SQLException When one of checks has failed.
     */
    private void checkCacheContents(String tblName, boolean checkLastName, int recCnt) throws SQLException {
        checkCacheContents(tblName, checkLastName, recCnt, ',');
    }

    /**
     * Checks cache contents after bulk loading data in the above tests: ASCII version.
     * <p>
     * Uses SQL SELECT command for querying entries.
     *
     * @param tblName Table name to query.
     * @param checkLastName Check 'lastName' column (not imported in some tests).
     * @param recCnt Number of records to expect.
     * @param delimiter The delimiter of fields.
     * @throws SQLException When one of checks has failed.
     */
    private void checkCacheContents(String tblName, boolean checkLastName, int recCnt, char delimiter) throws SQLException {
        ResultSet rs = stmt.executeQuery("select _key, age, firstName, lastName from " + tblName);

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("_key");

            SyntheticPerson sp = new SyntheticPerson(rs.getInt("age"),
                    rs.getString("firstName"), rs.getString("lastName"));

            if (id == 101)
                sp.validateValues(0, "FirstName101 MiddleName101", "LastName101", checkLastName);
            else if (id == 102)
                sp.validateValues(0, "FirstName102 MiddleName102", "LastName102", checkLastName);
            else if (id == 103)
                sp.validateValues(0, "FirstName103 MiddleName103", "LastName103", checkLastName);
            else if (id == 104)
                sp.validateValues(0, " FirstName104 MiddleName104", "LastName104", checkLastName);
            else if (id == 123)
                sp.validateValues(12, "FirstName123 MiddleName123", "LastName123", checkLastName);
            else if (id == 234)
                sp.validateValues(23, "FirstName|234", null, checkLastName);
            else if (id == 345)
                sp.validateValues(34, "FirstName,345", null, checkLastName);
            else if (id == 456)
                sp.validateValues(45, "FirstName456", "LastName456", checkLastName);
            else if (id == 567)
                sp.validateValues(56, null, null, checkLastName);
            else if (id == 678)
                sp.validateValues(67, null, null, checkLastName);
            else if (id == 789)
                sp.validateValues(78, "FirstName789 plus \"quoted\"", "LastName 789", checkLastName);
            else if (id == 101112)
                sp.validateValues(1011, "FirstName 101112",
                        "LastName\"" + delimiter + "\" 1011" + delimiter + " 12", checkLastName);
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(recCnt, cnt);
    }

    /**
     *
     */
    protected class SyntheticPerson {
        /** */
        int age;

        /** */
        String firstName;

        /** */
        String lastName;

        /** */
        public SyntheticPerson(int age, String firstName, String lastName) {
            this.age = age;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        /** */
        public void validateValues(int age, String firstName, String lastName, boolean checkLastName) {
            assertEquals(age, this.age);
            assertEquals(firstName, this.firstName);
            if (checkLastName)
                assertEquals(lastName, this.lastName);
        }
    }
}
