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

package org.apache.ignite.qa.query;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.TestJavaProcess;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.jdbc.JdbcTestUtils.sql;

/**
 * Checks JDBC thin client on different timezones.
 */
public class JdbcThinTimezoneTest extends AbstractIndexingCommonTest {
    /** Jdbc thin url. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** Jdbc thin url. */
    private static final String URL_TZ_DISABLE = "jdbc:ignite:thin://127.0.0.1/?disabledFeatures=time_zone";

    /** Time zones to check. */
    private static final String[] TIME_ZONES = {"EST5EDT", "IST", "Europe/Moscow"};

    /** Time zones to check. */
    private static final String[] NODE_TIME_ZONES = {"CST", "EST"};

    /** Time zone ID for other JVM to start remote grid. */
    private String tzId;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrid(0);

        for (String nodeTz : NODE_TIME_ZONES)
            startRemoteGrid("node-" + nodeTz, nodeTz);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        sql(URL, "DROP TABLE IF EXISTS TZ_TEST", Collections.emptyList());

        sql(URL, "CREATE TABLE IF NOT EXISTS TZ_TEST (" +
            "tz VARCHAR, " +
            "label VARCHAR, " +
            "dateVal DATE, " +
            "timeVal TIME, " +
            "tsVal TIMESTAMP, " +
            "PRIMARY KEY (tz, label))", Collections.emptyList());
    }

    /** */
    @Test
    public void testEnableTimezone() throws Exception {
        for (String tz : TIME_ZONES) {
            insertObjectByLegacyApi(URL, TimeZone.getTimeZone(tz));
            insertObjectByModernApi(URL, TimeZone.getTimeZone(tz));
            insertLiteral(URL, TimeZone.getTimeZone(tz));
        }

        checkResults(selectResultsForAllTimeZones(
            "SELECT tz, label, dateVal, timeVal, tsVal FROM TZ_TEST", true));

        checkResults(selectResultsForAllTimeZones(
            "SELECT tz, label, " +
                "CAST(dateVal AS VARCHAR), " +
                "CAST(timeVal AS VARCHAR), " +
                "CAST(tsVal AS VARCHAR) " +
                "FROM TZ_TEST", false));
    }

    /** */
    @Test
    public void testSelectWithConditions() throws Exception {
        for (String tz : TIME_ZONES) {
            insertObjectByLegacyApi(URL, TimeZone.getTimeZone(tz));
            insertObjectByModernApi(URL, TimeZone.getTimeZone(tz));
            insertLiteral(URL, TimeZone.getTimeZone(tz));
        }

        checkResults(selectResultsForAllTimeZones(
            "SELECT tz, label, dateVal, timeVal, tsVal FROM TZ_TEST " +
                "WHERE " +
                "dateVal = CAST('2019-09-09' AS DATE) " +
                "AND timeVal = CAST('09:09:09' AS TIME) " +
                "AND tsVal = CAST('2019-09-09 09:09:09.909' AS TIMESTAMP)", true));

        Map<String, List<String>> resMap = new HashMap<>();

        for (String tz : TIME_ZONES) {
            List<String> res = selectWithCondition(TimeZone.getTimeZone(tz));

            res.sort(String::compareTo);

            resMap.put(tz, res);
        }

        checkResults(resMap);
    }

    /** */
    @Test
    public void testDisableTimezone() throws Exception {
        for (String tz : TIME_ZONES) {
            insertObjectByLegacyApi(URL_TZ_DISABLE, TimeZone.getTimeZone(tz));
            insertObjectByModernApi(URL_TZ_DISABLE, TimeZone.getTimeZone(tz));
            insertLiteral(URL_TZ_DISABLE, TimeZone.getTimeZone(tz));
        }

        Map<String, List<String>> resMap = new HashMap<>();

        for (String tz : TIME_ZONES) {
            List<String> res = selectAndPrintMilliseconds("SELECT tz, label, dateVal, timeVal, tsVal FROM TZ_TEST",
                TimeZone.getTimeZone(tz));

            res.sort(String::compareTo);

            resMap.put(tz, res);
        }

        checkResults(resMap);
    }

    /** */
    @NotNull private Map<String, List<String>> selectResultsForAllTimeZones(String sql, boolean fetchDateObjs) throws Exception {
        Map<String, List<String>> resMap = new HashMap<>();

        for (String tz : TIME_ZONES) {
            List<String> res = select(sql, TimeZone.getTimeZone(tz), fetchDateObjs);

            res.sort(String::compareTo);

            resMap.put(tz, res);
        }

        return resMap;
    }

    /** */
    private void checkResults(Map<String, List<String>> resMap) {
        for (String tz : TIME_ZONES) {
            final List<String> res = resMap.get(tz);

            assertFalse(res.isEmpty());

            resMap.forEach((key, value) -> {
                if (!value.equals(res)) {
                    for (String s : res)
                        System.err.println(tz + ", " + s);

                    for (String s : value)
                        System.err.println(key + ", " + s);

                    fail("Not equal select result for date/time fields. Need investigate");
                }
            });
        }
    }

    /**
     * @param tz Default timezone for process.
     */
    private void insertObjectByLegacyApi(String url, final TimeZone tz) throws Exception {
        TestJavaProcess.exec((GridTestUtils.IgniteRunnableX)() -> {
            sql(url, "INSERT INTO TZ_TEST (tz, label, dateVal, timeVal, tsVal) " +
                    "VALUES (?, 'obj_legacy', ?, ?, ?)",
                Arrays.asList(
                    tz.getID(),
                    new Date(119, 8, 9),
                    new Time(9, 9, 9),
                    new Timestamp(119, 8, 9, 9, 9, 9, 909000000)
                )
            );
        }, "-Duser.timezone=" + tz.getID());
    }

    /**
     * @param tz Default timezone for process.
     */
    private void insertObjectByModernApi(String url, final TimeZone tz) throws Exception {
        TestJavaProcess.exec((GridTestUtils.IgniteRunnableX)() -> {
            Calendar dateCal = Calendar.getInstance();
            dateCal.set(2019, 8, 9);

            Calendar timeCal = Calendar.getInstance();
            timeCal.set(Calendar.HOUR_OF_DAY, 9);
            timeCal.set(Calendar.MINUTE, 9);
            timeCal.set(Calendar.SECOND, 9);
            timeCal.set(Calendar.MILLISECOND, 0);

            Calendar tsCal = Calendar.getInstance();
            tsCal.set(2019, 8, 9, 9, 9, 9);
            tsCal.set(Calendar.MILLISECOND, 909);

            sql(url, "INSERT INTO TZ_TEST (tz, label, dateVal, timeVal, tsVal) " +
                    "VALUES (?, 'obj_modern', ?, ?, ?)",
                Arrays.asList(
                    tz.getID(),
                    new Date(dateCal.getTimeInMillis()),
                    new Time(timeCal.getTimeInMillis()),
                    new Timestamp(tsCal.getTimeInMillis())
                )
            );
        }, "-Duser.timezone=" + tz.getID());
    }

    /**
     * @param tz Default timezone for process.
     */
    private void insertLiteral(String url, final TimeZone tz) throws Exception {
        TestJavaProcess.exec((GridTestUtils.IgniteRunnableX)() -> {

            sql(url, "INSERT INTO TZ_TEST (tz, label, dateVal, timeVal, tsVal) " +
                    "VALUES (?, 'literal', CAST(? AS DATE), CAST(? AS TIME), CAST(? AS TIMESTAMP))",
                Arrays.asList(
                    tz.getID(),
                    "2019-09-09",
                    "09:09:09",
                    "2019-09-09 09:09:09.909"
                )
            );
        }, "-Duser.timezone=" + tz.getID());
    }

    /**
     */
    private List<String> select(final String sql, final TimeZone tz, boolean fetchDateObjs) throws Exception {
        return TestJavaProcess.exec((IgniteCallable<List<String>>)() -> {
            List<String> res = new ArrayList<>();

            try (Connection conn = DriverManager.getConnection(URL)) {
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.execute();

                    ResultSet rs = pstmt.getResultSet();

                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();

                        row.add(rs.getString(1));
                        row.add(rs.getString(2));

                        if (fetchDateObjs) {
                            row.add(rs.getDate(3));
                            row.add(rs.getTime(4));
                            row.add(rs.getTimestamp(5));
                        }

                        row.add(rs.getObject(3));
                        row.add(rs.getObject(4));
                        row.add(rs.getObject(5));

                        res.add(row.toString());
                    }

                    return res;
                }
            }
        }, "-Duser.timezone=" + tz.getID());
    }

    /**
     */
    private List<String> selectWithCondition(final TimeZone tz) throws Exception {
        return TestJavaProcess.exec((IgniteCallable<List<String>>)() -> {
            List<String> res = new ArrayList<>();

            try (Connection conn = DriverManager.getConnection(URL)) {
                try (PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT tz, label, dateVal, timeVal, tsVal FROM TZ_TEST " +
                    "WHERE dateVal = ? AND timeVal = ? AND tsVal = ?")) {
                    pstmt.setObject(1, new Date(119, 8, 9));
                    pstmt.setObject(2, new Time(9, 9, 9));
                    pstmt.setObject(3,
                        new Timestamp(119, 8, 9, 9, 9, 9, 909000000));

                    pstmt.execute();

                    ResultSet rs = pstmt.getResultSet();

                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();

                        row.add(rs.getString(1));
                        row.add(rs.getString(2));

                        row.add(rs.getDate(3));
                        row.add(rs.getTime(4));
                        row.add(rs.getTimestamp(5));

                        row.add(rs.getObject(3));
                        row.add(rs.getObject(4));
                        row.add(rs.getObject(5));

                        res.add(row.toString());
                    }

                    return res;
                }
            }
        }, "-Duser.timezone=" + tz.getID());
    }

    /**
     */
    private List<String> selectAndPrintMilliseconds(final String sql, final TimeZone tz) throws Exception {
        return TestJavaProcess.exec((IgniteCallable<List<String>>)() -> {
            List<String> res = new ArrayList<>();

            try (Connection conn = DriverManager.getConnection(URL_TZ_DISABLE)) {
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.execute();

                    ResultSet rs = pstmt.getResultSet();

                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();

                        row.add(rs.getString(1));
                        row.add(rs.getString(2));

                        row.add(rs.getDate(3).getTime());
                        row.add(rs.getTime(4).getTime());
                        row.add(rs.getTimestamp(5).getTime());

                        res.add(row.toString());
                    }

                    return res;
                }
            }
        }, "-Duser.timezone=" + tz.getID());
    }

    /**
     * @return Additional JVM args for remote instances.
     */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Collections.singletonList("-Duser.timezone=" + tzId);
    }

    /**
     * @param name Remote node name.
     * @param tzId Default time zone for the node.
     */
    private void startRemoteGrid(String name, String tzId) throws Exception {
        this.tzId = tzId;

        startRemoteGrid(name, optimize(getConfiguration(name)), null);
    }
}
