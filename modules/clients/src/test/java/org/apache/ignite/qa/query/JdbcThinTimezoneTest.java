/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
import java.util.List;
import java.util.TimeZone;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.TestJavaProcess;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.jdbc.JdbcTestUtils.sql;

/**
 * Thin client authorization with Native Ignite authentication tests.
 */
public class JdbcThinTimezoneTest extends GridCommonAbstractTest {
    /** Jdbc thin url. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";
//    private static final String URL = "jdbc:postgresql://localhost/test?user=test&password=test";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrids(3);
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

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {

        super.afterTest();
    }

    @Test
    public void testDbg() throws Exception {
//        TestJavaProcess.exec((GridTestUtils.IgniteRunnableX)() -> {
//            sql(URL, "INSERT INTO TZ_TEST (tz, label, tsVal) " +
//                    "VALUES (?, 'obj_legacy', ?)",
//                Arrays.asList(
//                    "IST",
//                    new Timestamp(119, 8, 9, 9, 9, 9, 909000000)
//                )
//            );
//        }, "-Duser.timezone=IST");
        TestJavaProcess.exec((GridTestUtils.IgniteRunnableX)() -> {
            sql(URL, "INSERT INTO TZ_TEST (tz, label, tsVal) " +
                    "VALUES (?, 'literal', ?)",
                Arrays.asList(
                    "IST",
                    "2019-09-09 09:09:09.909"
                )
            );
        }, "-Duser.timezone=IST");

        TestJavaProcess.exec((GridTestUtils.IgniteRunnableX)() -> {
        try (Connection conn = DriverManager.getConnection(URL)) {
            try (PreparedStatement pstmt = conn.prepareStatement(
                "SELECT tz, label, dateVal, timeVal, tsVal FROM TZ_TEST")) {
                pstmt.execute();

                ResultSet rs = pstmt.getResultSet();

                while (rs.next()) {
                    List<Object> row = new ArrayList<>();

                    row.add(rs.getString(1));
                    row.add(rs.getString(2));
                    row.add(rs.getDate(3));
                    row.add(rs.getObject(3));
                    row.add(rs.getTime(4));
                    row.add(rs.getObject(4));
                    row.add(rs.getTimestamp(5));
                    row.add(rs.getObject(5));

                    System.out.println("+++ " + row);
                }

            }
        }
        }, "-Duser.timezone=IST");
    }
        /**
         *
         */
    @Test
    public void test() throws Exception {
        insertObjectByLegacyApi(TimeZone.getTimeZone("EST5EDT"));
        insertObjectByLegacyApi(TimeZone.getTimeZone("IST"));
        insertObjectByLegacyApi(TimeZone.getTimeZone("Europe/Moscow"));
        insertObjectByModernApi(TimeZone.getTimeZone("EST5EDT"));
        insertObjectByModernApi(TimeZone.getTimeZone("IST"));
        insertObjectByModernApi(TimeZone.getTimeZone("Europe/Moscow"));

        List<List<?>> res = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(URL)) {
            try (PreparedStatement pstmt = conn.prepareStatement(
                "SELECT tz, label, dateVal, timeVal, tsVal FROM TZ_TEST")) {
                pstmt.execute();

                ResultSet rs = pstmt.getResultSet();

                while (rs.next()) {
                    List<Object> row = new ArrayList<>();

                    row.add(rs.getString(1));
                    row.add(rs.getString(2));
                    row.add(rs.getDate(3));
                    row.add(rs.getObject(3));
                    row.add(rs.getTime(4));
                    row.add(rs.getObject(4));
                    row.add(rs.getTimestamp(5));
                    row.add(rs.getObject(5));

                    System.out.println("+++ " + row);
                }

            }
        }

//        System.out.println("+++ " + sql("select * from TZ_TEST"));
    }

    /**
     * @param tz Default timezone for process.
     */
    private void insertObjectByLegacyApi(final TimeZone tz) throws Exception {
        TestJavaProcess.exec((GridTestUtils.IgniteRunnableX)() -> {
            sql(URL, "INSERT INTO TZ_TEST (tz, label, dateVal, timeVal, tsVal) " +
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
    private void insertObjectByModernApi(final TimeZone tz) throws Exception {
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

            sql(URL, "INSERT INTO TZ_TEST (tz, label, dateVal, timeVal, tsVal) " +
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

}
