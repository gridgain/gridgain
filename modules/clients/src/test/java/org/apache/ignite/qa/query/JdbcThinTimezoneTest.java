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
import java.util.List;
import java.util.TimeZone;
import org.apache.ignite.lang.IgniteCallable;
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

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sql("CREATE TABLE TZ_TEST (" +
            "tz VARCHAR, " +
            "label VARCHAR, " +
            "dateVal DATE, " +
            "timeVal TIME, " +
            "tsVal TIMESTAMP, " +
            "PRIMARY KEY (tz, label))");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (String cache : grid(0).cacheNames())
            grid(0).cache(cache).destroy();

        super.afterTest();
    }

    /**
     */
    @Test
    public void test() throws Exception {
        insert(TimeZone.getTimeZone("EST5EDT"));
        insert(TimeZone.getTimeZone("IST"));
        insert(TimeZone.getTimeZone("Europe/Moscow"));

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
    private void insert(final TimeZone tz) throws Exception {
        TestJavaProcess.exec((GridTestUtils.IgniteRunnableX)() -> {
            sql("INSERT INTO TZ_TEST (tz, label, dateVal, timeVal, tsVal) " +
                "VALUES (?, 'legacy', ?, ?, ?)",
                tz.getID(),
                new Date(119, 8, 9),
                new Time(9, 9, 9),
                new Timestamp(119, 8, 9, 9, 9, 9, 90900000)
            );
        }, "-Duser.timezone=" + tz.getID());

    }

    /**
     */
    private List<List<?>> execSql(TimeZone tz, String sql, Object ... params) throws Exception {
        return TestJavaProcess.exec((IgniteCallable<List<List<?>>>)() -> {
            return sql(sql, params);
        }, "-Duser.timezone=" + tz.getID());
    }
}
