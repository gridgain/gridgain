/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.h2.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * A test that runs random join statements against two databases and compares the results.
 */
public class TestChooseIndexWithOrderBy {
    private static final int ROWS_CNT = 100;

    private static Connection connection;

    /**
     * @throws SQLException On error.
     */
    @BeforeClass
    public static void init() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:mem:idx_order_by");
        sql("DROP TABLE IF EXISTS TEST");

        sql("CREATE TABLE TEST (ID INT PRIMARY KEY, VAL0 INT, VAL1 INT, VAL2 VARCHAR)");
        sql("CREATE INDEX IDX_VAL0 ON TEST(VAL0, ID)");
        sql("CREATE INDEX IDX_VAL0_VAL1 ON TEST(VAL0, VAL1, ID)");

        for (int i = 0; i < ROWS_CNT; ++i) {
            sql(
                "INSERT INTO TEST (ID, VAL0, VAL1, VAL2) VALUES (?, ?, ?, ?)",
                i,
                i / 10,
                i,
                "val" + i);
        }
    }

    /**
     * @throws SQLException On error.
     */
    @AfterClass
    public static void cleanup() throws SQLException {
        sql("DROP TABLE IF EXISTS TEST");

        connection.close();
    }

    /**
     * Check query plan. HASH_JOIN_IDX index must be chosen.
     * @throws Exception On error.
     */
    @Test
    public void test() throws Exception {
        String plan = null;

        plan = sqlStr("EXPLAIN SELECT VAL0, VAL1, VAL2 FROM TEST " +
            "WHERE VAL0 = 0 " +
            "ORDER BY VAL0, VAL1");
        assertTrue("Unexpected plan: " + plan, plan.contains("IDX_VAL0_VAL1"));

        plan = sqlStr("EXPLAIN SELECT VAL0, VAL1, VAL2 FROM TEST " +
            "WHERE VAL0 = 0 " +
            "ORDER BY 1, 2");
        assertTrue("Unexpected plan: " + plan, plan.contains("IDX_VAL0_VAL1"));
    }

    /**
     * @param sql SQL query.
     * @param params Parameters.
     * @throws SQLException On error.
     * @return Result set or updated count are printed to string.
     */
    private static String sqlStr(String sql, Object... params) throws SQLException {
        try (PreparedStatement prep = connection.prepareStatement(sql)) {
            for (int j = 0; j < params.length; j++)
                prep.setObject(j + 1, params[j]);

            if (prep.execute()) {
                ResultSet rs = prep.getResultSet();

                return readResult(rs);
            }
            else
                return "UPD: " + prep.getUpdateCount();
        }
    }

    /**
     * @param sql SQL query.
     * @param params Parameters.
     * @throws SQLException On error.
     * @return Result set or updated count are printed to string.
     */
    private static List<List<Object>> sql(String sql, Object... params) throws SQLException {
        try (PreparedStatement prep = connection.prepareStatement(sql)) {
            for (int j = 0; j < params.length; j++)
                prep.setObject(j + 1, params[j]);

            if (prep.execute()) {
                ResultSet rs = prep.getResultSet();

                int colCnt = rs.getMetaData().getColumnCount();

                List<List<Object>> res = new ArrayList<>();

                while (rs.next()) {
                    List<Object> row = new ArrayList<>(colCnt);

                    for (int i = 0; i < colCnt; i++)
                        row.add(rs.getObject(i + 1));

                    res.add(row);
                }

                return res;
            }
            else
                return Collections.singletonList(Collections.singletonList((Object)prep.getUpdateCount()));
        }
    }

    /**
     * @param rs Result set.
     * @return Result set printed to string.
     * @throws SQLException On error.
     */
    private static String readResult(ResultSet rs) throws SQLException {
        StringBuilder b = new StringBuilder();

        ResultSetMetaData meta = rs.getMetaData();

        int columnCount = meta.getColumnCount();

        for (int i = 0; i < columnCount; i++) {
            if (i > 0)
                b.append(",");

            b.append(StringUtils.toUpperEnglish(meta.getColumnLabel(i + 1)));
        }

        b.append(":\n");

        String result = b.toString();

        ArrayList<String> list = new ArrayList<>();

        while (rs.next()) {
            b = new StringBuilder();

            for (int i = 0; i < columnCount; i++) {
                if (i > 0)
                    b.append(",");

                b.append(rs.getString(i + 1));
            }

            list.add(b.toString());
        }
        Collections.sort(list);

        for (int i = 0; i < list.size(); i++)
            result += list.get(i) + "\n";

        return result;
    }
}
