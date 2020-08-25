/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.h2.test.utils.Utils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.h2.test.utils.Utils.sql;
import static org.junit.Assert.assertTrue;

/**
 * A test that runs random join statements against two databases and compares the results.
 */
public class TestChooseIndexWithOrderBy {
    private static final int ROWS_CNT = 100;

    private static Connection conn;

    /**
     * @throws SQLException On error.
     */
    @BeforeClass
    public static void init() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:idx_order_by");
        sql(conn, "DROP TABLE IF EXISTS TEST");

        sql(conn, "CREATE TABLE TEST (ID INT PRIMARY KEY, VAL0 INT, VAL1 INT, VAL2 VARCHAR)");
        sql(conn, "CREATE INDEX IDX_VAL0 ON TEST(VAL0, ID)");
        sql(conn, "CREATE INDEX IDX_VAL0_VAL1 ON TEST(VAL0, VAL1, ID)");

        for (int i = 0; i < ROWS_CNT; ++i) {
            sql(conn,
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
        sql(conn, "DROP TABLE IF EXISTS TEST");

        conn.close();
    }

    /**
     * Check query plan. HASH_JOIN_IDX index must be chosen.
     * @throws Exception On error.
     */
    @Test
    public void test() throws Exception {
        String plan = null;

        plan = Utils.sqlStr(conn, "EXPLAIN SELECT VAL0, VAL1, VAL2 FROM TEST " +
            "WHERE VAL0 = 0 " +
            "ORDER BY VAL0, VAL1");
        assertTrue("Unexpected plan: " + plan, plan.contains("IDX_VAL0_VAL1"));

        plan = Utils.sqlStr(conn, "EXPLAIN SELECT VAL0, VAL1, VAL2 FROM TEST " +
            "WHERE VAL0 = 0 " +
            "ORDER BY 1, 2");
        assertTrue("Unexpected plan: " + plan, plan.contains("IDX_VAL0_VAL1"));
    }
}
