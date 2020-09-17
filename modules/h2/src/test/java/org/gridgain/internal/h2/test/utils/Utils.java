/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.test.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.gridgain.internal.h2.util.StringUtils;

/**
 * Test utilities functions.
 */
public class Utils {
    /**
     * Close constructor.
     */
    private void TestUtils() {
        // No-op.
    }

    /**
     * @param c Connection.
     * @param sql SQL query.
     * @param params Parameters.
     * @throws SQLException On error.
     * @return Result set or updated count are printed to string.
     */
    public static String sqlStr(Connection c, String sql, Object... params) throws SQLException {
        try (PreparedStatement prep = c.prepareStatement(sql)) {
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
     * @param c Connection.
     * @param sql SQL query.
     * @param params Parameters.
     * @throws SQLException On error.
     * @return Result set or updated count are printed to string.
     */
    public static List<List<Object>> sql(Connection c, String sql, Object... params) throws SQLException {
        try (PreparedStatement prep = c.prepareStatement(sql)) {
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
