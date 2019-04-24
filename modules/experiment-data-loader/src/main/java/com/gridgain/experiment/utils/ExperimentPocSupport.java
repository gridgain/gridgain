package com.gridgain.experiment.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class ExperimentPocSupport {

    private static final int FETCH_SIZE = 1000;

    private final static AtomicLong hiveOpsCount = new AtomicLong(0);

    private final static AtomicLong igniteOpsCount = new AtomicLong(0);

    private static Map<Long, QueryMetrics> executedHiveQueries = new ConcurrentHashMap<>();

    private static Map<Long, QueryMetrics> executedIgniteQueries = new ConcurrentHashMap<>();

    public static ResultSet executeHiveQuery(Connection con, String sql) {
        ResultSet res = null;

        try {
            Statement stmt = con.createStatement();

            stmt.setFetchSize(FETCH_SIZE);

            long start = System.currentTimeMillis();
            res = stmt.executeQuery(sql);
            long time = System.currentTimeMillis() - start;

            final long opsCount = hiveOpsCount.incrementAndGet();
            executedHiveQueries.put(opsCount, new QueryMetrics(sql, time));

            System.out.println("Hive query #" + opsCount + " execution time : " + time + " ms.");
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return res;
    }

    public static boolean executeHiveStmt(Connection con, String sql) {
        boolean res = false;

        try {
            Statement stmt = con.createStatement();

            long start = System.currentTimeMillis();
            res = stmt.execute(sql);
            long time = System.currentTimeMillis() - start;

            final long opsCount = hiveOpsCount.incrementAndGet();
            executedHiveQueries.put(opsCount, new QueryMetrics(sql, time));

            System.out.println("Hive command #" + opsCount + " execution time : " + time + " ms.");
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return res;
    }

    public static void loadHiveDriver() {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new IllegalStateException("No org.apache.hive.jdbc.HiveDriver found", e);
        }
    }

    public static void loadIgniteJdbcDriver() {
        try {
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("No org.apache.hive.jdbc.HiveDriver found", e);
        }
    }

    public static ResultSet executeIgniteQuery(Connection con, String sql) {
        ResultSet res = null;

        try {
            Statement stmt = con.createStatement();

            long start = System.currentTimeMillis();
            res = stmt.executeQuery(sql);
            long time = System.currentTimeMillis() - start;

            final long opsCount = igniteOpsCount.incrementAndGet();

            int size = 0;

            while(res.next())
                size++;

            executedIgniteQueries.put(opsCount, new QueryMetrics(sql, time, size));

            System.out.println("Ignite command #" + opsCount + " execution time : " + time + " ms.");

        } catch (SQLException e) {
            e.printStackTrace();

            return null;
        }

        return res;
    }

    public static ResultSet executeIgniteQueryNoStats(Connection con, String sql) {
        ResultSet res = null;

        try {
            Statement stmt = con.createStatement();

            res = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();

            return null;
        }

        return res;
    }

    public static Map<Long, QueryMetrics> getIgniteQueryMetrics() {
        return executedIgniteQueries;
    }

}
