/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.test.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.gridgain.internal.h2.api.ErrorCode;
import org.gridgain.internal.h2.command.dml.Select;
import org.gridgain.internal.h2.result.SortOrder;
import org.gridgain.internal.h2.test.TestBase;
import org.gridgain.internal.h2.test.TestDb;
import org.gridgain.internal.h2.tools.SimpleResultSet;
import org.gridgain.internal.h2.value.ValueInt;

/**
 * Index tests.
 */
public class TestIndex extends TestDb {

    private static int testFunctionIndexCounter;

    private Connection conn;
    private Statement stat;
    private final Random random = new Random();

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("index");
        testOrderIndex();
        testIndexTypes();
        testHashIndexOnMemoryTable();
        testErrorMessage();
        testDuplicateKeyException();
        int to = config.lockTimeout;
        config.lockTimeout = 50000;
        try {
            testConcurrentUpdate();
        } finally {
            config.lockTimeout = to;
        }
        testNonUniqueHashIndex();
        testRenamePrimaryKey();
        testRandomized();
        testDescIndex();
        testHashIndex();

        if (config.networked && config.big) {
            return;
        }

        random.setSeed(100);

        deleteDb("index");
        testWideIndex(147);
        testWideIndex(313);
        testWideIndex(979);
        testWideIndex(1200);
        testWideIndex(2400);
        if (config.big) {
            Random r = new Random();
            for (int j = 0; j < 10; j++) {
                int i = r.nextInt(3000);
                if ((i % 100) == 0) {
                    println("width: " + i);
                }
                testWideIndex(i);
            }
        }

        testLike();
        reconnect();
        testConstraint();
        testLargeIndex();
        testMultiColumnIndex();
        // long time;
        // time = System.nanoTime();
        testHashIndex(true, false);

        testHashIndex(false, false);
        testHashIndex(true, true);
        testHashIndex(false, true);

        testMultiColumnHashIndex();

        testFunctionIndex();

        testInStatementUsesIndex();
        testQueriesWithInStatementAndIndexWithContinuousPrefix();
        testQueriesWithInStatementAndIndexWithSkippedColumnsInPrefix();

        conn.close();
        deleteDb("index");

        // This test uses own connection
        testEnumIndex();
    }

    private void testOrderIndex() throws SQLException {
        Connection conn = getConnection("index");
        stat = conn.createStatement();
        stat.execute("create table test(id int, name varchar)");
        stat.execute("insert into test values (2, 'a'), (1, 'a')");
        stat.execute("create index on test(name)");
        ResultSet rs = stat.executeQuery(
                "select id from test where name = 'a' order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
        deleteDb("index");
    }

    private void testIndexTypes() throws SQLException {
        Connection conn = getConnection("index");
        stat = conn.createStatement();
        for (String type : new String[] { "unique", "hash", "unique hash" }) {
            stat.execute("create table test(id int)");
            stat.execute("create " + type + " index idx_name on test(id)");
            stat.execute("insert into test select x from system_range(1, 1000)");
            ResultSet rs = stat.executeQuery("select * from test where id=100");
            assertTrue(rs.next());
            assertFalse(rs.next());
            stat.execute("delete from test where id=100");
            rs = stat.executeQuery("select * from test where id=100");
            assertFalse(rs.next());
            rs = stat.executeQuery("select min(id), max(id) from test");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1000, rs.getInt(2));
            stat.execute("drop table test");
        }
        conn.close();
        deleteDb("index");
    }

    private void testErrorMessage() throws SQLException {
        reconnect();
        stat.execute("create table test(id int primary key, name varchar)");
        testErrorMessage("PRIMARY", "KEY", " ON PUBLIC.TEST(ID)");
        stat.execute("create table test(id int, name varchar primary key)");
        testErrorMessage("PRIMARY_KEY_2 ON PUBLIC.TEST(NAME)");
        stat.execute("create table test(id int, name varchar, primary key(id, name))");
        testErrorMessage("PRIMARY_KEY_2 ON PUBLIC.TEST(ID, NAME)");
        stat.execute("create table test(id int, name varchar, primary key(name, id))");
        testErrorMessage("PRIMARY_KEY_2 ON PUBLIC.TEST(NAME, ID)");
        stat.execute("create table test(id int, name int primary key)");
        testErrorMessage("PRIMARY", "KEY", " ON PUBLIC.TEST(NAME)");
        stat.execute("create table test(id int, name int, unique(name))");
        testErrorMessage("CONSTRAINT_INDEX_2 ON PUBLIC.TEST(NAME)");
        stat.execute("create table test(id int, name int, " +
                "constraint abc unique(name, id))");
        testErrorMessage("ABC_INDEX_2 ON PUBLIC.TEST(NAME, ID)");
    }

    private void testErrorMessage(String... expected) throws SQLException {
        try {
            stat.execute("INSERT INTO TEST VALUES(1, 1)");
            stat.execute("INSERT INTO TEST VALUES(1, 1)");
            fail();
        } catch (SQLException e) {
            String m = e.getMessage();
            int start = m.indexOf('"'), end = m.lastIndexOf('"');
            String s = m.substring(start + 1, end);
            for (String t : expected) {
                assertContains(s, t);
            }
        }
        stat.execute("drop table test");
    }

    private void testDuplicateKeyException() throws SQLException {
        reconnect();
        stat.execute("create table test(id int primary key, name varchar(255))");
        stat.execute("create unique index idx_test_name on test(name)");
        stat.execute("insert into TEST values(1, 'Hello')");
        try {
            stat.execute("insert into TEST values(2, 'Hello')");
            fail();
        } catch (SQLException ex) {
            assertEquals(ErrorCode.DUPLICATE_KEY_1, ex.getErrorCode());
            String m = ex.getMessage();
            // The format of the VALUES clause varies a little depending on the
            // type of the index, so just test that we're getting useful info
            // back.
            assertContains(m, "IDX_TEST_NAME ON PUBLIC.TEST(NAME)");
            assertContains(m, "'Hello'");
        }
        stat.execute("drop table test");
    }

    private class ConcurrentUpdateThread extends Thread {
        private final AtomicInteger concurrentUpdateId, concurrentUpdateValue;

        private final PreparedStatement psInsert, psDelete;

        boolean haveDuplicateKeyException;

        ConcurrentUpdateThread(Connection c, AtomicInteger concurrentUpdateId,
                AtomicInteger concurrentUpdateValue) throws SQLException {
            this.concurrentUpdateId = concurrentUpdateId;
            this.concurrentUpdateValue = concurrentUpdateValue;
            psInsert = c.prepareStatement("insert into test(id, value) values (?, ?)");
            psDelete = c.prepareStatement("delete from test where value = ?");
        }

        @Override
        public void run() {
            for (int i = 0; i < 10000; i++) {
                try {
                    if (Math.random() > 0.05) {
                        psInsert.setInt(1, concurrentUpdateId.incrementAndGet());
                        psInsert.setInt(2, concurrentUpdateValue.get());
                        psInsert.executeUpdate();
                    } else {
                        psDelete.setInt(1, concurrentUpdateValue.get());
                        psDelete.executeUpdate();
                    }
                } catch (SQLException ex) {
                    switch (ex.getErrorCode()) {
                    case 23505:
                        haveDuplicateKeyException = true;
                        break;
                    case 90131:
                        // Unlikely but possible
                        break;
                    default:
                        ex.printStackTrace();
                    }
                }
                if (Math.random() > 0.95)
                    concurrentUpdateValue.incrementAndGet();
            }
        }
    }

    private void testConcurrentUpdate() throws SQLException {
        Connection c = getConnection("index");
        Statement stat = c.createStatement();
        stat.execute("create table test(id int primary key, value int)");
        stat.execute("create unique index idx_value_name on test(value)");
        PreparedStatement check = c.prepareStatement("select value from test");
        ConcurrentUpdateThread[] threads = new ConcurrentUpdateThread[4];
        AtomicInteger concurrentUpdateId = new AtomicInteger(), concurrentUpdateValue = new AtomicInteger();

        // The same connection
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new ConcurrentUpdateThread(c, concurrentUpdateId, concurrentUpdateValue);
        }
        testConcurrentUpdateRun(threads, check);
        // Different connections
        Connection[] connections = new Connection[threads.length];
        for (int i = 0; i < threads.length; i++) {
            Connection c2 = getConnection("index");
            connections[i] = c2;
            threads[i] = new ConcurrentUpdateThread(c2, concurrentUpdateId, concurrentUpdateValue);
        }
        testConcurrentUpdateRun(threads, check);
        for (Connection c2 : connections) {
            c2.close();
        }
        stat.execute("drop table test");
        c.close();
    }

    private void testConcurrentUpdateRun(ConcurrentUpdateThread[] threads, PreparedStatement check)
            throws SQLException {
        for (ConcurrentUpdateThread t : threads) {
            t.start();
        }
        boolean haveDuplicateKeyException = false;
        for (ConcurrentUpdateThread t : threads) {
            try {
                t.join();
                haveDuplicateKeyException |= t.haveDuplicateKeyException;
            } catch (InterruptedException e) {
            }
        }
        assertTrue("haveDuplicateKeys", haveDuplicateKeyException);
        HashSet<Integer> set = new HashSet<>();
        try (ResultSet rs = check.executeQuery()) {
            while (rs.next()) {
                if (!set.add(rs.getInt(1))) {
                    fail("unique index violation");
                }
            }
        }
    }

    private void testNonUniqueHashIndex() throws SQLException {
        reconnect();
        stat.execute("create memory table test(id bigint, data bigint)");
        stat.execute("create hash index on test(id)");
        Random rand = new Random(1);
        PreparedStatement prepInsert = conn.prepareStatement(
                "insert into test values(?, ?)");
        PreparedStatement prepDelete = conn.prepareStatement(
                "delete from test where id=?");
        PreparedStatement prepSelect = conn.prepareStatement(
                "select count(*) from test where id=?");
        HashMap<Long, Integer> map = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            long key = rand.nextInt(10) * 1000000000L;
            Integer r = map.get(key);
            int result = r == null ? 0 : (int) r;
            if (rand.nextBoolean()) {
                prepSelect.setLong(1, key);
                ResultSet rs = prepSelect.executeQuery();
                rs.next();
                assertEquals(result, rs.getInt(1));
            } else {
                if (rand.nextBoolean()) {
                    prepInsert.setLong(1, key);
                    prepInsert.setInt(2, rand.nextInt());
                    prepInsert.execute();
                    map.put(key, result + 1);
                } else {
                    prepDelete.setLong(1, key);
                    prepDelete.execute();
                    map.put(key, 0);
                }
            }
        }
        stat.execute("drop table test");
        conn.close();
    }

    private void testRenamePrimaryKey() throws SQLException {
        if (config.memory) {
            return;
        }
        reconnect();
        stat.execute("create table test(id int not null)");
        stat.execute("alter table test add constraint x primary key(id)");
        ResultSet rs;
        rs = conn.getMetaData().getIndexInfo(null, null, "TEST", true, false);
        rs.next();
        String old = rs.getString("INDEX_NAME");
        stat.execute("alter index " + old + " rename to y");
        rs = conn.getMetaData().getIndexInfo(null, null, "TEST", true, false);
        rs.next();
        assertEquals("Y", rs.getString("INDEX_NAME"));
        reconnect();
        rs = conn.getMetaData().getIndexInfo(null, null, "TEST", true, false);
        rs.next();
        assertEquals("Y", rs.getString("INDEX_NAME"));
        stat.execute("drop table test");
    }

    private void testRandomized() throws SQLException {
        boolean reopen = !config.memory;
        Random rand = new Random(1);
        reconnect();
        stat.execute("drop all objects");
        stat.execute("CREATE TABLE TEST(ID identity)");
        int len = getSize(100, 1000);
        for (int i = 0; i < len; i++) {
            switch (rand.nextInt(4)) {
            case 0:
                if (rand.nextInt(10) == 0) {
                    if (reopen) {
                        trace("reconnect");
                        reconnect();
                    }
                }
                break;
            case 1:
                trace("insert");
                stat.execute("insert into test(id) values(null)");
                break;
            case 2:
                trace("delete");
                stat.execute("delete from test");
                break;
            case 3:
                trace("insert 1-100");
                stat.execute("insert into test select null from system_range(1, 100)");
                break;
            }
        }
        stat.execute("drop table test");
    }

    private void testHashIndex() throws SQLException {
        reconnect();
        stat.execute("create table testA(id int primary key, name varchar)");
        stat.execute("create table testB(id int primary key hash, name varchar)");
        int len = getSize(300, 3000);
        stat.execute("insert into testA select x, 'Hello' from " +
                "system_range(1, " + len + ")");
        stat.execute("insert into testB select x, 'Hello' from " +
                "system_range(1, " + len + ")");
        Random rand = new Random(1);
        for (int i = 0; i < len; i++) {
            int x = rand.nextInt(len);
            String sql = "";
            switch (rand.nextInt(3)) {
            case 0:
                sql = "delete from testA where id = " + x;
                break;
            case 1:
                sql = "update testA set name = " + rand.nextInt(100) + " where id = " + x;
                break;
            case 2:
                sql = "select name from testA where id = " + x;
                break;
            default:
            }
            boolean result = stat.execute(sql);
            if (result) {
                ResultSet rs = stat.getResultSet();
                String s1 = rs.next() ? rs.getString(1) : null;
                rs = stat.executeQuery(sql.replace('A', 'B'));
                String s2 = rs.next() ? rs.getString(1) : null;
                assertEquals(s1, s2);
            } else {
                int count1 = stat.getUpdateCount();
                int count2 = stat.executeUpdate(sql.replace('A', 'B'));
                assertEquals(count1, count2);
            }
        }
        stat.execute("drop table testA, testB");
        conn.close();
    }

    private void reconnect() throws SQLException {
        if (conn != null) {
            conn.close();
            conn = null;
        }
        conn = getConnection("index");
        stat = conn.createStatement();
    }

    private void testDescIndex() throws SQLException {
        if (config.memory) {
            return;
        }
        ResultSet rs;
        reconnect();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("CREATE INDEX IDX_ND ON TEST(ID DESC)");
        rs = conn.getMetaData().getIndexInfo(null, null, "TEST", false, false);
        rs.next();
        assertEquals("D", rs.getString("ASC_OR_DESC"));
        assertEquals(SortOrder.DESCENDING, rs.getInt("SORT_TYPE"));
        stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(1, 30)");
        rs = stat.executeQuery(
                "SELECT COUNT(*) FROM TEST WHERE ID BETWEEN 10 AND 20");
        rs.next();
        assertEquals(11, rs.getInt(1));
        reconnect();
        rs = conn.getMetaData().getIndexInfo(null, null, "TEST", false, false);
        rs.next();
        assertEquals("D", rs.getString("ASC_OR_DESC"));
        assertEquals(SortOrder.DESCENDING, rs.getInt("SORT_TYPE"));
        rs = stat.executeQuery(
                "SELECT COUNT(*) FROM TEST WHERE ID BETWEEN 10 AND 20");
        rs.next();
        assertEquals(11, rs.getInt(1));
        stat.execute("DROP TABLE TEST");

        stat.execute("create table test(x int, y int)");
        stat.execute("insert into test values(1, 1), (1, 2)");
        stat.execute("create index test_x_y on test (x desc, y desc)");
        rs = stat.executeQuery("select * from test where x=1 and y<2");
        assertTrue(rs.next());

        conn.close();
    }

    private String getRandomString(int len) {
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < len; i++) {
            buff.append((char) ('a' + random.nextInt(26)));
        }
        return buff.toString();
    }

    private void testWideIndex(int length) throws SQLException {
        reconnect();
        stat.execute("drop all objects");
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
        for (int i = 0; i < 100; i++) {
            stat.execute("INSERT INTO TEST VALUES(" + i +
                    ", SPACE(" + length + ") || " + i + " )");
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY NAME");
        while (rs.next()) {
            int id = rs.getInt("ID");
            String name = rs.getString("NAME");
            assertEquals("" + id, name.trim());
        }
        if (!config.memory) {
            reconnect();
            rs = stat.executeQuery("SELECT * FROM TEST ORDER BY NAME");
            while (rs.next()) {
                int id = rs.getInt("ID");
                String name = rs.getString("NAME");
                assertEquals("" + id, name.trim());
            }
        }
        stat.execute("drop all objects");
    }

    private void testLike() throws SQLException {
        reconnect();
        stat.execute("CREATE TABLE ABC(ID INT, NAME VARCHAR)");
        stat.execute("INSERT INTO ABC VALUES(1, 'Hello')");
        PreparedStatement prep = conn.prepareStatement(
                "SELECT * FROM ABC WHERE NAME LIKE CAST(? AS VARCHAR)");
        prep.setString(1, "Hi%");
        prep.execute();
        stat.execute("DROP TABLE ABC");
    }

    private void testConstraint() throws SQLException {
        if (config.memory) {
            return;
        }
        stat.execute("CREATE TABLE PARENT(ID INT PRIMARY KEY)");
        stat.execute("CREATE TABLE CHILD(ID INT PRIMARY KEY, " +
                "PID INT, FOREIGN KEY(PID) REFERENCES PARENT(ID))");
        reconnect();
        stat.execute("DROP TABLE PARENT");
        stat.execute("DROP TABLE CHILD");
    }

    private void testLargeIndex() throws SQLException {
        random.setSeed(10);
        for (int i = 1; i < 100; i += getSize(1000, 7)) {
            stat.execute("DROP TABLE IF EXISTS TEST");
            stat.execute("CREATE TABLE TEST(NAME VARCHAR(" + i + "))");
            stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
            PreparedStatement prep = conn.prepareStatement(
                    "INSERT INTO TEST VALUES(?)");
            for (int j = 0; j < getSize(2, 5); j++) {
                prep.setString(1, getRandomString(i));
                prep.execute();
            }
            if (!config.memory) {
                conn.close();
                conn = getConnection("index");
                stat = conn.createStatement();
            }
            ResultSet rs = stat.executeQuery(
                    "SELECT COUNT(*) FROM TEST WHERE NAME > 'mdd'");
            rs.next();
            int count = rs.getInt(1);
            trace(i + " count=" + count);
        }

        stat.execute("DROP TABLE IF EXISTS TEST");
    }

    private void testHashIndex(boolean primaryKey, boolean hash)
            throws SQLException {
        if (config.memory) {
            return;
        }

        reconnect();

        stat.execute("DROP TABLE IF EXISTS TEST");
        if (primaryKey) {
            stat.execute("CREATE TABLE TEST(A INT PRIMARY KEY " +
                    (hash ? "HASH" : "") + ", B INT)");
        } else {
            stat.execute("CREATE TABLE TEST(A INT, B INT)");
            stat.execute("CREATE UNIQUE " + (hash ? "HASH" : "") + " INDEX ON TEST(A)");
        }
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        int len = getSize(5, 1000);
        for (int a = 0; a < len; a++) {
            prep.setInt(1, a);
            prep.setInt(2, a);
            prep.execute();
            assertEquals(1,
                    getValue("SELECT COUNT(*) FROM TEST WHERE A=" + a));
            assertEquals(0,
                    getValue("SELECT COUNT(*) FROM TEST WHERE A=-1-" + a));
        }

        reconnect();

        prep = conn.prepareStatement("DELETE FROM TEST WHERE A=?");
        for (int a = 0; a < len; a++) {
            if (getValue("SELECT COUNT(*) FROM TEST WHERE A=" + a) != 1) {
                assertEquals(1,
                        getValue("SELECT COUNT(*) FROM TEST WHERE A=" + a));
            }
            prep.setInt(1, a);
            assertEquals(1, prep.executeUpdate());
        }
        assertEquals(0, getValue("SELECT COUNT(*) FROM TEST"));
    }

    private void testMultiColumnIndex() throws SQLException {
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(A INT, B INT)");
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        int len = getSize(3, 260);
        for (int a = 0; a < len; a++) {
            prep.setInt(1, a);
            prep.setInt(2, a);
            prep.execute();
        }
        stat.execute("INSERT INTO TEST SELECT A, B FROM TEST");
        stat.execute("CREATE INDEX ON TEST(A, B)");
        prep = conn.prepareStatement("DELETE FROM TEST WHERE A=?");
        for (int a = 0; a < len; a++) {
            log("SELECT * FROM TEST");
            assertEquals(2,
                    getValue("SELECT COUNT(*) FROM TEST WHERE A=" + (len - a - 1)));
            assertEquals((len - a) * 2, getValue("SELECT COUNT(*) FROM TEST"));
            prep.setInt(1, len - a - 1);
            prep.execute();
        }
        assertEquals(0, getValue("SELECT COUNT(*) FROM TEST"));
    }

    private void testMultiColumnHashIndex() throws SQLException {
        if (config.memory) {
            return;
        }

        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(A INT, B INT, DATA VARCHAR(255))");
        stat.execute("CREATE UNIQUE HASH INDEX IDX_AB ON TEST(A, B)");
        PreparedStatement prep;
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
        // speed is quadratic (len*len)
        int len = getSize(2, 14);
        for (int a = 0; a < len; a++) {
            for (int b = 0; b < len; b += 2) {
                prep.setInt(1, a);
                prep.setInt(2, b);
                prep.setString(3, "i(" + a + "," + b + ")");
                prep.execute();
            }
        }

        reconnect();

        prep = conn.prepareStatement(
                "UPDATE TEST SET DATA=DATA||? WHERE A=? AND B=?");
        for (int a = 0; a < len; a++) {
            for (int b = 0; b < len; b += 2) {
                prep.setString(1, "u(" + a + "," + b + ")");
                prep.setInt(2, a);
                prep.setInt(3, b);
                prep.execute();
            }
        }

        reconnect();

        ResultSet rs = stat.executeQuery(
                "SELECT * FROM TEST WHERE DATA <> 'i('||a||','||b||')u('||a||','||b||')'");
        assertFalse(rs.next());
        assertEquals(len * (len / 2), getValue("SELECT COUNT(*) FROM TEST"));
        stat.execute("DROP TABLE TEST");
    }

    private void testHashIndexOnMemoryTable() throws SQLException {
        reconnect();
        stat.execute("drop table if exists hash_index_test");
        stat.execute("create memory table hash_index_test as " +
                "select x as id, x % 10 as data from (select *  from system_range(1, 100))");
        stat.execute("create hash index idx2 on hash_index_test(data)");
        assertEquals(10,
                getValue("select count(*) from hash_index_test where data = 1"));

        stat.execute("drop index idx2");
        stat.execute("create unique hash index idx2 on hash_index_test(id)");
        assertEquals(1,
                getValue("select count(*) from hash_index_test where id = 1"));
    }

    private int getValue(String sql) throws SQLException {
        ResultSet rs = stat.executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }

    private void log(String sql) throws SQLException {
        trace(sql);
        ResultSet rs = stat.executeQuery(sql);
        int cols = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < cols; i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append("[" + i + "]=" + rs.getString(i + 1));
            }
            trace(buff.toString());
        }
        trace("---done---");
    }

    /**
     * This method is called from the database.
     *
     * @return the result set
     */
    public static ResultSet testFunctionIndexFunction() {
        // There are additional callers like JdbcConnection.prepareCommand() and
        // CommandContainer.recompileIfRequired()
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            if (element.getClassName().startsWith(Select.class.getName())) {
                testFunctionIndexCounter++;
                break;
            }
        }
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", Types.INTEGER, ValueInt.PRECISION, 0);
        rs.addColumn("VALUE", Types.INTEGER, ValueInt.PRECISION, 0);
        rs.addRow(1, 10);
        rs.addRow(2, 20);
        rs.addRow(3, 30);
        return rs;
    }

    private void testFunctionIndex() throws SQLException {
        testFunctionIndexCounter = 0;
        stat.execute("CREATE ALIAS TEST_INDEX FOR \"" + TestIndex.class.getName() + ".testFunctionIndexFunction\"");
        try (ResultSet rs = stat.executeQuery("SELECT * FROM TEST_INDEX() WHERE ID = 1 OR ID = 3")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(10, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(30, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            stat.execute("DROP ALIAS TEST_INDEX");
        }
        assertEquals(1, testFunctionIndexCounter);
    }

    private void testInStatementUsesIndex() throws SQLException {
        stat.execute("create table in_table (f0 int, f1 int, f2 int, f3 int, f4 int, primary key (f0))");

        try {
            stat.execute("create index PK_IDX on in_table (f1, f2, f3)");

            String sqlInsert = "insert into in_table (f0, f1, f2, f3, f4) values (%d, %d, %d, %d, 2)";

            for (int i = 1; i <= 10; ++i)
                for (int j = 1; j <= 10; ++j)
                    for (int k = 1; k <= 10; ++k)
                        stat.execute(String.format(sqlInsert, (i * 100) + (j * 10) + k, i, j, k));

            String[] queries = new String[] {
                "select * from in_table where f1 = 8 and f2 in (1, 5, 3, 7) and f3 = 5",
                "select * from in_table where f1 in (1, 5, 3, 7) and f2 = 8 and f3 = 5",
                "select * from in_table where f1 = 8 and f2 = 5 and f3 in (1, 5, 3, 7)",
                "select * from in_table use index(PK_IDX) where f1 = 8 and f2 in (1, 5, 3, 7) and f3 = 5",
                "select * from in_table use index(PK_IDX) where f1 in (1, 5, 3, 7) and f2 = 8 and f3 = 5",
                "select * from in_table use index(PK_IDX) where f1 = 8 and f2 = 5 and f3 in (1, 5, 3, 7)"
            };

            for (String sql : queries) {
                try (ResultSet rs = stat.executeQuery(sql)) {
                    int resCnt = 0;

                    while (rs.next())
                        resCnt++;

                    assertEquals(4, resCnt);
                }

                try (ResultSet rs = stat.executeQuery("explain analyze " + sql)) {
                    rs.next();

                    assertContains(rs.getString(1), "/* scanCount: 5 */");
                }
            }
        } finally {
            stat.execute("drop table in_table");
        }
    }

    private void testEnumIndex() throws SQLException {
        if (config.memory || config.networked) {
            return;
        }
        deleteDb("index");
        String url = "jdbc:gg-h2:" + getBaseDir() + "/index;DB_CLOSE_DELAY=0";
        Connection conn = DriverManager.getConnection(url);
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE TEST(ID INT, V ENUM('A', 'B'), CONSTRAINT PK PRIMARY KEY(ID, V))");
        stat.execute("INSERT INTO TEST VALUES (1, 'A'), (2, 'B')");

        conn.close();
        conn = DriverManager.getConnection(url);
        stat = conn.createStatement();

        stat.execute("DELETE FROM TEST WHERE V = 'A'");
        stat.execute("DROP TABLE TEST");

        conn.close();
        deleteDb("index");
    }

    private void testQueriesWithInStatementAndIndexWithContinuousPrefix() throws SQLException {
        testIndexesAndQueriesWithInStatements("IDX_F1_F2_F3", () -> {
                stat.execute("create index IDX_F1_F2_F3 on in_table (f1, f2, f3)");
        },
            // IN_LIST
            // =======

            // scan_count: |{1-10}| * |{1, 5, 3, 7}| * |{1-10}| = 10 * 4 * 10 = 400
            challenge(predicates("f1 in (1, 5, 3, 7)", null, null), 400, 400),

            // scan_count: |{?}| * |{1, 5, 3, 7}| * |{1-10}| = 1 * 4 * 10 = 40
            challenge(predicates("f1 = ?", "f2 in (1, 5, 3, 7)", null), 40, 40, 8),
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 = ?", null), 40, 40, 8),

            // scan_count: |{?}| * |{1, 5, 3, 7}| * |{?}| = 1 * 4 * 1 = 4
            challenge(predicates("f1 = ?", "f2 in (1, 5, 3, 7)", "f3 = ?"), 4, 4, 8, 5),
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 = ?", "f3 = ?"), 4, 4, 8, 5),

            // scan_count: |{8}| * |{1, 5, 3, 7}| * |{9,10}| = 1 * 4 * 2 = 8
            challenge(predicates("f1 = ?", "f2 in (1, 5, 3, 7)", "f3 > ?"), 4, 8, 8, 9),
            challenge(predicates("f1 = ?", "f2 in (1, 5, 3, 7)", "f3 < ?"), 4, 8, 8, 2),

            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 = ?", "f3 < ?"), 4, 8, 8, 2),
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 = ?", "f3 > ?"), 4, 8, 8, 9),

            // scan_count: |{8}| * |{1, 5, 3, 7}| * |{3-4}| = 1 * 4 * 2
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 = ?", "f3 < ? and f3 > ?"), 8, 16, 8, 5, 2),
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 = ?", "f3 > ? and f3 < ?"), 8, 16, 8, 5, 8),

            // scan_count ~= 40 = 36 + 4
            // 36 :: Number of valid rows: |{8}| * |{1, 5, 3, 7}| * |{1-10} \ {?}| = 1 * 4 * 9
            //  4 :: Times index scan meets rows that don't satisfy criteria but this still increment scan count
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 = ?", "f3 <> ?"), 36, 40, 8, 7),
            challenge(predicates("f1 = ?", "f2 in (1, 5, 3, 7)", "f3 <> ?"), 36, 40, 8, 7),

            // scan_count: |{1, 5, 3, 7}| * |{1, 5, 3, 7}| * |{1-10}| = 4 * 4 * 10 = 160
            // Since 2 IN statements are not supported only first one will be expanded, so scan count is going to be = 400
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 in (1, 5, 3, 7)", null), 160, 400),
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 in (1, 5, 3, 7)", "f3 = ?"), 16, 400, 1),
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 in (1, 5, 3, 7)", "f3 > ?"), 80, 400, 5),
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 in (1, 5, 3, 7)", "f3 < ?"), 64, 400, 5),
            challenge(predicates("f1 in (1, 5, 3, 7)", "f2 in (1, 5, 3, 7)", "f3 <> ?"), 144, 400, 5),

            // IN_QUERY
            // ========

            challenge(predicates(inScalarSubquery("f1"), "f2=?", null), 10, 10, 7),
            challenge(predicates("f1=?", inScalarSubquery("f2"), null), 10, 10, 7),
            challenge(predicates("f1=?", inScalarSubquery("f2"), "f3 = ?"), 1, 1, 7, 5),
            challenge(predicates("f1=?", inScalarSubquery("f2"), "f3 > ?"), 5, 6, 7, 5),
            challenge(predicates("f1=?", inScalarSubquery("f2"), "f3 < ?"), 4, 5, 7, 5),
            challenge(predicates("f1=?", inScalarSubquery("f2"), "f3 <> ?"), 9, 10, 7, 5),

            challenge(predicates(inMultirowSubquery("f1"), "f2=?", null), 50, 50, 7),
            challenge(predicates("f1=?", inMultirowSubquery("f2"), null), 50, 50, 7),
            challenge(predicates("f1=?", inMultirowSubquery("f2"), "f3 = ?"), 5, 5, 7, 5),
            challenge(predicates("f1=?", inMultirowSubquery("f2"), "f3 > ?"), 25, 30, 7, 5),
            challenge(predicates("f1=?", inMultirowSubquery("f2"), "f3 < ?"), 20, 25, 7, 5),
            challenge(predicates("f1=?", inMultirowSubquery("f2"), "f3 <> ?"), 45, 50, 7, 5),

            // N x M :: For multiple IN predicates we pick only first one
            // ==========================================================

            // optimal scan_count: |{10}| * |{1, 5, 3, 7}| * |{1-10}| = 1 * 4 * 10 = 40
            //    real scan_count: |{10}| * |{1-10}| * |{1-10}| = 1 * 10 * 10 = 100
            challenge(predicates(inScalarSubquery("f1"), "f2 in (1, 5, 3, 7)", null), 40, 100),
            challenge(predicates("f1 in (1, 5, 3, 7)", inScalarSubquery("f2"), null), 40, 400),

            challenge(predicates(inMultirowSubquery("f1"), "f2 in (1, 5, 3, 7)", null), 200, 500),
            challenge(predicates("f1 in (1, 5, 3, 7)", inMultirowSubquery("f2"), null), 200, 400),

            challenge(predicates(inScalarSubquery("f1"), "f2 in (1, 5, 3, 7)", "f3 = ?"), 4, 100, 5),
            challenge(predicates("f1 in (1, 5, 3, 7)", inScalarSubquery("f2"), "f3 > ?"), 20, 400, 5),
            challenge(predicates(inScalarSubquery("f1"), "f2 in (1, 5, 3, 7)", "f3 < ?"), 16, 100, 5),
            challenge(predicates("f1 in (1, 5, 3, 7)", inScalarSubquery("f2"), "f3 <> ?"), 36, 400, 5),

            // No prefix :: Fall back to full-scan
            // ===================================

            // rs_size: |{1-10}| * |{1, 5, 3, 7}| * |{?}| = 10 * 4 * 1 = 40
            challenge(predicates(null, "f2 in (1, 5, 3, 7)", "f3 = ?"), 40, 1000, 7),
            challenge(predicates(null, "f2 = ?", "f3 in (1, 5, 3, 7)"), 40, 1000, 7),

            // rs_size: |{1-10}| * |{1, 5, 3, 7}| * |{8, 9, 10}| = 10 * 4 * 3 = 120
            challenge(predicates(null, "f2 in (1, 5, 3, 7)", "f3 > ?"), 120, 1000, 7),

            // rs_size: |{1-10}| * |{1-6}| * |{1, 5, 3, 7}| = 10 * 6 * 4 = 40
            challenge(predicates(null, "f2 < ?", "f3 in (1, 5, 3, 7)"), 240, 1000, 7),

            // rs_size: |{1-10}| * |{6, 7, 8, 9, 10}| * |{?}| = 10 * 5 * 1 = 50
            challenge(predicates(null, inMultirowSubquery("f2"), "f3 = ?"), 50, 1000, 7),
            challenge(predicates(null, "f2 = ?", inMultirowSubquery("f3")), 50, 1000, 7),

            // Predicates with holes :: Use only first column
            // ==============================================

            //    rs_size: |{?}| * |{1-10}| * |{?}| = 1 * 10 * 1 = 10
            // scan_count: |{?}| * |{1-10}| * |{1-10}| = 1 * 10 * 10 = 100
            challenge(predicates("f1 = ?", null, "f3 = ?"), 10, 100, 3, 7),

            //    rs_size: |{1, 5, 3, 7}| * |{1-10}| * |{?}| = 4 * 10 * 1 = 40
            // scan_count: |{1, 5, 3, 7}| * |{1-10}| * |{1-10}| = 4 * 10 * 10 = 400
            challenge(predicates("f1 in (1, 5, 3, 7)", null, "f3 = ?"), 40, 400, 7),

            // scan_count: |{1, 5, 3, 7}| * |{1-10}| * |{1-10}| = 4 * 10 * 10 = 400
            //    rs_size: |{1, 5, 3, 7}| * |{1-10}| * |{8,9,10}| = 4 * 10 * 3 = 120
            challenge(predicates("f1 in (1, 5, 3, 7)", null, "f3 > ?"), 120, 400, 7),

            // NON_EQUALITY ON PREFIX :: Only first prefix column is used
            // ==========================================================

            //            rs_size: |{9, 10}| * |{1, 5, 3, 7}| * |{?}| = 2 * 4 * 1 = 8
            // optimal scan_count: |{9, 10}| * |{1-10}| * |{1-10}| = 2 * 10 * 10 = 200
            //    real scan_count: |{8, 9, 10}| * |{1-10}| * |{1-10}| = 3 * 10 * 10 = 300
            // H2 can is not smart enough to infer that in case of enumerable types end boundary
            // can be set to (?+1 = 8+1 = 9), so it scans with filter (f1=8, f2=null, f3=null)
            challenge(predicates("f1 > ?", "f2 in (1, 5, 3, 7)", "f3 = ?"), 8, 300, 8, 5),

            // rs_size: |{1, 2}| * |{1-10}| * |{1, 5, 3, 7}| = 2 * 10 * 4 = 80
            challenge(predicates("f1 < ?", null, "f3 in (1, 5, 3, 7)"), 80, 300, 3)
        );
    }

    private void testQueriesWithInStatementAndIndexWithSkippedColumnsInPrefix() throws SQLException {
        System.out.println("\n\n");

        testIndexesAndQueriesWithInStatements(null, () -> {
            stat.execute("create index IDX_F1_F3 on in_table (f1, f3)");
        },
            challenge(predicates("f1 in (1, 5, 3, 7)", null, "f3 = ?"), 40, 40, 9),

            // rs & scan_count: |{?}| * |{1-10}| * |{1, 5, 3, 7}| = 1 * 10 * 4 = 40
            challenge(predicates("f1 = ?", null, "f3 in (1, 5, 3, 7)"), 40, 40, 7),

            //         rs: |{1, 5, 3, 7}| * |{1-10}| * |{9, 10}| = 4 * 10 * 2 = 80
            // scan_count: |{1, 5, 3, 7}| * |{1-10}| * |{8, 9, 10}| = 4 * 10 * 3 = 300
            challenge(predicates("f1 in (1, 5, 3, 7)", null, "f3 > ?"), 80, 120, 8),

            // rs & optimal scan_count: |{8, 9, 10}| * |{1-10}| * |{1, 5, 3, 7}| = 3 * 10 * 4 = 120
            //         real scan_count: |{7, 8, 9, 10}| * |{1-10}| * |{1-10}| = 4 * 10 * 10 = 400
            challenge(predicates("f1 > ?", null, "f3 in (1, 5, 3, 7)"), 120, 400, 7)
        );
    }

    private void testIndexesAndQueriesWithInStatements(@Nullable String index, SqlAction indexesSetup, QueryChallenge... templates) throws SQLException {
        stat.execute("create table in_table (f0 int, f1 int, f2 int, f3 int, f4 int, primary key (f0))");

        try {
            indexesSetup.exec();

            String sqlInsert = "insert into in_table (f0, f1, f2, f3, f4) values (%d, %d, %d, %d, 2)";

            for (int i = 1; i <= 10; ++i)
                for (int j = 1; j <= 10; ++j)
                    for (int k = 1; k <= 10; ++k)
                        stat.execute(String.format(sqlInsert, (i * 100) + (j * 10) + k, i, j, k));

            SqlConsumer<QueryChallenge> checks = qc -> {
                qc.assertResult(conn, rs -> {
                    int resCnt = 0;

                    StringBuilder results = new StringBuilder()
                        .append("row_id |   f0  f1  f2  f3  f4").append("\n");

                    while (rs.next()) {
                        int f0 = rs.getInt(1);
                        int f1 = rs.getInt(2);
                        int f2 = rs.getInt(3);
                        int f3 = rs.getInt(4);
                        int f4 = rs.getInt(5);

                        results.append(String.format("%6d | %4d %3d %3d %3d %3d", resCnt, f0, f1, f2, f3, f4)).append("\n");

                        resCnt++;
                    }

                    boolean match = qc.expectedRsSize == resCnt;

                    if (qc.isDebug() || !match) {
                        System.out.println("Query: " + qc.description());
                        System.out.println(results);
                    }

                    if (!match) {
                        throw new AssertionError("Unexpected RS size. " +
                            "Expected <" + qc.expectedRsSize + ">, but was <" + resCnt + ">. " +
                            "Query: \"" + qc.description() + "\"");
                    }
                });

                qc.assertScanCount(conn);
            };

            for (QueryChallenge template : templates) {
                if (template.isDebug())
                    System.out.println(template.description());

                QueryChallenge[] challenges = index != null
                    ? template.expand("in_table", index)
                    : template.expand();

                for (QueryChallenge challenge : challenges) {
                    checks.exec(challenge);
                }
            }
        }
        finally {
            stat.execute("drop table in_table");
        }
    }

    private static QueryChallenge challenge(String query, int expectedRsSize, int expectedSc, int... params) {
        return new QueryChallenge(query, params, expectedRsSize, expectedSc, false);
    }

    private static String predicates(String f1, String f2, String f3) {
        StringBuilder query = new StringBuilder()
            .append("select * from in_table where ");

        boolean and = false;
        if (f1 != null) {
            query.append(f1);
            and = true;
        }

        if (f2 != null) {
            if (and)
                query.append(" and ");
            query.append(f2);
            and = true;
        }

        if (f3 != null) {
            if (and)
                query.append(" and ");
            query.append(f3);
        }

        return query.toString();
    }

    private static String inScalarSubquery(String column) {
        return column + " in (select count(distinct(f3)) from in_table)";
    }

    private static String inMultirowSubquery(String column) {
        return column + " in (select distinct(f3) from in_table where f3 > 5)";
    }

    private static class QueryChallenge {
        private final String query;

        /** Query params. */
        private final int[] params;

        /** Expected ResultSet size. */
        private final int expectedRsSize;

        /** Expected scan_count. */
        private final int expectedSc;

        /** Marker that challenge assertions should print debug info. */
        private boolean debug;

        QueryChallenge(String query, int[] params, int expectedRsSize, int expectedSc, boolean debug) {
            this.query = query;
            this.params = params;
            this.expectedRsSize = expectedRsSize;
            this.expectedSc = expectedSc;
            this.debug = debug;
        }

        public QueryChallenge debug() {
            this.debug = true;
            return this;
        }

        public boolean isDebug() {
            return this.debug;
        }

        /**
         * Generate all possible combinations of queries based on original query pattern.
         * For example, query {@code SELECT * FROM t WHERE f1=?, f2=?} with supplied
         * params {@code [3,7]} will be expanded to the following list:
         * <pre>
         *     1. SELECT * FROM t WHERE f1=?, f2=?       params=[3,7]
         *     2. SELECT * FROM t WHERE f1=3, f2=?       params=[3]
         *     3. SELECT * FROM t WHERE f1=?, f2=7       params=[7]
         *     4. SELECT * FROM t WHERE f1=3, f2=7       params=[]
         * </pre>
         *
         * @return Arrays of expanded challenge variants.
         */
        QueryChallenge[] expand() {
            int variantsCnt = (int)Math.pow(2, params.length);
            QueryChallenge[] result = new QueryChallenge[variantsCnt];

            for (int i = 0; i < variantsCnt; i++) {
                result[i] = expand(i);
            }

            return result;
        }

        /**
         * Similar to {@link #expand()}, but it produces also variants with explicit index hint.
         *
         * @param table Table name that will be used for HINT insertion point.
         * @param indexName Index name that will be used inside the hint.
         * @return Arrays of expanded challenge variants.
         */
        QueryChallenge[] expand(String table, String indexName) {
            QueryChallenge[] noHint = this.expand();

            QueryChallenge[] withHint = new QueryChallenge(
                query.replace(table, table + " USE INDEX(" + indexName + ")"),
                params, expectedRsSize, expectedSc, debug
            ).expand();

            return Stream.concat(Stream.of(noHint), Stream.of(withHint)).toArray(QueryChallenge[]::new);
        }

        /**
         * Inlines params to the original query string according to specified bitmask.
         * I.e. if bitmask=5, and original params=[1,3,5] then resulting query will look like:
         * {@code SELECT * FROM t WHERE f1=1, f2=?, f3=5}, {@code params=[3]}.
         *
         * @param inlineMask bitmask of param indexes that should be inlined.
         * @return Challenge with inlined params.
         */
        QueryChallenge expand(int inlineMask) {
            int[] remainedParams = new int[params.length - Integer.bitCount(inlineMask)];
            int remainedParamsCnt = 0;
            String inlinedQuery = query;

            for (int i = 0; i < params.length; i++) {
                if ((1 << i & inlineMask) != 0) {
                    inlinedQuery = replaceNthOccurrence(inlinedQuery, "?", String.valueOf(params[i]), remainedParamsCnt);
                }
                else {
                    remainedParams[remainedParamsCnt++] = params[i];
                }
            }

            return new QueryChallenge(inlinedQuery, remainedParams, expectedRsSize, expectedSc, debug);
        }

        PreparedStatement prepareStatement(Connection conn, String qry) throws SQLException {
            PreparedStatement ps = conn.prepareStatement(qry);

            if (isParametrized()) {
                int pos = 1;

                for (int p : params) {
                    ps.setInt(pos++, p);
                }
            }

            return ps;
        }

        boolean isParametrized() {
            return query.contains("?");
        }

        String description() {
            String description = query;

            for (int i = 0; i < params.length; i++) {
                description = replaceNthOccurrence(description, "?", "[?:" + params[i] + "]", i);
            }

            return description;
        }

        void assertResult(Connection conn, SqlConsumer<ResultSet> checks) throws SQLException {
            try (PreparedStatement ps = prepareStatement(conn, query)) {
                try (ResultSet rs = ps.executeQuery()) {
                    checks.exec(rs);
                }
            }
        }

        void assertPlan(Connection conn, Consumer<String> checks) throws SQLException {
            try (PreparedStatement ps = prepareStatement(conn, "explain analyze " + query)) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();

                    String plan = rs.getString(1);

                    checks.accept(plan);
                }
            }
        }

        void assertScanCount(Connection conn) throws SQLException {
            assertPlan(conn, plan -> {
                // Remove trailing scan count increment (happens in TablerFilter.next() before method exit),
                // for better alignment with number of read rows
                int scanCount = extractScanCount(plan) - 1;
                boolean match = scanCount == expectedSc;

                if (debug || !match) {
                    System.out.println("Query: " + description());
                    System.out.println(plan);
                }

                if (!match) {
                    throw new AssertionError("Unexpected scan count. " +
                        "Expected <" + expectedSc + ">, but was <" + scanCount + ">. " +
                        "Query: \"" + description() + "\"");
                }
            });
        }
    }

    private static String replaceNthOccurrence(String input, String target, String replacement, int n) {
        int index = -1;

        for (int i = 0; i <= n; i++) {
            index = input.indexOf(target, index + 1);

            if (index == -1) {
                return input;
            }
        }

        return input.substring(0, index)
            + replacement
            + input.substring(index + target.length());
    }

    private static final Pattern SCAN_COUNT_REGEXP = Pattern.compile("/\\*\\s*scanCount:\\s*(\\d+)\\s*\\*/");

    private static int extractScanCount(String plan) {
        assert plan != null;

        Matcher matcher = SCAN_COUNT_REGEXP.matcher(plan);

        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }

        throw new IllegalStateException("Scan count not found in the plan: " + plan);
    }

    @FunctionalInterface
    private interface SqlAction {
        void exec() throws SQLException;
    }

    @FunctionalInterface
    private interface SqlConsumer<T> {
        void exec(T arg) throws SQLException;
    }
}
