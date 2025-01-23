/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.gridgain.internal.h2.samples.CachedPreparedStatements;
import org.gridgain.internal.h2.samples.Compact;
import org.gridgain.internal.h2.samples.CsvSample;
import org.gridgain.internal.h2.samples.Function;
import org.gridgain.internal.h2.samples.ReadOnlyDatabaseInZip;
import org.gridgain.internal.h2.store.fs.FileUtils;
import org.gridgain.internal.h2.tools.ChangeFileEncryption;
import org.gridgain.internal.h2.tools.RunScript;
import org.gridgain.internal.h2.test.TestBase;
import org.gridgain.internal.h2.test.TestDb;
import org.gridgain.internal.h2.tools.DeleteDbFiles;
import org.gridgain.internal.h2.util.IOUtils;
import org.gridgain.internal.h2.util.StringUtils;

/**
 * Tests the sample apps.
 */
public class TestSampleApps extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public boolean isEnabled() {
        if (!getBaseDir().startsWith(TestBase.BASE_TEST_DIR)) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws Exception {
        deleteDb(getTestName());
        InputStream in = getClass().getClassLoader().getResourceAsStream(
                "org/gridgain/internal/h2/samples/optimizations.sql");
        new File(getBaseDir()).mkdirs();
        FileOutputStream out = new FileOutputStream(getBaseDir() +
                "/optimizations.sql");
        IOUtils.copyAndClose(in, out);
        String url = "jdbc:gg-h2:" + getBaseDir() + "/" + getTestName();
        testApp("", RunScript.class, "-url", url, "-user", "sa",
                "-password", "sa", "-script", getBaseDir() +
                        "/optimizations.sql", "-checkResults");
        deleteDb(getTestName());
        testApp("Compacting...\nDone.", Compact.class);
        testApp("NAME: Bob Meier\n" +
                "EMAIL: bob.meier@abcde.abc\n" +
                "PHONE: +41123456789\n\n" +
                "NAME: John Jones\n" +
                "EMAIL: john.jones@abcde.abc\n" +
                "PHONE: +41976543210\n",
                CsvSample.class);
        testApp("",
                CachedPreparedStatements.class);
        testApp("2 is prime\n" +
                "3 is prime\n" +
                "5 is prime\n" +
                "7 is prime\n" +
                "11 is prime\n" +
                "13 is prime\n" +
                "17 is prime\n" +
                "19 is prime\n" +
                "30\n" +
                "20\n" +
                "0/0\n" +
                "0/1\n" +
                "1/0\n" +
                "1/1\n" +
                "10",
                Function.class);
        // Not compatible with PostgreSQL JDBC driver (throws a
        // NullPointerException):
        // testApp(org.gridgain.internal.h2.samples.SecurePassword.class, null, "Joe");
        // TODO test ShowProgress (percent numbers are hardware specific)
        // TODO test ShutdownServer (server needs to be started in a separate
        // process)
        testApp(
                "adding test data...\n" +
                "defrag to reduce random access...\n" +
                "create the zip file...\n" +
                "open the database from the zip file...",
                ReadOnlyDatabaseInZip.class);

        // tools
        testApp("Allows changing the database file encryption password or algorithm*",
                ChangeFileEncryption.class, "-help");
        testApp("Deletes all files belonging to a database.*",
                DeleteDbFiles.class, "-help");
        FileUtils.delete(getBaseDir() + "/optimizations.sql");
    }

    private void testApp(String expected, Class<?> clazz, String... args)
            throws Exception {
        DeleteDbFiles.execute("target/data", "test", true);
        Method m = clazz.getMethod("main", String[].class);
        PrintStream oldOut = System.out, oldErr = System.err;
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(buff, false, "UTF-8");
        System.setOut(out);
        System.setErr(out);
        try {
            m.invoke(null, new Object[] { args });
        } catch (InvocationTargetException e) {
            TestBase.logError("error", e.getTargetException());
        } catch (Throwable e) {
            TestBase.logError("error", e);
        }
        out.flush();
        System.setOut(oldOut);
        System.setErr(oldErr);
        String s = new String(buff.toByteArray(), StandardCharsets.UTF_8);
        s = StringUtils.replaceAll(s, "\r\n", "\n");
        s = s.trim();
        expected = expected.trim();
        if (expected.endsWith("*")) {
            expected = expected.substring(0, expected.length() - 1);
            if (!s.startsWith(expected)) {
                assertEquals(expected.trim(), s.trim());
            }
        } else {
            assertEquals(expected.trim(), s.trim());
        }
    }
}
