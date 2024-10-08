/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.gridgain.internal.h2.test;

import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import org.gridgain.internal.h2.store.fs.FilePathRec;
import org.gridgain.internal.h2.store.fs.FileUtils;
import org.gridgain.internal.h2.test.auth.TestAuthentication;
import org.gridgain.internal.h2.test.db.TestAlter;
import org.gridgain.internal.h2.test.db.TestAlterSchemaRename;
import org.gridgain.internal.h2.test.db.TestAnalyzeTableTx;
import org.gridgain.internal.h2.test.db.TestAutoRecompile;
import org.gridgain.internal.h2.test.db.TestBackup;
import org.gridgain.internal.h2.test.db.TestBigDb;
import org.gridgain.internal.h2.test.db.TestBigResult;
import org.gridgain.internal.h2.test.db.TestCases;
import org.gridgain.internal.h2.test.db.TestCheckpoint;
import org.gridgain.internal.h2.test.db.TestCluster;
import org.gridgain.internal.h2.test.db.TestCompatibility;
import org.gridgain.internal.h2.test.db.TestCompatibilityOracle;
import org.gridgain.internal.h2.test.db.TestCompatibilitySQLServer;
import org.gridgain.internal.h2.test.db.TestCsv;
import org.gridgain.internal.h2.test.db.TestDateStorage;
import org.gridgain.internal.h2.test.db.TestDeadlock;
import org.gridgain.internal.h2.test.db.TestDrop;
import org.gridgain.internal.h2.test.db.TestDuplicateKeyUpdate;
import org.gridgain.internal.h2.test.db.TestEncryptedDb;
import org.gridgain.internal.h2.test.db.TestExclusive;
import org.gridgain.internal.h2.test.db.TestFunctionOverload;
import org.gridgain.internal.h2.test.db.TestFunctions;
import org.gridgain.internal.h2.test.db.TestGeneralCommonTableQueries;
import org.gridgain.internal.h2.test.db.TestIndex;
import org.gridgain.internal.h2.test.db.TestIndexHints;
import org.gridgain.internal.h2.test.db.TestLargeBlob;
import org.gridgain.internal.h2.test.db.TestLinkedTable;
import org.gridgain.internal.h2.test.db.TestListener;
import org.gridgain.internal.h2.test.db.TestLob;
import org.gridgain.internal.h2.test.db.TestMemoryUsage;
import org.gridgain.internal.h2.test.db.TestMergeUsing;
import org.gridgain.internal.h2.test.db.TestMultiConn;
import org.gridgain.internal.h2.test.db.TestMultiDimension;
import org.gridgain.internal.h2.test.db.TestMultiThread;
import org.gridgain.internal.h2.test.db.TestMultiThreadedKernel;
import org.gridgain.internal.h2.test.db.TestOpenClose;
import org.gridgain.internal.h2.test.db.TestOptimizations;
import org.gridgain.internal.h2.test.db.TestOptimizerHints;
import org.gridgain.internal.h2.test.db.TestOutOfMemory;
import org.gridgain.internal.h2.test.db.TestPersistentCommonTableExpressions;
import org.gridgain.internal.h2.test.db.TestPowerOff;
import org.gridgain.internal.h2.test.db.TestQueryCache;
import org.gridgain.internal.h2.test.db.TestReadOnly;
import org.gridgain.internal.h2.test.db.TestRecursiveQueries;
import org.gridgain.internal.h2.test.db.TestRights;
import org.gridgain.internal.h2.test.db.TestRowFactory;
import org.gridgain.internal.h2.test.db.TestRunscript;
import org.gridgain.internal.h2.test.db.TestSQLInjection;
import org.gridgain.internal.h2.test.db.TestSelectCountNonNullColumn;
import org.gridgain.internal.h2.test.db.TestSequence;
import org.gridgain.internal.h2.test.db.TestSessionsLocks;
import org.gridgain.internal.h2.test.db.TestSetCollation;
import org.gridgain.internal.h2.test.db.TestSpaceReuse;
import org.gridgain.internal.h2.test.db.TestSpatial;
import org.gridgain.internal.h2.test.db.TestSpeed;
import org.gridgain.internal.h2.test.db.TestSubqueryPerformanceOnLazyExecutionMode;
import org.gridgain.internal.h2.test.db.TestSynonymForTable;
import org.gridgain.internal.h2.test.db.TestTableEngines;
import org.gridgain.internal.h2.test.db.TestTempTables;
import org.gridgain.internal.h2.test.db.TestTransaction;
import org.gridgain.internal.h2.test.db.TestTwoPhaseCommit;
import org.gridgain.internal.h2.test.db.TestUpgrade;
import org.gridgain.internal.h2.test.db.TestUsingIndex;
import org.gridgain.internal.h2.test.db.TestView;
import org.gridgain.internal.h2.test.db.TestViewAlterTable;
import org.gridgain.internal.h2.test.db.TestViewDropView;
import org.gridgain.internal.h2.test.jdbc.TestBatchUpdates;
import org.gridgain.internal.h2.test.jdbc.TestCallableStatement;
import org.gridgain.internal.h2.test.jdbc.TestCancel;
import org.gridgain.internal.h2.test.jdbc.TestConcurrentConnectionUsage;
import org.gridgain.internal.h2.test.jdbc.TestConnection;
import org.gridgain.internal.h2.test.jdbc.TestCustomDataTypesHandler;
import org.gridgain.internal.h2.test.jdbc.TestDatabaseEventListener;
import org.gridgain.internal.h2.test.jdbc.TestDriver;
import org.gridgain.internal.h2.test.jdbc.TestGetGeneratedKeys;
import org.gridgain.internal.h2.test.jdbc.TestJavaObject;
import org.gridgain.internal.h2.test.jdbc.TestJavaObjectSerializer;
import org.gridgain.internal.h2.test.jdbc.TestLimitUpdates;
import org.gridgain.internal.h2.test.jdbc.TestLobApi;
import org.gridgain.internal.h2.test.jdbc.TestManyJdbcObjects;
import org.gridgain.internal.h2.test.jdbc.TestMetaData;
import org.gridgain.internal.h2.test.jdbc.TestNativeSQL;
import org.gridgain.internal.h2.test.jdbc.TestPreparedStatement;
import org.gridgain.internal.h2.test.jdbc.TestResultSet;
import org.gridgain.internal.h2.test.jdbc.TestSQLXML;
import org.gridgain.internal.h2.test.jdbc.TestStatement;
import org.gridgain.internal.h2.test.jdbc.TestTransactionIsolation;
import org.gridgain.internal.h2.test.jdbc.TestUpdatableResultSet;
import org.gridgain.internal.h2.test.jdbc.TestUrlJavaObjectSerializer;
import org.gridgain.internal.h2.test.jdbc.TestZloty;
import org.gridgain.internal.h2.test.mvcc.TestMvcc1;
import org.gridgain.internal.h2.test.mvcc.TestMvcc2;
import org.gridgain.internal.h2.test.mvcc.TestMvcc3;
import org.gridgain.internal.h2.test.mvcc.TestMvcc4;
import org.gridgain.internal.h2.test.mvcc.TestMvccMultiThreaded;
import org.gridgain.internal.h2.test.mvcc.TestMvccMultiThreaded2;
import org.gridgain.internal.h2.test.unit.TestBitStream;
import org.gridgain.internal.h2.test.unit.TestCache;
import org.gridgain.internal.h2.test.unit.TestClearReferences;
import org.gridgain.internal.h2.test.unit.TestCompress;
import org.gridgain.internal.h2.test.unit.TestDataPage;
import org.gridgain.internal.h2.test.unit.TestExit;
import org.gridgain.internal.h2.test.unit.TestFileLock;
import org.gridgain.internal.h2.test.unit.TestFileLockSerialized;
import org.gridgain.internal.h2.test.unit.TestFileSystem;
import org.gridgain.internal.h2.test.unit.TestFtp;
import org.gridgain.internal.h2.test.unit.TestJmx;
import org.gridgain.internal.h2.test.unit.TestLocalResultFactory;
import org.gridgain.internal.h2.test.unit.TestMode;
import org.gridgain.internal.h2.test.unit.TestModifyOnWrite;
import org.gridgain.internal.h2.test.unit.TestNetUtils;
import org.gridgain.internal.h2.test.unit.TestOldVersion;
import org.gridgain.internal.h2.test.unit.TestPageStore;
import org.gridgain.internal.h2.test.unit.TestPattern;
import org.gridgain.internal.h2.test.unit.TestPerfectHash;
import org.gridgain.internal.h2.test.unit.TestReader;
import org.gridgain.internal.h2.test.unit.TestSampleApps;
import org.gridgain.internal.h2.test.unit.TestScriptReader;
import org.gridgain.internal.h2.test.unit.TestSecurity;
import org.gridgain.internal.h2.test.unit.TestShell;
import org.gridgain.internal.h2.test.unit.TestSort;
import org.gridgain.internal.h2.test.unit.TestStringCache;
import org.gridgain.internal.h2.test.unit.TestTimeStampWithTimeZone;
import org.gridgain.internal.h2.test.unit.TestTraceSystem;
import org.gridgain.internal.h2.test.unit.TestValue;
import org.gridgain.internal.h2.Driver;
import org.gridgain.internal.h2.engine.Constants;
import org.gridgain.internal.h2.jdbcx.JdbcDataSourceFactory;
import org.gridgain.internal.h2.test.bench.TestPerformance;
import org.gridgain.internal.h2.test.jdbcx.TestConnectionPool;
import org.gridgain.internal.h2.test.jdbcx.TestDataSource;
import org.gridgain.internal.h2.test.jdbcx.TestXA;
import org.gridgain.internal.h2.test.jdbcx.TestXASimple;
import org.gridgain.internal.h2.test.poweroff.TestReorderWrites;
import org.gridgain.internal.h2.test.recover.RecoverLobTest;
import org.gridgain.internal.h2.test.rowlock.TestRowLocks;
import org.gridgain.internal.h2.test.scripts.TestScript;
import org.gridgain.internal.h2.test.server.TestAutoServer;
import org.gridgain.internal.h2.test.server.TestInit;
import org.gridgain.internal.h2.test.server.TestNestedLoop;
import org.gridgain.internal.h2.test.server.TestWeb;
import org.gridgain.internal.h2.test.store.TestCacheConcurrentLIRS;
import org.gridgain.internal.h2.test.store.TestCacheLIRS;
import org.gridgain.internal.h2.test.store.TestCacheLongKeyLIRS;
import org.gridgain.internal.h2.test.store.TestConcurrent;
import org.gridgain.internal.h2.test.store.TestDataUtils;
import org.gridgain.internal.h2.test.store.TestDefrag;
import org.gridgain.internal.h2.test.store.TestFreeSpace;
import org.gridgain.internal.h2.test.store.TestKillProcessWhileWriting;
import org.gridgain.internal.h2.test.store.TestMVRTree;
import org.gridgain.internal.h2.test.store.TestMVStore;
import org.gridgain.internal.h2.test.store.TestMVStoreBenchmark;
import org.gridgain.internal.h2.test.store.TestMVStoreStopCompact;
import org.gridgain.internal.h2.test.store.TestMVStoreTool;
import org.gridgain.internal.h2.test.store.TestMVTableEngine;
import org.gridgain.internal.h2.test.store.TestObjectDataType;
import org.gridgain.internal.h2.test.store.TestRandomMapOps;
import org.gridgain.internal.h2.test.store.TestSpinLock;
import org.gridgain.internal.h2.test.store.TestStreamStore;
import org.gridgain.internal.h2.test.store.TestTransactionStore;
import org.gridgain.internal.h2.test.synth.TestBtreeIndex;
import org.gridgain.internal.h2.test.synth.TestConcurrentUpdate;
import org.gridgain.internal.h2.test.synth.TestCrashAPI;
import org.gridgain.internal.h2.test.synth.TestDiskFull;
import org.gridgain.internal.h2.test.synth.TestFuzzOptimizations;
import org.gridgain.internal.h2.test.synth.TestHaltApp;
import org.gridgain.internal.h2.test.synth.TestJoin;
import org.gridgain.internal.h2.test.synth.TestKill;
import org.gridgain.internal.h2.test.synth.TestKillRestart;
import org.gridgain.internal.h2.test.synth.TestKillRestartMulti;
import org.gridgain.internal.h2.test.synth.TestLimit;
import org.gridgain.internal.h2.test.synth.TestMultiThreaded;
import org.gridgain.internal.h2.test.synth.TestNestedJoins;
import org.gridgain.internal.h2.test.synth.TestOuterJoins;
import org.gridgain.internal.h2.test.synth.TestRandomCompare;
import org.gridgain.internal.h2.test.synth.TestRandomSQL;
import org.gridgain.internal.h2.test.synth.TestStringAggCompatibility;
import org.gridgain.internal.h2.test.synth.TestTimer;
import org.gridgain.internal.h2.test.synth.sql.TestSynth;
import org.gridgain.internal.h2.test.synth.thread.TestMulti;
import org.gridgain.internal.h2.test.unit.TestAnsCompression;
import org.gridgain.internal.h2.test.unit.TestBinaryArithmeticStream;
import org.gridgain.internal.h2.test.unit.TestBnf;
import org.gridgain.internal.h2.test.unit.TestCharsetCollator;
import org.gridgain.internal.h2.test.unit.TestCollation;
import org.gridgain.internal.h2.test.unit.TestConnectionInfo;
import org.gridgain.internal.h2.test.unit.TestDate;
import org.gridgain.internal.h2.test.unit.TestDateIso8601;
import org.gridgain.internal.h2.test.unit.TestDateTimeUtils;
import org.gridgain.internal.h2.test.unit.TestDbException;
import org.gridgain.internal.h2.test.unit.TestFile;
import org.gridgain.internal.h2.test.unit.TestGeometryUtils;
import org.gridgain.internal.h2.test.unit.TestIntArray;
import org.gridgain.internal.h2.test.unit.TestIntIntHashMap;
import org.gridgain.internal.h2.test.unit.TestIntPerfectHash;
import org.gridgain.internal.h2.test.unit.TestInterval;
import org.gridgain.internal.h2.test.unit.TestKeywords;
import org.gridgain.internal.h2.test.unit.TestLocale;
import org.gridgain.internal.h2.test.unit.TestMVTempResult;
import org.gridgain.internal.h2.test.unit.TestMathUtils;
import org.gridgain.internal.h2.test.unit.TestMemoryUnmapper;
import org.gridgain.internal.h2.test.unit.TestObjectDeserialization;
import org.gridgain.internal.h2.test.unit.TestOverflow;
import org.gridgain.internal.h2.test.unit.TestPageStoreCoverage;
import org.gridgain.internal.h2.test.unit.TestPgServer;
import org.gridgain.internal.h2.test.unit.TestRecovery;
import org.gridgain.internal.h2.test.unit.TestReopen;
import org.gridgain.internal.h2.test.unit.TestStreams;
import org.gridgain.internal.h2.test.unit.TestStringUtils;
import org.gridgain.internal.h2.test.unit.TestUtils;
import org.gridgain.internal.h2.test.unit.TestValueMemory;
import org.gridgain.internal.h2.test.utils.OutputCatcher;
import org.gridgain.internal.h2.test.utils.SelfDestructor;
import org.gridgain.internal.h2.test.utils.TestColumnNamer;
import org.gridgain.internal.h2.tools.DeleteDbFiles;
import org.gridgain.internal.h2.tools.Server;
import org.gridgain.internal.h2.util.AbbaLockingDetector;
import org.gridgain.internal.h2.util.Profiler;
import org.gridgain.internal.h2.util.StringUtils;
import org.gridgain.internal.h2.util.Task;
import org.gridgain.internal.h2.util.ThreadDeadlockDetector;
import org.gridgain.internal.h2.util.Utils;

/**
 * The main test application. JUnit is not used because loops are easier to
 * write in regular java applications (most tests are ran multiple times using
 * different settings).
 */
public class TestAll {

    static {
        // Locale.setDefault(new Locale("ru", "ru"));
    }

/*

PIT test:
java org.pitest.mutationtest.MutationCoverageReport
--reportDir data --targetClasses org.gridgain.internal.h2.dev.store.btree.StreamStore*
--targetTests org.gridgain.internal.h2.test.store.TestStreamStore
--sourceDirs src/test,src/tools

Dump heap on out of memory:
-XX:+HeapDumpOnOutOfMemoryError

Random test:
java15
cd h2database/h2/bin
del *.db
start cmd /k "java -cp .;%H2DRIVERS% org.gridgain.internal.h2.test.TestAll join >testJoin.txt"
start cmd /k "java -cp . org.gridgain.internal.h2.test.TestAll synth >testSynth.txt"
start cmd /k "java -cp . org.gridgain.internal.h2.test.TestAll all >testAll.txt"
start cmd /k "java -cp . org.gridgain.internal.h2.test.TestAll random >testRandom.txt"
start cmd /k "java -cp . org.gridgain.internal.h2.test.TestAll btree >testBtree.txt"
start cmd /k "java -cp . org.gridgain.internal.h2.test.TestAll halt >testHalt.txt"
java -cp . org.gridgain.internal.h2.test.TestAll crash >testCrash.txt

java org.gridgain.internal.h2.test.TestAll timer

*/

    /**
     * Set to true if any of the tests fail. Used to return an error code from
     * the whole program.
     */
    static boolean atLeastOneTestFailed;

    /**
     * Whether the MVStore storage is used.
     */
    public boolean mvStore = true;

    /**
     * If the test should run with many rows.
     */
    public boolean big;

    /**
     * If remote database connections should be used.
     */
    public boolean networked;

    /**
     * If in-memory databases should be used.
     */
    public boolean memory;

    /**
     * If code coverage is enabled.
     */
    public boolean codeCoverage;

    /**
     * If the multi-threaded mode should be used.
     */
    public boolean multiThreaded;

    /**
     * If lazy queries should be used.
     */
    public boolean lazy;

    /**
     * The cipher to use (null for unencrypted).
     */
    public String cipher;

    /**
     * The file trace level value to use.
     */
    public int traceLevelFile;

    /**
     * If test trace information should be written (for debugging only).
     */
    public boolean traceTest;

    /**
     * If testing on Google App Engine.
     */
    public boolean googleAppEngine;

    /**
     * If a small cache and a low number for MAX_MEMORY_ROWS should be used.
     */
    public boolean diskResult;

    /**
     * Test using the recording file system.
     */
    public boolean reopen;

    /**
     * Test the split file system.
     */
    public boolean splitFileSystem;

    /**
     * If only fast/CI/Jenkins/Travis tests should be run.
     */
    public boolean travis;

    /**
     * the vmlens.com race condition tool
     */
    public boolean vmlens;

    /**
     * The lock timeout to use
     */
    public int lockTimeout = 50;

    /**
     * If the transaction log should be kept small (that is, the log should be
     * switched early).
     */
    boolean smallLog;

    /**
     * If SSL should be used for remote connections.
     */
    boolean ssl;

    /**
     * If MAX_MEMORY_UNDO=3 should be used.
     */
    public boolean diskUndo;

    /**
     * If TRACE_LEVEL_SYSTEM_OUT should be set to 2 (for debugging only).
     */
    boolean traceSystemOut;

    /**
     * If the tests should run forever.
     */
    boolean endless;

    /**
     * The THROTTLE value to use.
     */
    public int throttle;

    /**
     * The THROTTLE value to use by default.
     */
    int throttleDefault = Integer.parseInt(System.getProperty("throttle", "0"));

    /**
     * If the test should stop when the first error occurs.
     */
    public boolean stopOnError;

    /**
     * If the database should always be defragmented when closing.
     */
    public boolean defrag;

    /**
     * The cache type.
     */
    public String cacheType;

    /** If not null the database should be opened with the collation parameter */
    public String collation;


    /**
     * The AB-BA locking detector.
     */
    AbbaLockingDetector abbaLockingDetector;

    /**
     * The list of tests.
     */
    ArrayList<TestBase> tests = new ArrayList<>();

    private Server server;

    /**
     * The map of executed tests to detect not executed tests.
     * Boolean value is 'false' for a disabled test.
     */
    HashMap<Class<? extends TestBase>, Boolean> executedTests = new HashMap<>();

    /**
     * Run all tests.
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws Exception {
        OutputCatcher catcher = OutputCatcher.start();
        run(args);
        catcher.stop();
        catcher.writeTo("Test Output", "docs/html/testOutput.html");
        if (atLeastOneTestFailed) {
            System.exit(1);
        }
    }

    private static void run(String... args) throws Exception {
        SelfDestructor.startCountdown(4 * 60);
        long time = System.nanoTime();
        printSystemInfo();

        // use lower values, to better test those cases,
        // and (for delays) to speed up the tests

        System.setProperty("h2.maxMemoryRows", "100");

        System.setProperty("h2.delayWrongPasswordMin", "0");
        System.setProperty("h2.delayWrongPasswordMax", "0");
        System.setProperty("h2.useThreadContextClassLoader", "true");

        // System.setProperty("h2.modifyOnWrite", "true");

        // speedup
        // System.setProperty("h2.syncMethod", "");

/*

recovery tests with small freeList pages, page size 64

reopen org.gridgain.internal.h2.test.unit.TestPageStore
-Xmx1500m -D reopenOffset=3 -D reopenShift=1

power failure test
power failure test: MULTI_THREADED=TRUE
power failure test: larger binaries and additional index.
power failure test with randomly generating / dropping indexes and tables.

drop table test;
create table test(id identity, name varchar(100) default space(100));
@LOOP 10 insert into test select null, null from system_range(1, 100000);
delete from test;

documentation: review package and class level javadocs
documentation: rolling review at main.html

-------------

remove old TODO, move to roadmap

kill a test:
kill -9 `jps -l | grep "org.gridgain.internal.h2.test." | cut -d " " -f 1`

*/
        TestAll test = new TestAll();
        if (args.length > 0) {
            if ("travis".equals(args[0])) {
                test.travis = true;
                test.testAll();
            } else if ("vmlens".equals(args[0])) {
                test.vmlens = true;
                test.testAll();
            } else if ("reopen".equals(args[0])) {
                System.setProperty("h2.delayWrongPasswordMin", "0");
                System.setProperty("h2.analyzeAuto", "100");
                System.setProperty("h2.pageSize", "64");
                System.setProperty("h2.reopenShift", "5");
                FilePathRec.register();
                test.reopen = true;
                TestReopen reopen = new TestReopen();
                reopen.init();
                FilePathRec.setRecorder(reopen);
                test.runTests();
            } else if ("crash".equals(args[0])) {
                test.endless = true;
                new TestCrashAPI().runTest(test);
            } else if ("synth".equals(args[0])) {
                new TestSynth().runTest(test);
            } else if ("kill".equals(args[0])) {
                new TestKill().runTest(test);
            } else if ("random".equals(args[0])) {
                test.endless = true;
                new TestRandomSQL().runTest(test);
            } else if ("join".equals(args[0])) {
                new TestJoin().runTest(test);
                test.endless = true;
            } else if ("btree".equals(args[0])) {
                new TestBtreeIndex().runTest(test);
            } else if ("all".equals(args[0])) {
                test.testEverything();
            } else if ("codeCoverage".equals(args[0])) {
                test.codeCoverage = true;
                test.runCoverage();
            } else if ("multiThread".equals(args[0])) {
                new TestMulti().runTest(test);
            } else if ("halt".equals(args[0])) {
                new TestHaltApp().runTest(test);
            } else if ("timer".equals(args[0])) {
                new TestTimer().runTest(test);
            }
        } else {
            test.testAll();
        }
        System.out.println(TestBase.formatTime(
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time)) + " total");
    }

    private void testAll() throws Exception {
        runTests();
        if (!travis && !vmlens) {
            Profiler prof = new Profiler();
            prof.depth = 16;
            prof.interval = 1;
            prof.startCollecting();
            TestPerformance.main("-init", "-db", "1", "-size", "1000");
            prof.stopCollecting();
            System.out.println(prof.getTop(5));
            TestPerformance.main("-init", "-db", "1", "-size", "1000");
        }
    }

    /**
     * Run all tests in all possible combinations.
     */
    private void testEverything() throws SQLException {
        for (int c = 0; c < 2; c++) {
            if (c == 0) {
                cipher = null;
            } else {
                cipher = "AES";
            }
            for (int a = 0; a < 64; a++) {
                smallLog = (a & 1) != 0;
                big = (a & 2) != 0;
                networked = (a & 4) != 0;
                memory = (a & 8) != 0;
                ssl = (a & 16) != 0;
                diskResult = (a & 32) != 0;
                for (int trace = 0; trace < 3; trace++) {
                    traceLevelFile = trace;
                    test();
                }
            }
        }
    }

    /**
     * Run the tests with a number of different settings.
     */
    private void runTests() throws SQLException {

        if (Boolean.getBoolean("abba")) {
            abbaLockingDetector = new AbbaLockingDetector().startCollecting();
        }

        smallLog = big = networked = memory = ssl = false;
        diskResult = traceSystemOut = diskUndo = false;
        traceTest = stopOnError = false;
        defrag = false;
        traceLevelFile = throttle = 0;
        cipher = null;

        // memory is a good match for multi-threaded, makes things happen
        // faster, more chance of exposing race conditions
        memory = true;
        multiThreaded = true;
        test();
        if (vmlens) {
            return;
        }
        testAdditional();

        // test utilities
        big = !travis;
        testUtils();
        big = false;

        // lazy
        lazy = true;
        memory = true;
        multiThreaded = true;
        test();
        lazy = false;

        // but sometimes race conditions need bigger windows
        memory = false;
        multiThreaded = true;
        test();
        testAdditional();

        // a more normal setup
        memory = false;
        multiThreaded = false;
        test();
        testAdditional();

        // basic pagestore testing
        memory = false;
        multiThreaded = false;
        mvStore = false;
        test();
        testAdditional();

        mvStore = true;
        memory = true;
        multiThreaded = false;
        networked = true;
        test();

        memory = false;
        networked = false;
        diskUndo = true;
        diskResult = true;
        traceLevelFile = 3;
        throttle = 1;
        cacheType = "SOFT_LRU";
        cipher = "AES";
        test();

        diskUndo = false;
        diskResult = false;
        traceLevelFile = 1;
        throttle = 0;
        cacheType = null;
        cipher = null;
        defrag = true;
        test();

        if (!travis) {
            traceLevelFile = 0;
            smallLog = true;
            networked = true;
            defrag = false;
            ssl = true;
            test();

            big = true;
            smallLog = false;
            networked = false;
            ssl = false;
            traceLevelFile = 0;
            test();
            testAdditional();

            big = false;
            cipher = "AES";
            test();
            cipher = null;
            test();
        }

        for (Entry<Class<? extends TestBase>, Boolean> entry : executedTests.entrySet()) {
            if (!entry.getValue()) {
                System.out.println("Warning: test " + entry.getKey().getName() + " was not executed.");
            }
        }
    }

    private void runCoverage() throws SQLException {
        smallLog = big = networked = memory = ssl = false;
        diskResult = traceSystemOut = diskUndo = false;
        traceTest = stopOnError = false;
        defrag = false;
        traceLevelFile = throttle = 0;
        cipher = null;

        memory = true;
        multiThreaded = true;
        test();
        testAdditional();
        testUtils();

        multiThreaded = false;
        mvStore = false;
        test();
        // testUnit();
    }

    /**
     * Run all tests with the current settings.
     */
    protected void test() throws SQLException {
        System.out.println();
        System.out.println("Test " + toString() +
                " (" + Utils.getMemoryUsed() + " KB used)");
        beforeTest();

        // db
        addTest(new TestScript());
        addTest(new TestAlter());
        addTest(new TestAlterSchemaRename());
        addTest(new TestAutoRecompile());
        addTest(new TestBackup());
        addTest(new TestBigDb());
        addTest(new TestBigResult());
        addTest(new TestCases());
        addTest(new TestCheckpoint());
        addTest(new TestCompatibility());
        addTest(new TestCompatibilityOracle());
        addTest(new TestCompatibilitySQLServer());
        addTest(new TestCsv());
        addTest(new TestDeadlock());
        if (vmlens) {
            return;
        }
        addTest(new TestDrop());
        addTest(new TestDuplicateKeyUpdate());
        addTest(new TestEncryptedDb());
        addTest(new TestExclusive());
        addTest(new TestFunctionOverload());
        addTest(new TestFunctions());
        addTest(new TestInit());
        addTest(new TestIndex());
        addTest(new TestIndexHints());
        addTest(new TestLargeBlob());
        addTest(new TestLinkedTable());
        addTest(new TestListener());
        addTest(new TestLob());
        addTest(new TestMergeUsing());
        addTest(new TestMultiConn());
        addTest(new TestMultiDimension());
        addTest(new TestMultiThreadedKernel());
        addTest(new TestOpenClose());
        addTest(new TestOptimizerHints());
        addTest(new TestReadOnly());
        addTest(new TestRecursiveQueries());
        addTest(new TestGeneralCommonTableQueries());
        if (!memory) {
            // requires persistent store for reconnection tests
            addTest(new TestPersistentCommonTableExpressions());
        }
        addTest(new TestRights());
        addTest(new TestRunscript());
        addTest(new TestSQLInjection());
        addTest(new TestSessionsLocks());
        addTest(new TestSelectCountNonNullColumn());
        addTest(new TestSequence());
        addTest(new TestSpaceReuse());
        addTest(new TestSpatial());
        addTest(new TestSpeed());
        addTest(new TestTableEngines());
        addTest(new TestRowFactory());
        addTest(new TestTempTables());
        addTest(new TestTransaction());
        addTest(new TestTwoPhaseCommit());
        addTest(new TestView());
        addTest(new TestViewAlterTable());
        addTest(new TestViewDropView());
        addTest(new TestSynonymForTable());
        addTest(new TestColumnNamer());

        // jdbc
        addTest(new TestBatchUpdates());
        addTest(new TestCallableStatement());
        addTest(new TestCancel());
        addTest(new TestConcurrentConnectionUsage());
        addTest(new TestConnection());
        addTest(new TestDatabaseEventListener());
        addTest(new TestJavaObject());
        addTest(new TestLimitUpdates());
        addTest(new TestLobApi());
        addTest(new TestSQLXML());
        addTest(new TestManyJdbcObjects());
        addTest(new TestMetaData());
        addTest(new TestNativeSQL());
        addTest(new TestPreparedStatement());
        addTest(new TestResultSet());
        addTest(new TestStatement());
        addTest(new TestGetGeneratedKeys());
        addTest(new TestTransactionIsolation());
        addTest(new TestUpdatableResultSet());
        addTest(new TestZloty());
        addTest(new TestCustomDataTypesHandler());
        addTest(new TestSetCollation());

        // jdbcx
        addTest(new TestConnectionPool());
        addTest(new TestDataSource());
        addTest(new TestXA());
        addTest(new TestXASimple());

        // server
        addTest(new TestAutoServer());
        addTest(new TestNestedLoop());

        // mvcc & row level locking
        addTest(new TestMvcc1());
        addTest(new TestMvcc2());
        addTest(new TestMvcc3());
        addTest(new TestMvcc4());
        addTest(new TestMvccMultiThreaded());
        addTest(new TestMvccMultiThreaded2());
        addTest(new TestRowLocks());
        addTest(new TestAnalyzeTableTx());

        // synth
        addTest(new TestBtreeIndex());
        addTest(new TestConcurrentUpdate());
        addTest(new TestDiskFull());
        addTest(new TestCrashAPI());
        addTest(new TestFuzzOptimizations());
        addTest(new TestLimit());
        addTest(new TestRandomCompare());
        addTest(new TestKillRestart());
        addTest(new TestKillRestartMulti());
        addTest(new TestMultiThreaded());
        addTest(new TestOuterJoins());
        addTest(new TestNestedJoins());
        addTest(new TestStringAggCompatibility());

        runAddedTests();

        // serial
        addTest(new TestDateStorage());
        addTest(new TestDriver());
        addTest(new TestJavaObjectSerializer());
        addTest(new TestLocale());
        addTest(new TestMemoryUsage());
        addTest(new TestMultiThread());
        addTest(new TestPowerOff());
        addTest(new TestReorderWrites());
        addTest(new TestRandomSQL());
        addTest(new TestQueryCache());
        addTest(new TestUrlJavaObjectSerializer());
        addTest(new TestWeb());

        // other unsafe
        addTest(new TestOptimizations());
        addTest(new TestOutOfMemory());

        runAddedTests(1);

        afterTest();
    }

    /**
     * Run additional tests.
     */
    protected void testAdditional() {
        if (networked) {
            throw new RuntimeException("testAdditional() is not allowed in networked mode");
        }

        addTest(new TestMVTableEngine());
//        addTest(new TestAutoReconnect());
        addTest(new TestBnf());
        addTest(new TestCache());
        addTest(new TestCollation());
        addTest(new TestCompress());
        addTest(new TestConnectionInfo());
        addTest(new TestExit());
        addTest(new TestFileLock());
        addTest(new TestJmx());
        addTest(new TestModifyOnWrite());
        addTest(new TestOldVersion());
        addTest(new TestMultiThreadedKernel());
        addTest(new TestPageStore());
        addTest(new TestPageStoreCoverage());
        addTest(new TestPgServer());
        addTest(new TestRecovery());
        addTest(new RecoverLobTest());
        addTest(createTest("org.gridgain.internal.h2.test.unit.TestServlet"));
        addTest(new TestTimeStampWithTimeZone());
        addTest(new TestUpgrade());
        addTest(new TestUsingIndex());
        addTest(new TestValue());
        addTest(new TestWeb());

        runAddedTests();

        addTest(new TestCluster());
        addTest(new TestFileLockSerialized());
//        addTest(new TestFileLockProcess());
        addTest(new TestDefrag());
//        addTest(new TestTools());
        addTest(new TestSampleApps());
        addTest(new TestSubqueryPerformanceOnLazyExecutionMode());

        runAddedTests(1);
    }

    /**
     * Run tests for utilities.
     */
    protected void testUtils() {
        System.out.println();
        System.out.println("Test utilities (" + Utils.getMemoryUsed() + " KB used)");

        // mv store
        addTest(new TestCacheConcurrentLIRS());
        addTest(new TestCacheLIRS());
        addTest(new TestCacheLongKeyLIRS());
        addTest(new TestDataUtils());
        addTest(new TestFreeSpace());
        addTest(new TestKillProcessWhileWriting());
        addTest(new TestMVRTree());
        addTest(new TestMVStore());
        addTest(new TestMVStoreBenchmark());
        addTest(new TestMVStoreStopCompact());
        addTest(new TestMVStoreTool());
        addTest(new TestObjectDataType());
        addTest(new TestRandomMapOps());
        addTest(new TestSpinLock());
        addTest(new TestStreamStore());
        addTest(new TestTransactionStore());
        addTest(new TestMVTempResult());

        // unit
        addTest(new TestAnsCompression());
        addTest(new TestBinaryArithmeticStream());
        addTest(new TestBitStream());
        addTest(new TestCharsetCollator());
        addTest(new TestClearReferences());
        addTest(new TestDataPage());
        addTest(new TestDateIso8601());
        addTest(new TestDbException());
        addTest(new TestFile());
        addTest(new TestFileSystem());
        addTest(new TestFtp());
        addTest(new TestGeometryUtils());
        addTest(new TestInterval());
        addTest(new TestIntArray());
        addTest(new TestIntIntHashMap());
        addTest(new TestIntPerfectHash());
        addTest(new TestKeywords());
        addTest(new TestMathUtils());
        addTest(new TestMemoryUnmapper());
        addTest(new TestMode());
        addTest(new TestObjectDeserialization());
        addTest(new TestOverflow());
        addTest(new TestPerfectHash());
        addTest(new TestReader());
        addTest(new TestScriptReader());
        addTest(new TestSecurity());
        addTest(new TestShell());
        addTest(new TestSort());
        addTest(new TestStreams());
        addTest(new TestStringUtils());
        addTest(new TestTraceSystem());
        addTest(new TestUtils());
        addTest(new TestLocalResultFactory());

        runAddedTests();

        // serial
        addTest(new TestDate());
        addTest(new TestDateTimeUtils());
        addTest(new TestConcurrent());
        addTest(new TestNetUtils());
        addTest(new TestPattern());
        addTest(new TestStringCache());
        addTest(new TestValueMemory());
        addTest(new TestAuthentication());

        runAddedTests(1);
    }

    protected void addTest(TestBase test) {
        // tests.add(test);
        // run directly for now, because concurrently running tests
        // fails on Raspberry Pi quite often (seems to be a JVM problem)

        // event queue watchdog for tests that get stuck when running in Jenkins
        final java.util.Timer watchdog = new java.util.Timer();
        // 5 minutes
        watchdog.schedule(new TimerTask() {
            @Override
            public void run() {
                ThreadDeadlockDetector.dumpAllThreadsAndLocks("test watchdog timed out");
            }
        }, 5 * 60 * 1000);
        try {
            test.runTest(this);
        } finally {
            watchdog.cancel();
        }
    }

    private void runAddedTests() {
        int threadCount = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
        // threadCount = 2;
        runAddedTests(threadCount);
    }

    private void runAddedTests(int threadCount) {
        Task[] tasks = new Task[threadCount];
        for (int i = 0; i < threadCount; i++) {
            Task t = new Task() {
                @Override
                public void call() throws Exception {
                    while (true) {
                        TestBase test;
                        synchronized (tests) {
                            if (tests.isEmpty()) {
                                break;
                            }
                            test = tests.remove(0);
                        }
                        test.runTest(TestAll.this);
                    }
                }
            };
            t.execute();
            tasks[i] = t;
        }
        for (Task t : tasks) {
            t.get();
        }
    }

    private static TestBase createTest(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return (TestBase) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception | NoClassDefFoundError e) {
            // ignore
            TestBase.printlnWithTime(0, className + " class not found");
        }
        return new TestBase() {

            @Override
            public void test() throws Exception {
                // ignore
            }

        };
    }

    /**
     * This method is called before a complete set of tests is run. It deletes
     * old database files in the test directory and trace files. It also starts
     * a TCP server if the test uses remote connections.
     */
    public void beforeTest() throws SQLException {
        Driver.load();
        FileUtils.deleteRecursive(TestBase.BASE_TEST_DIR, true);
        DeleteDbFiles.execute(TestBase.BASE_TEST_DIR, null, true);
        FileUtils.deleteRecursive("target/trace.db", false);
        if (networked) {
            String[] args = ssl ? new String[] { "-ifNotExists", "-tcpSSL" } : new String[] { "-ifNotExists" };
            server = Server.createTcpServer(args);
            try {
                server.start();
            } catch (SQLException e) {
                System.out.println("FAIL: can not start server (may already be running)");
                server = null;
            }
        }
    }

    /**
     * Stop the server if it was started.
     */
    public void afterTest() {
        if (networked && server != null) {
            server.stop();
        }
        JdbcDataSourceFactory.getTraceSystem().close();
        FileUtils.deleteRecursive("target/trace.db", true);
        FileUtils.deleteRecursive(TestBase.BASE_TEST_DIR, true);
    }

    public int getPort() {
        return server == null ? 9192 : server.getPort();
    }

    /**
     * Print system information.
     */
    public static void printSystemInfo() {
        Properties prop = System.getProperties();
        System.out.println("H2 " + Constants.getFullVersion() +
                " @ " + new java.sql.Timestamp(System.currentTimeMillis()).toString());
        System.out.println("Java " +
                prop.getProperty("java.runtime.version") + ", " +
                prop.getProperty("java.vm.name")+", " +
                prop.getProperty("java.vendor") + ", " +
                prop.getProperty("sun.arch.data.model"));
        System.out.println(
                prop.getProperty("os.name") + ", " +
                prop.getProperty("os.arch")+", "+
                prop.getProperty("os.version")+", "+
                prop.getProperty("sun.os.patch.level")+", "+
                prop.getProperty("file.separator")+" "+
                prop.getProperty("path.separator")+" "+
                StringUtils.javaEncode(prop.getProperty("line.separator")) + " " +
                prop.getProperty("user.country") + " " +
                prop.getProperty("user.language") + " " +
                prop.getProperty("user.timezone") + " " +
                prop.getProperty("user.variant")+" "+
                prop.getProperty("file.encoding"));
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        appendIf(buff, lazy, "lazy");
        if (mvStore) {
            buff.append("mvStore ");
        } else {
            buff.append("pageStore ");
        }
        appendIf(buff, big, "big");
        appendIf(buff, networked, "net");
        appendIf(buff, memory, "memory");
        appendIf(buff, codeCoverage, "codeCoverage");
        appendIf(buff, multiThreaded, "multiThreaded");
        appendIf(buff, cipher != null, cipher);
        appendIf(buff, cacheType != null, cacheType);
        appendIf(buff, smallLog, "smallLog");
        appendIf(buff, ssl, "ssl");
        appendIf(buff, diskUndo, "diskUndo");
        appendIf(buff, diskResult, "diskResult");
        appendIf(buff, traceSystemOut, "traceSystemOut");
        appendIf(buff, endless, "endless");
        appendIf(buff, traceLevelFile > 0, "traceLevelFile");
        appendIf(buff, throttle > 0, "throttle:" + throttle);
        appendIf(buff, traceTest, "traceTest");
        appendIf(buff, stopOnError, "stopOnError");
        appendIf(buff, defrag, "defrag");
        appendIf(buff, splitFileSystem, "split");
        appendIf(buff, collation != null, collation);
        return buff.toString();
    }

    private static void appendIf(StringBuilder buff, boolean flag, String text) {
        if (flag) {
            buff.append(text);
            buff.append(' ');
        }
    }

}
