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

package org.apache.ignite.development.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.lang.String.valueOf;
import static java.lang.System.setOut;
import static java.lang.System.setProperty;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.development.utils.IgniteWalConverter.PRINT_RECORDS;
import static org.apache.ignite.development.utils.IgniteWalConverter.SENSITIVE_DATA;
import static org.apache.ignite.development.utils.ProcessSensitiveDataUtils.md5;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.wal.record.RecordUtils.isLogEnabled;

/**
 * Class for testing sensitive data when reading {@link WALRecord} using
 * {@link IgniteWalConverter}.
 */
public class IgniteWalConverterSensitiveDataTest extends GridCommonAbstractTest {
    /** Sensitive data. */
    private static final String SENSITIVE_DATA_VALUE = "must_hide_it";

    /** Path to directory where WAL is stored. */
    private static String WAL_DIR_PATH;

    /** Page size. */
    private static int PAGE_SIZE;

    /** System out. */
    private static PrintStream SYS_OUT;

    /**
     * Test out - can be injected via {@link #injectTestSystemOut()} instead
     * of System.out and analyzed in test.
     */
    private static ByteArrayOutputStream TEST_OUT;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        SYS_OUT = System.out;
        TEST_OUT = new ByteArrayOutputStream(16 * 1024);

        int nodeId = 0;

        IgniteEx crd = startGrid(nodeId);
        crd.cluster().active(true);

        try (Transaction tx = crd.transactions().txStart()) {
            crd.cache(DEFAULT_CACHE_NAME).put(SENSITIVE_DATA_VALUE, SENSITIVE_DATA_VALUE);
            tx.commit();
        }

        GridKernalContext kernalCtx = crd.context();
        IgniteWriteAheadLogManager wal = kernalCtx.cache().context().wal();

        for (WALRecord walRecord : withSensitiveData()) {
            if (isLogEnabled(walRecord))
                wal.log(walRecord);
        }

        wal.flush(null, true);

        IgniteConfiguration cfg = crd.configuration();

        String wd = cfg.getWorkDirectory();
        String wp = cfg.getDataStorageConfiguration().getWalPath();
        String fn = kernalCtx.pdsFolderResolver().resolveFolders().folderName();

        WAL_DIR_PATH = wd + File.separator + wp + File.separator + fn;
        PAGE_SIZE = cfg.getDataStorageConfiguration().getPageSize();

        stopGrid(nodeId);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clearGridToStringClassCache();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        log.info("Test output for " + currentTestMethod());
        log.info("----------------------------------------");

        setOut(SYS_OUT);

        log.info(TEST_OUT.toString());
        resetTestOut();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )).setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setAtomicityMode(TRANSACTIONAL));
    }

    /**
     * Test checks that by default {@link WALRecord} will not be output without
     * system option {@link IgniteWalConverter#PRINT_RECORDS}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotPrintRecordsByDefault() throws Exception {
        injectTestSystemOut();

        IgniteWalConverter.main(new String[] {valueOf(PAGE_SIZE), WAL_DIR_PATH});

        assertNotContains(log, TEST_OUT.toString(), SENSITIVE_DATA_VALUE);
    }

    /**
     * Test checks that by default sensitive data is displayed.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = PRINT_RECORDS, value = "true")
    public void testShowSensitiveDataByDefault() throws Exception {
        injectTestSystemOut();

        IgniteWalConverter.main(new String[] {valueOf(PAGE_SIZE), WAL_DIR_PATH});

        assertContains(log, TEST_OUT.toString(), SENSITIVE_DATA_VALUE);
    }

    /**
     * Test checks that sensitive data is displayed.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = PRINT_RECORDS, value = "true")
    @WithSystemProperty(key = SENSITIVE_DATA, value = "SHOW")
    public void testShowSensitiveData() throws Exception {
        injectTestSystemOut();

        IgniteWalConverter.main(new String[] {valueOf(PAGE_SIZE), WAL_DIR_PATH});
        assertContains(log, TEST_OUT.toString(), SENSITIVE_DATA_VALUE);

        setProperty(SENSITIVE_DATA, currentTestMethod().getName());
        resetTestOut();

        IgniteWalConverter.main(new String[] {valueOf(PAGE_SIZE), WAL_DIR_PATH});
        assertContains(log, TEST_OUT.toString(), SENSITIVE_DATA_VALUE);
    }

    /**
     * Test verifies that sensitive data will be hidden.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = PRINT_RECORDS, value = "true")
    @WithSystemProperty(key = SENSITIVE_DATA, value = "HIDE")
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "true")
    public void testHideSensitiveData() throws Exception {
        injectTestSystemOut();

        IgniteWalConverter.main(new String[] {valueOf(PAGE_SIZE), WAL_DIR_PATH});

        assertNotContains(log, TEST_OUT.toString(), SENSITIVE_DATA_VALUE);
    }

    /**
     * Test verifies that sensitive data should be replaced with hash.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = PRINT_RECORDS, value = "true")
    @WithSystemProperty(key = SENSITIVE_DATA, value = "HASH")
    public void testHashSensitiveData() throws Exception {
        injectTestSystemOut();

        IgniteWalConverter.main(new String[] {valueOf(PAGE_SIZE), WAL_DIR_PATH});

        String testOut = TEST_OUT.toString();

        assertNotContains(log, testOut, SENSITIVE_DATA_VALUE);
        assertContains(log, testOut, valueOf(SENSITIVE_DATA_VALUE.hashCode()));
    }

    /**
     * Test verifies that sensitive data should be replaced with MD5 hash.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = PRINT_RECORDS, value = "true")
    @WithSystemProperty(key = SENSITIVE_DATA, value = "MD5")
    public void testMd5HashSensitiveData() throws Exception {
        injectTestSystemOut();

        IgniteWalConverter.main(new String[] {valueOf(PAGE_SIZE), WAL_DIR_PATH});

        String testOut = TEST_OUT.toString();

        assertNotContains(log, testOut, SENSITIVE_DATA_VALUE);
        assertContains(log, testOut, md5(SENSITIVE_DATA_VALUE));
    }

    /**
     * Inject {@link #TEST_OUT} to System.out for analyze in test.
     */
    private void injectTestSystemOut() {
        setOut(new PrintStream(TEST_OUT));
    }

    /**
     * Reset {@link #TEST_OUT}.
     */
    private void resetTestOut() {
        TEST_OUT.reset();
    }

    /**
     * Creating {@link WALRecord} instances with sensitive data.
     *
     * @return {@link WALRecord} instances with sensitive data.
     */
    private Collection<WALRecord> withSensitiveData() {
        List<WALRecord> walRecords = new ArrayList<>();

        int cacheId = CU.cacheId(DEFAULT_CACHE_NAME);

        DataEntry dataEntry = new DataEntry(
            cacheId,
            new KeyCacheObjectImpl(SENSITIVE_DATA_VALUE, null, 0),
            new CacheObjectImpl(SENSITIVE_DATA_VALUE, null),
            GridCacheOperation.CREATE,
            new GridCacheVersion(),
            new GridCacheVersion(),
            0,
            0,
            0
        );

        byte[] sensitiveDataBytes = SENSITIVE_DATA_VALUE.getBytes(StandardCharsets.UTF_8);

        walRecords.add(new DataRecord(dataEntry));
        walRecords.add(new MetastoreDataRecord(SENSITIVE_DATA_VALUE, sensitiveDataBytes));

        return walRecords;
    }
}
