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
package org.apache.ignite.internal.processors.query.oom;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.exceptions.SqlMemoryQuotaExceededException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_ENABLE_CONNECTION_MEMORY_QUOTA;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE;
import static org.apache.ignite.internal.processors.query.h2.QueryMemoryManager.DISK_SPILL_DIR;

/**
 * Base class for disk spilling tests.
 */
@WithSystemProperty(key = IGNITE_SQL_MEMORY_RESERVATION_BLOCK_SIZE, value = "2048")
@WithSystemProperty(key = IGNITE_SQL_ENABLE_CONNECTION_MEMORY_QUOTA, value = "true")
public abstract class DiskSpillingAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static final int PERS_CNT = 1002;

    /** */
    private static final int DEPS_CNT = 100;

    /** */
    protected static final long SMALL_MEM_LIMIT = 4096;

    /** */
    protected static final long HUGE_MEM_LIMIT = Long.MAX_VALUE;

    /** */
    protected boolean checkSortOrder;

    /** */
    protected boolean checkGroupsSpilled;

    /** */
    protected List<Integer> listAggs;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setSqlConfiguration(new SqlConfiguration().setSqlOffloadingEnabled(true));

        // Dummy cache.
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cfg.setCacheConfiguration(cache);

        if (persistence()) {
            DataStorageConfiguration storageCfg = new DataStorageConfiguration();

            DataRegionConfiguration regionCfg = new DataRegionConfiguration();
            regionCfg.setPersistenceEnabled(true);

            storageCfg.setDefaultDataRegionConfiguration(regionCfg);

            cfg.setDataStorageConfiguration(storageCfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        initGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        destroyGrid();
    }

    /**
     * Creates grid and populates it with data.
     *
     * @throws Exception If failed.
     */
    void initGrid() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrids(nodeCount());

        if (persistence())
            grid(0).cluster().active(true);

        CacheConfiguration<?,?> personCache = defaultCacheConfiguration();
        personCache.setQueryParallelism(queryParallelism());
        personCache.setName("person");
        personCache.setCacheMode(CacheMode.PARTITIONED);
        personCache.setBackups(1);
        grid(0).addCacheConfiguration(personCache);

        CacheConfiguration<?,?> orgCache = defaultCacheConfiguration();
        orgCache.setQueryParallelism(queryParallelism());
        orgCache.setName("organization");
        orgCache.setCacheMode(CacheMode.PARTITIONED);
        orgCache.setBackups(1);
        grid(0).addCacheConfiguration(orgCache);

        if (startClient())
            startGrid(getConfiguration("client").setClientMode(true));

        populateData();
    }

    /** */
    protected boolean startClient() {
        return true;
    }

    /**
     * Destroys grid.
     *
     * @throws Exception If failed.
     */
    void destroyGrid() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    protected int nodeCount() {
        return 1;
    }

    /** */
    protected boolean persistence() {
        return false;
    }

    /** */
    protected int queryParallelism() {
        return 1;
    }

    /** */
    protected boolean fromClient() {
        return false;
    }

    /** */
    protected boolean localQuery() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        FileUtils.cleanDirectory(getWorkDir().toFile());

        checkSortOrder = false;
        checkGroupsSpilled = false;
        listAggs = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        checkMemoryManagerState();

        FileUtils.cleanDirectory(getWorkDir().toFile());
    }

    /** */
    protected void assertInMemoryAndOnDiskSameResults(boolean lazy, String sql) {
        WatchService watchSvc = null;

        try {
            Path workDir = getWorkDir();

            if (log.isInfoEnabled())
                log.info("workDir=" + workDir.toString());

            watchSvc = FileSystems.getDefault().newWatchService();

            WatchKey watchKey = workDir.register(watchSvc, ENTRY_CREATE, ENTRY_DELETE);

            // In-memory.
            long startInMem = System.currentTimeMillis();

            if (log.isInfoEnabled())
                log.info("Run query in memory.");

            watchKey.reset();

            List<List<?>> inMemRes = runSql(sql, lazy, HUGE_MEM_LIMIT);

            assertFalse("In-memory result is empty.", inMemRes.isEmpty());

            assertWorkDirClean();
            checkMemoryManagerState();

            List<WatchEvent<?>> dirEvts = watchKey.pollEvents();

            // No files should be created for in-memory mode.
            assertTrue("Disk events is not empty for in-memory query: :" + dirEvts.stream().map(e ->
                e.kind().toString()).collect(Collectors.joining(", ")), dirEvts.isEmpty());

            // On disk.
            if (log.isInfoEnabled())
                log.info("Run query with disk offloading.");

            long startOnDisk = System.currentTimeMillis();

            watchKey.reset();

            List<List<?>> onDiskRes = runSql(sql, lazy, SMALL_MEM_LIMIT);

            assertFalse("On disk result is empty.", onDiskRes.isEmpty());

            long finish = System.currentTimeMillis();

            dirEvts = watchKey.pollEvents();

            // Check files have been created but deleted later.
            assertFalse("Disk events is empty for on-disk query. ", dirEvts.isEmpty());

            assertWorkDirClean();
            checkMemoryManagerState();

            if (log.isInfoEnabled())
                log.info("Spill files events (created + deleted): " + dirEvts.size());

            if (!checkSortOrder) {
                fixSortOrder(onDiskRes);
                fixSortOrder(inMemRes);
            }

            if (listAggs != null) {
                fixListAggsSort(onDiskRes);
                fixListAggsSort(inMemRes);
            }

            if (log.isInfoEnabled())
                log.info("In-memory time=" + (startOnDisk - startInMem) + ", on-disk time=" + (finish - startOnDisk));

            if (log.isDebugEnabled())
                log.debug("In-memory result:\n" + inMemRes + "\nOn disk result:\n" + onDiskRes);

            assertEqualsCollections(inMemRes, onDiskRes);

            checkMemoryManagerState();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            U.closeQuiet(watchSvc);
        }
    }

    /**
     * Results for LISTAGG aggregate may arrive to reduce node in arbitrary order, so we need to fix this order
     * to be able to compare results.
     *
     * @param res Result.
     */
    private void fixListAggsSort(List<List<?>> res) {
        for (List row : res) {
            for (Integer idx : listAggs) {
                String listAgg = (String) row.get(idx);

                String[] strings = listAgg.split(",");

                Arrays.sort(strings);

                String newListAgg = Arrays.stream(strings).collect(Collectors.joining(","));

                row.set(idx, newListAgg);
            }
        }
    }

    /** */
    private void populateData() {
        // Persons
        runDdlDml("CREATE TABLE person (" +
            "id BIGINT PRIMARY KEY, " +
            "name VARCHAR, " +
            "depId SMALLINT, " +
            "code CHAR(3)," +
            "male BOOLEAN," +
            "age TINYINT," +
            "height SMALLINT," +
            "salary INT," +
            "tax DECIMAL(4,4)," +
            "weight DOUBLE," +
            "temperature REAL," +
            "time TIME," +
            "date DATE," +
            "timestamp TIMESTAMP," +
            "uuid UUID, " +
            "nulls INT) " +
            "WITH \"TEMPLATE=person\"");

        for (int i = 0; i < PERS_CNT; i++) {
            runDdlDml("INSERT INTO person (" +
                    "id, " +
                    "name, " +
                    "depId,  " +
                    "code, " +
                    "male, " +
                    "age, " +
                    "height, " +
                    "salary, " +
                    "tax, " +
                    "weight, " +
                    "temperature," +
                    "time," +
                    "date," +
                    "timestamp," +
                    "uuid, " +
                    "nulls) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                i,                    // id
                "Vasya" + i,                // name
                i % DEPS_CNT,               // depId
                "p" + i % 31,               // code
                i % 2,                      // male
                i % 100,                    // age
                150 + (i % 50),             // height
                50000 + i,                  // salary
                i / 1000d,       // tax
                50d + i % (PERS_CNT / 2),             // weight
                (i % 10 == 0) ? null : (36.6 + i % 5),   // temperature_nullable
                "20:00:" + i % 60,          // time
                "2019-04-" + (i % 29 + 1),  // date
                "2019-04-04 04:20:08." + i % 900, // timestamp
                "736bc956-090c-40d2-94da-916f2161cda" + i % 10, // uuid
                null);                      // nulls
        }

        // Departments
        runDdlDml("CREATE TABLE department (" +
            "id INT PRIMARY KEY, " +
            "title VARCHAR_IGNORECASE) " +
            "WITH \"TEMPLATE=organization\"");

        for (int i = 0; i < DEPS_CNT; i++) {
            runDdlDml("INSERT INTO department (id, title) VALUES (?, ?)", i, "IT" + i);
        }
    }

    /** */
    protected List<List<?>> runSql(String sql, boolean lazy, long memLimit) {
        Ignite node = fromClient() ? grid("client") : grid(0);
        return node.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQueryEx(sql, null)
            .setMaxMemory(memLimit)
            .setLazy(lazy)
            .setLocal(localQuery())
        ).getAll();
    }

    /** */
    protected List<List<?>> runDdlDml(String sql, Object... args) {
        return grid(0)
            .cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQueryEx(sql, null)
                .setArgs(args)
            ).getAll();
    }

    /** */
    private void fixSortOrder(List<List<?>> res) {
        Collections.sort(res, new Comparator<List<?>>() {
            @Override public int compare(List<?> l1, List<?> l2) {
                for (int i = 0; i < l1.size(); i++) {
                    Object o1 = l1.get(i);
                    Object o2 = l2.get(i);

                    if (o1 == null || o2 == null) {
                        if (o1 == null && o2 == null)
                            return 0;

                        return o1 == null ? 1 : -1;
                    }

                    if (o1.hashCode() == o2.hashCode())
                        continue;

                    return o1.hashCode() > o2.hashCode() ? 1 : -1;
                }

                return 0;
            }
        });
    }

    /** */
    protected Path getWorkDir() {
        Path workDir;
        try {
            workDir = Paths.get(U.defaultWorkDirectory(), DISK_SPILL_DIR);
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException(ex);
        }

        workDir.toFile().mkdir();
        return workDir;
    }

    /** */
    protected void assertWorkDirClean() {
        List<String> spillFiles = listOfSpillFiles();

        assertEquals("Files are not deleted: " + spillFiles, 0, spillFiles.size());
    }

    /** */
    protected List<String> listOfSpillFiles() {
        Path workDir = getWorkDir();

        assertTrue(workDir.toFile().isDirectory());

        return Arrays.asList(workDir.toFile().list());
    }

    /**
     *
     */
    protected void checkMemoryManagerState() {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memoryManager = memoryManager((IgniteEx)node);

            assertEquals(0, memoryManager.reserved());
        }
    }

    /**
     * @param node Node.
     * @return Memory manager
     */
    protected QueryMemoryManager memoryManager(IgniteEx node) {
        IgniteH2Indexing h2 = (IgniteH2Indexing)node.context().query().getIndexing();

        return h2.memoryManager();
    }

    /**
     * @param node Node.
     * @return Running query manager.
     */
    protected RunningQueryManager runningQueryManager(IgniteEx node) {
        IgniteH2Indexing h2 = (IgniteH2Indexing)node.context().query().getIndexing();

        return h2.runningQueryManager();
    }

    /** */
    protected void checkQuery(Result res, String sql) {
        checkQuery(res, sql, 1, 1);
    }

    /** */
    protected void checkQuery(Result res, String sql, int threadNum, int iterations) {
        WatchService watchSvc = null;
        WatchKey watchKey = null;

        try {
            watchSvc = FileSystems.getDefault().newWatchService();

            Path workDir = getWorkDir();

            watchKey = workDir.register(watchSvc, ENTRY_CREATE, ENTRY_DELETE);

            final AtomicBoolean oomExThrown = new AtomicBoolean();

            multithreaded(() -> {
                try {
                    for (int i = 0; i < iterations; i++) {
                        IgniteEx grid = fromClient() ? grid("client") : grid(0);
                        grid.cache(DEFAULT_CACHE_NAME)
                            .query(new SqlFieldsQuery(sql))
                            .getAll();
                    }
                }
                catch (SqlMemoryQuotaExceededException e) {
                    oomExThrown.set(true);

                    assertFalse("Unexpected exception:" + X.getFullStackTrace(e) ,res.success);

                    if (res == Result.ERROR_GLOBAL_QUOTA)
                        assertTrue("Wrong message:" + X.getFullStackTrace(e), e.getMessage().contains("Global quota was exceeded."));
                    else
                        assertTrue("Wrong message:" + X.getFullStackTrace(e), e.getMessage().contains("Query quota was exceeded."));
                }
                catch (Throwable t) {
                    log.error("Caught exception:" + X.getFullStackTrace(t));

                    throw t;
                }

            }, threadNum);

            assertEquals("Exception expected=" + !res.success + ", exception thrown=" + oomExThrown.get(),
                !res.success, oomExThrown.get());
        }
        catch (Exception e) {
            fail(X.getFullStackTrace(e));
        }
        finally {
            try {
                if (watchKey != null) {
                    List<WatchEvent<?>> dirEvts = watchKey.pollEvents();

                    assertEquals("Disk spilling " + (res.offload ? "not" : "") + " happened.",
                        res.offload, !dirEvts.isEmpty());
                }

                assertWorkDirClean();

                checkMemoryManagerState();
            }
            finally {
                U.closeQuiet(watchSvc);
            }
        }
    }

    /** */
    public enum Result {
        /** */
        SUCCESS_WITH_OFFLOADING(true, true),

        /** */
        SUCCESS_NO_OFFLOADING(false, true),

        /** */
        ERROR_GLOBAL_QUOTA(false, false),

        /** */
        ERROR_QUERY_QUOTA(false, false);

        /** */
        Result(boolean offload, boolean success) {
            this.offload = offload;
            this.success = success;
        }

        /** */
        final boolean offload;

        /** */
        final boolean success;
    }
}
