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
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Test;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;

/**
 *  Basic test cases for disk spilling.
 */
public class DiskSpillingBasicTest extends DiskSpillingAbstractTest {
    /** */
    @Test
    public void testMultiThreaded() {
        final int NUM_THREADS = Runtime.getRuntime().availableProcessors() + 1;

        final AtomicBoolean stop = new AtomicBoolean();

        String[] qrys = new String[] {
            // Union/intersect.
            "(SELECT * FROM person WHERE depId < 40 " +
                "INTERSECT " +
                "SELECT * FROM person WHERE depId > 1 )" +
                "UNION  " +
                "SELECT * FROM person WHERE age > 50 ORDER BY id LIMIT 1000 OFFSET 50 ",

            // Unsorted.
            "SELECT p.id, p.name, p.depId, d.title " +
                "FROM person p, department d " +
                " WHERE p.depId > d.id",

            // Sorted.
            "SELECT  code, depId, salary, id  " +
                "FROM person ORDER BY code, salary, id",

            // Aggregate.
            "SELECT code, SUM(temperature), AVG(salary) FROM person WHERE age > 5 GROUP BY code",

            // Distinct
            "SELECT DISTINCT code, salary, id  " +
                "FROM person ORDER BY code, salary, id",

            // Subquery.
            "SELECT code, depId, salary, id  " +
                "FROM person WHERE depId IN (SELECT id FROM department)"
        };

        final int qrysSize = qrys.length;

        final List<List> results = new ArrayList<>(qrysSize);

        for (int i = 0; i < qrysSize; i++)
            results.add(i, grid(0).cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQueryEx(qrys[i], true).setMaxMemory(16384)).getAll());

        final AtomicReference<Throwable> err = new AtomicReference<>();

        AtomicIntegerArray cntrs = new AtomicIntegerArray(NUM_THREADS);

        AtomicInteger cntr = new AtomicInteger();

        Thread testThread = Thread.currentThread();

        Runnable qryRunner = new Runnable() {
            @Override public void run() {
                int threadId = cntr.getAndIncrement();

                try {
                    while (!stop.get()) {
                        int qryNum = ThreadLocalRandom.current().nextInt(qrysSize);

                        List res = grid(0).cache(DEFAULT_CACHE_NAME)
                            .query(new SqlFieldsQueryEx(qrys[qryNum], null).setMaxMemory(SMALL_MEM_LIMIT).setLazy(true))
                            .getAll();

                        assertEqualsCollections(results.get(qryNum), res);

                        cntrs.incrementAndGet(threadId);
                    }
                }
                catch (Throwable e) {
                    log.error("Error during query execution: " + X.getFullStackTrace(e));

                    err.compareAndSet(null, e);

                    stop.set(true);

                    testThread.interrupt();
                }
            }
        };

        Collection<Thread> runners = new ArrayList<>(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            Thread runner = new Thread(qryRunner);

            runners.add(runner);

            runner.start();
        }

        try {
            Thread.sleep(10_000);
        }
        catch (InterruptedException ignore) {
            // Need to check for errors.
        }

        stop.set(true);

        for (Thread runner : runners) {
            try {
                runner.join(60_000);

                assertFalse(runner.isAlive());
            }
            catch (InterruptedException e) {
                throw new RuntimeException("Can not stop thread:" + Arrays.toString(runner.getStackTrace()));
            }
        }

        assertNull(X.getFullStackTrace(err.get()), err.get());

        for (int i = 0; i < NUM_THREADS; i++)
            assertTrue("Thread with not executed queries found.", cntrs.get(i) > 0);

        assertWorkDirClean();
        checkMemoryManagerState();
    }

    /** */
    @Test
    public void testFilesDeletedOnError() throws IOException {
        // Query should throw "Division by zero exception" during execution.
        String qry = "SELECT DISTINCT *, 1 / (p.id - 800) " +
            "FROM person p, department d " +
            " WHERE p.depId = d.id";

        Path workDir = getWorkDir();

        WatchService watchSvc = FileSystems.getDefault().newWatchService();

        WatchKey watchKey = workDir.register(watchSvc, ENTRY_CREATE, ENTRY_DELETE);

        try {
            List res = grid(0).cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQueryEx(qry, null)
                    .setMaxMemory(SMALL_MEM_LIMIT)
                    .setLazy(true))
                .getAll();

            System.out.println("res=" + res);

            fail("Exception should be thrown.");
        }
        catch (Exception ignore) {
            // No-op.
        }

        List<WatchEvent<?>> dirEvts = watchKey.pollEvents();

        // Check files have been created but deleted later.
        assertFalse(dirEvts.isEmpty());

        assertWorkDirClean();
        checkMemoryManagerState();
    }

    /** */
    @Test
    public void testNodeStartupDoesNotAffectRunningQueries() throws Exception {
        String query = "SELECT DISTINCT * " +
            "FROM person p, department d " +
            "WHERE p.depId = d.id";

        Path workDir = getWorkDir();

        WatchService watchSvc = FileSystems.getDefault().newWatchService();

        WatchKey watchKey = workDir.register(watchSvc, ENTRY_CREATE, ENTRY_DELETE);

        Iterator<List<?>> res = grid(0).cache(DEFAULT_CACHE_NAME)
            .query(new SqlFieldsQueryEx(query, null)
                .setMaxMemory(SMALL_MEM_LIMIT)
            .setLazy(true))
            .iterator();

        assertFalse(res.next().isEmpty());

        startGrid();

        assertFalse(res.next().isEmpty());

        while (res.hasNext())
            res.next();

        List<WatchEvent<?>> dirEvts = watchKey.pollEvents();

        // Check files have been created but deleted later.
        assertFalse(dirEvts.isEmpty());

        assertWorkDirClean();
        checkMemoryManagerState();
    }
}
