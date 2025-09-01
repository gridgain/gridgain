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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointReadWriteLock;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Tests critical failure handling on checkpoint read lock acquisition errors.
 */
public class CheckpointReadLockContentionTest extends GridCommonAbstractTest {
    /**  */
    private ListeningTestLogger testLog;

    /**  */
    private static final AbstractFailureHandler FAILURE_HND = new AbstractFailureHandler() {
        @Override
        protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            if (failureCtx.type() != FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT)
                return true;

            if (hndLatch != null)
                hndLatch.countDown();

            return false;
        }
    };

    /**  */
    private static volatile CountDownLatch hndLatch;

    /** {@inheritDoc} */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
                .setFailureHandler(FAILURE_HND)
                .setDataStorageConfiguration(new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                                .setPersistenceEnabled(true))
                        .setCheckpointFrequency(Integer.MAX_VALUE)
                        .setCheckpointReadLockTimeout(1));

        if (testLog != null) {
            cfg.setGridLogger(testLog);

            testLog = null;
        }

        return cfg;
    }

    /**
     *
     */
    @BeforeClass
    public static void beforeClass() {
        Set<FailureType> ignoredFailureTypes = new HashSet<>(FAILURE_HND.getIgnoredFailureTypes());
        ignoredFailureTypes.remove(FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT);

        FAILURE_HND.setIgnoredFailureTypes(ignoredFailureTypes);
    }

    /**
     *
     */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     */
    @After
    public void after() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * Runs infinitely
     */
    @Test
    @WithSystemProperty(key = "foo", value = "bar")
    @WithSystemProperty(key = "CLEANUP_WORKER_SLEEP_INTERVAL", value = "6000000000")
    public void cpLockContention() throws Exception {
        hndLatch = new CountDownLatch(1);

        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager) ig.context().cache().context().database();

        CheckpointReadWriteLock cpLock = U.field(
                db.checkpointManager.checkpointTimeoutLock(), "checkpointReadWriteLock"
        );

        U.sleep(1000);

        AtomicBoolean wlCaptured = new AtomicBoolean(false);

        final int READERS_COUNT = 4;
        ExecutorService readers = Executors.newFixedThreadPool(READERS_COUNT);

        for (int i = 0; i < READERS_COUNT; i++) {
            readers.submit(() -> {
                while (!wlCaptured.get()) {

                    // GridDhtLocalPartition.clearAll
                    if (!db.tryCheckpointReadLock()) {
                        db.checkpointReadLock();
                    }

                    try {
                        TimeUnit.MICROSECONDS.sleep(1);

                        // RowStore.removeRow()
                        db.checkpointReadLock();

                        try {
                            TimeUnit.MICROSECONDS.sleep(1);
                        } finally {
                            db.checkpointReadUnlock();
                        }
                        // end of RowStore.removeRow()

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        db.checkpointReadUnlock();
                    }
                    // end of GridDhtLocalPartition.clearAll
                }

                log("WL captured, exit");
            });
        }

        U.sleep(1000);
        cpLock.writeLock();
        wlCaptured.set(true);

        try {
            U.sleep(1000);

        } finally {
            cpLock.writeUnlock();
        }
    }

    /** Runs infinitely, manages to break through io READERS_COUNT=1 **/
    @Test
    public void orderingFailsTest() throws Exception {
        ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

        AtomicBoolean wlCaptured = new AtomicBoolean(false);

        final int READERS_COUNT = 4;
        ExecutorService readers = Executors.newFixedThreadPool(READERS_COUNT);

        for (int i = 0; i < READERS_COUNT; i++) {
            readers.submit(() -> {
                log(" Start");
                int round = 0;

                try {
                    while (!wlCaptured.get()) {

                        // GridDhtLocalPartition.clearAll
                        if (!rwLock.readLock().tryLock()) {
                            log("tryLock() failed, round=" + round);
                            rwLock.readLock().lock();
                        }

                        try {
                            TimeUnit.MICROSECONDS.sleep(1);

                            // RowStore.removeRow()
                            if (!rwLock.readLock().tryLock(5, TimeUnit.SECONDS)) {
                                throw new RuntimeException("Fail to get RL #1");
                            }

                            try {
                                TimeUnit.MICROSECONDS.sleep(1);
                                if (round < 10
                                        || (round < 100 && (round % 10 == 0))
                                        || (round < 1000 && (round % 100 == 0))
                                        || (round % 1000 == 0)) {
                                    log("round=" + round);
                                }

                            } finally {
                                rwLock.readLock().unlock();
                            }
                            // end of RowStore.removeRow()

                        } catch (Exception e) {
                            throw new RuntimeException(e);

                        } finally {
                            rwLock.readLock().unlock();
                        }
                        // end of GridDhtLocalPartition.clearAll

                        round++;
                    }

                } catch (Exception e) {
                    log("Error: " + e.getMessage());
                }

                if (wlCaptured.get()) {
                    log("WL captured, exit");
                }
            });
        }

        U.sleep(1000);

        log("Request WL");  // never happens
        rwLock.writeLock().lock();

        log("WL acquired");
        wlCaptured.set(true);

        try {
            U.sleep(1000);

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /** Finishes quickly after first WL request **/
    @Test
    public void orderingPreservedTest() throws Exception {
        ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(false);

        AtomicBoolean wlCaptured = new AtomicBoolean(false);

        final int READERS_COUNT = 8;
        ExecutorService readers = Executors.newFixedThreadPool(READERS_COUNT);

        for (int i = 0; i < READERS_COUNT; i++) {
            readers.submit(() -> {
                log(" Start");
                int round = 0;

                try {
                    while (!wlCaptured.get()) {

                        // GridDhtLocalPartition.clearAll
                        rwLock.readLock().lock();

                        // GridDhtLocalPartition.clearAll
                        if (!rwLock.readLock().tryLock(5, TimeUnit.SECONDS)) {
                            log("tryLock(t, u) failed, round=" + round);
                            rwLock.readLock().lock();
                        }

                        try {
                            TimeUnit.MICROSECONDS.sleep(1); // simulate some ops

                            // RowStore.removeRow()
                            if (!rwLock.readLock().tryLock(5, TimeUnit.SECONDS)) {
                                throw new RuntimeException("Fail to get RL #1");
                            }

                            try {
                                TimeUnit.MICROSECONDS.sleep(1); // simulate some ops
                                if (round < 10
                                        || (round < 100 && (round % 10 == 0))
                                        || (round < 1000 && (round % 100 == 0))
                                        || (round % 1000 == 0)) {
                                    log("round=" + round);
                                }

                            } finally {
                                rwLock.readLock().unlock();
                            }
                            // end of RowStore.removeRow()

                        } catch (Exception e) {
                            throw new RuntimeException(e);

                        } finally {
                            rwLock.readLock().unlock();
                        }
                        // end of GridDhtLocalPartition.clearAll

                        round++;
                    }

                } catch (Exception e) {
                    log("Error: " + e.getMessage());
                }

                if (wlCaptured.get()) {
                    log("WL captured, exit");
                }
            });
        }

        U.sleep(1000);

        log("Request WL");
        rwLock.writeLock().lock();

        log("WL acquired");
        wlCaptured.set(true);

        try {
            U.sleep(1000);

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private static void log(String msg) {
        System.out.println("[" + LocalTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME) + "] [" + Thread.currentThread().getName() + "]  " + msg);
    }

}
