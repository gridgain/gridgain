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

package org.apache.ignite.internal;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_USE_ASYNC_FILE_IO_FACTORY;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.util.IgniteUtils.jdkVersion;
import static org.apache.ignite.internal.util.IgniteUtils.majorJavaVersion;

/**
 * Check threads for default names in single and thread pool instances.
 * Actually, this checks may be moved to the base test, but we already have:
 * 1) A lot of tests with default Threads/ThreadPools
 * 2) Part of functionality uses integrations, which may creates Threads/ThreadPools without name specification.
 */
@WithSystemProperty(key = IGNITE_USE_ASYNC_FILE_IO_FACTORY, value = "false")
public class ThreadNameValidationTest extends GridCommonAbstractTest {
    /** {@link Executors.DefaultThreadFactory} count before test. */
    private static transient int defaultThreadFactoryCountBeforeTest;

    /** {@link Thread#threadInitNumber} count before test. */
    private static transient int anonymousThreadCountBeforeTest;

    /** Sequence for sets objects. */
    private static final AtomicLong SEQUENCE = new AtomicLong();

    /** */
    private static final TestRule beforeAllTestRule = (base, description) -> new Statement() {
        @Override public void evaluate() throws Throwable {
            defaultThreadFactoryCountBeforeTest = getDefaultPoolCount();
            base.evaluate();
        }
    };

    /** Manages before first test execution. */
    @ClassRule public static RuleChain firstLastTestRule
        = RuleChain.outerRule(beforeAllTestRule).around(GridAbstractTest.firstLastTestRule);

    /** */
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                ).setWalSegmentSize(1024 * 512)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();

        // MBean used LogManager with anonymous shutdown hook Thread,
        // init here if required for same behavior in runs in suite and test only
        if (!U.IGNITE_MBEANS_DISABLED) {
            ManagementFactory.getPlatformMBeanServer();
        }

        anonymousThreadCountBeforeTest = getAnonymousThreadCount();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            assertEquals("Executors.DefaultThreadFactory usage detected, IgniteThreadPoolExecutor is preferred",
                defaultThreadFactoryCountBeforeTest, getDefaultPoolCount());

            assertEquals("Thread without specific name detected",
                anonymousThreadCountBeforeTest, getAnonymousThreadCount());

        } finally {
            super.afterTest();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testThreadsWithDefaultNames() throws Exception {
        Map<Long, String> originalSnapshot = threadsSnapshot();

        IgniteEx ignite = startGrids(1);
        ignite.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = ignite.createCache(DEFAULT_CACHE_NAME);

        final int ENTRY_CNT = 10;

        for (int i = 0; i < ENTRY_CNT; i++)
            cache.put(i, userObject("user-" + i));

        validateThreadNames(originalSnapshot, threadsSnapshot());

        cache.removeAll();

        validateThreadNames(originalSnapshot, threadsSnapshot());
    }

    /**
     * @param userName User name.
     * @return Binary object.
     */
    private UserEntry userObject(String userName) {
        return new UserEntry(SEQUENCE.getAndIncrement(), userName);
    }

    /**
     * Takes a "snapshot" of current threads that have default name, aka "Thread-".
     *
     * @return "snapshot" of current threads that have default name, aka "Thread-".
     */
    private Map<Long, String> threadsSnapshot() {
        Map<Long, String> snapshot = new HashMap<>();

        for (ThreadInfo t : threadMXBean.dumpAllThreads(false, false)) {
            if (t != null && t.getThreadName().startsWith("Thread-")) {
                StringBuilder sb = new StringBuilder();
                sb.append("Thread with default name detected. StackTrace: ");

                for (StackTraceElement element : t.getStackTrace()) {
                    sb.append(System.lineSeparator())
                        .append(element.toString());
                }

                snapshot.put(t.getThreadId(), sb.toString());
            }
        }

        return snapshot;
    }

    /**
     * Validates current existed thread names.
     */
    private void validateThreadNames(Map<Long, String> origin, Map<Long, String> snap) {
        for (Map.Entry<Long, String> e : snap.entrySet()) {
            if (!origin.containsKey(e.getKey())) {
                // new thread detected.
                fail(e.getValue());
            }
        }
    }

    /**
     * Gets pools count with {@link Executors.DefaultThreadFactory}.
     * @return count
     */
    private static int getDefaultPoolCount() throws ReflectiveOperationException {
        Class<?> defaultThreadFactory = Class.forName("java.util.concurrent.Executors$DefaultThreadFactory");
        Field poolNumber = defaultThreadFactory.getDeclaredField("poolNumber");
        poolNumber.setAccessible(true);
        AtomicInteger counter = (AtomicInteger)poolNumber.get(null);
        return counter.get();
    }

    /**
     * Gets anonymous threads count since JVM start.
     * @return count
     */
    private static int getAnonymousThreadCount() throws ReflectiveOperationException {
        int javaVersion = majorJavaVersion(jdkVersion());

        if (javaVersion > 17) {
            Class<?> threadNumberingCls = Class.forName("java.lang.Thread$ThreadNumbering");
            Field next = threadNumberingCls.getDeclaredField("next");
            next.setAccessible(true);
            return (Integer) next.get(null);
        }
        else {
            Field threadInitNumberField = Thread.class.getDeclaredField("threadInitNumber");
            threadInitNumberField.setAccessible(true);
            return threadInitNumberField.getInt(null);
        }
    }

    /** Entity for tests.  */
    private static class UserEntry {
        /** Id. */
        long id;

        String name;
        /** Name. */

        /**
         * Constructor.
         *
         * @param id user ID
         * @param name user name
         */
        public UserEntry(Long id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
