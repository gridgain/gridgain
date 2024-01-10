/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

import java.io.File;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.stringContainsInOrder;

/**
 * Test class for checking logging related to persistent storage.
 */
public class PersistenceDirectoryLoggingTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSISTENT_DIR_IS_TMP_DIR_MSG_PREFIX = "Persistence store directory is in the " +
        "temp directory and may be cleaned.";

    /** */
    private static final String INDEX_BIN_DELETED_MSG_PREFIX = "'index.bin' was deleted for cache: ";

    /** */
    private ListeningTestLogger listeningLogger = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLogger)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)
                    ))
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME),
                new CacheConfiguration<>(DEFAULT_CACHE_NAME + 1),
                new CacheConfiguration<>(DEFAULT_CACHE_NAME + 2).setGroupName(DEFAULT_CACHE_NAME + "grp")
            );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPdsDirWarningSuppressed() throws Exception {
        ListenerLogMessages listenerLogMessages = new ListenerLogMessages(PERSISTENT_DIR_IS_TMP_DIR_MSG_PREFIX);

        listeningLogger.registerListener(listenerLogMessages);

        startGrid();

        assertThat(listenerLogMessages.logMessages, empty());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPdsDirWarningIsLogged() throws Exception {
        ListenerLogMessages listenerLogMessages = new ListenerLogMessages(PERSISTENT_DIR_IS_TMP_DIR_MSG_PREFIX);

        listeningLogger.registerListener(listenerLogMessages);

        IgniteConfiguration cfg = getConfiguration("0");

        String tempDir = System.getProperty("java.io.tmpdir");

        assertNotNull(tempDir);

        // Emulates that Ignite work directory has not been calculated,
        // and IgniteUtils#workDirectory resolved directory into "java.io.tmpdir"
        cfg.setWorkDirectory(tempDir);

        startGrid(cfg);

        assertThat(listenerLogMessages.logMessages, not(empty()));
    }

    /**
     * Test that temporary work directory warning is not printed, if PDS is not actually in temporary directory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPdsDirWarningIsNotLogged() throws Exception {
        ListenerLogMessages listenerLogMessages = new ListenerLogMessages(PERSISTENT_DIR_IS_TMP_DIR_MSG_PREFIX);

        listeningLogger.registerListener(listenerLogMessages);

        IgniteConfiguration cfg = getConfiguration("0");

        String tempDir = System.getProperty("java.io.tmpdir");

        assertNotNull(tempDir);

        File workDir;

        if (U.isWindows()) {
            workDir = new File(U.defaultWorkDirectory(), "tmp");
        } else {
            // Set working directory to file not in tmp directory, but with temp directory folder name in path.
            workDir = new File(U.defaultWorkDirectory(), tempDir);
        }

        cfg.setWorkDirectory(workDir.getAbsolutePath());

        startGrid(cfg);

        assertThat(listenerLogMessages.logMessages, empty());
    }

    /**
     * Check that a log message will appear if user cache "index.bin" have been deleted.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCachesWithDeletedIndexBinLogged() throws Exception {
        ListenerLogMessages listenerLogMessages = new ListenerLogMessages(INDEX_BIN_DELETED_MSG_PREFIX);

        listeningLogger.registerListener(listenerLogMessages);

        IgniteEx n = startGrid(0);

        n.cluster().state(ACTIVE);

        File cacheIndexBinFile = getIndexBinFile(n, DEFAULT_CACHE_NAME + 1);
        File groupIndexBinFile = getIndexBinFile(n, DEFAULT_CACHE_NAME + "grp");

        assertTrue(cacheIndexBinFile.toString(), cacheIndexBinFile.exists());
        assertTrue(groupIndexBinFile.toString(), groupIndexBinFile.exists());

        stopAllGrids();

        assertThat(listenerLogMessages.logMessages, empty());

        assertTrue(cacheIndexBinFile.toString(), U.delete(cacheIndexBinFile));
        assertTrue(groupIndexBinFile.toString(), U.delete(groupIndexBinFile));

        n = startGrid(0);

        n.cluster().state(ACTIVE);

        assertThat(
            listenerLogMessages.logMessages,
            containsInAnyOrder(
                stringContainsInOrder(DEFAULT_CACHE_NAME + 1),
                stringContainsInOrder(DEFAULT_CACHE_NAME + "grp")
            )
        );
    }

    /**
     * @param n Node.
     * @param cacheOrGroupName Cache or cache group name.
     * @return Path to 'index.bin'.
     */
    private static File getIndexBinFile(IgniteEx n, String cacheOrGroupName) {
        FilePageStoreManager pageStoreManager = ((FilePageStoreManager)n.context().cache().context().pageStore());

        boolean sharedGroup = n.context().cache().cacheGroup(CU.cacheId(cacheOrGroupName)).sharedGroup();

        return new File(pageStoreManager.cacheWorkDir(sharedGroup, cacheOrGroupName), INDEX_FILE_NAME);
    }

    /** */
    private static class ListenerLogMessages implements Consumer<String> {
        /** */
        private final Collection<String> logMessages = new ConcurrentLinkedQueue<>();

        /** */
        private final String logMessagePrefix;

        /** */
        private ListenerLogMessages(String logMessagePrefix) {
            this.logMessagePrefix = logMessagePrefix;
        }

        /** {@inheritDoc} */
        @Override public void accept(String logMessage) {
            if (logMessage.startsWith(logMessagePrefix))
                logMessages.add(logMessage);
        }
    }
}
