/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.io.File.separatorChar;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;

// TODO: 20.11.19 Add to appropriate suites.
/**
 * Tests for checking partition reconciliation control.sh command.
 */
public class GridCommandHandlerPartitionReconciliationTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    protected static File dfltDiagnosticDir;

    /** */
    protected static File customDiagnosticDir;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        initDiagnosticDir();

        cleanDiagnosticDir();

        cleanPersistenceDir();

        IgniteEx ignite = startGrids(5);

        ignite.cluster().active(true);

        // TODO: 20.11.19 use proper cache name.
        ignite.createCache(new CacheConfiguration<>("Cache123")
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setCacheMode(PARTITIONED)
            .setPartitionLossPolicy(READ_ONLY_SAFE)
            .setBackups(1));

        try (IgniteDataStreamer streamer = ignite.dataStreamer("Cache123")) {
            for (int i = 0; i < 10000; i++)
                streamer.addData(i, new byte[i]);
        }
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        cleanDiagnosticDir();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDiagnosticDir() throws IgniteCheckedException {
        dfltDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + DEFAULT_TARGET_FOLDER + separatorChar);

        customDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + "diagnostic_test_dir" + separatorChar);
    }

    /**
     * Clean diagnostic directories.
     */
    protected void cleanDiagnosticDir() {
        U.delete(dfltDiagnosticDir);
        U.delete(customDiagnosticDir);
    }

    @Test
    public void testCacheConsistencyCheckerNoConflicts() throws Exception {
        execute("--cache", "partition_reconciliation");
//        execute("--cache", "partition_reconciliation", "Cache123");

        // TODO: 20.11.19 check and assert log. 
    }

    // TODO: 20.11.19 Tests for invalid values.

    @Test
    public void testCacheConsistencyCheckerConflictKey() throws Exception {
        corruptDataEntry(ignite(0).cachex("Cache123").context(), 5, false, true);

        execute("--cache", "partition_reconciliation");
//        execute("--cache", "partition_reconciliation", "Cache123");

        // TODO: 20.11.19 check and assert log.
    }

    @Test
    public void testCacheConsistencyCheckerConflictKeyFix() throws Exception {
        corruptDataEntry(ignite(0).cachex("Cache123").context(), 5, false, true);

        assertTrue(idleVerify(ignite(0), "Cache123").hasConflicts());

        execute("--cache", "partition_reconciliation", "--fix-mode");
//        execute("--cache", "partition_reconciliation", "Cache123");

        assertFalse(idleVerify(ignite(0), "Cache123").hasConflicts());
        // TODO: 20.11.19 check and assert log.
    }

}
