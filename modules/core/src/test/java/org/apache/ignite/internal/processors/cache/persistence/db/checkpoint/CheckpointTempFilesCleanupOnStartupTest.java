/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.io.File;
import java.util.UUID;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class CheckpointTempFilesCleanupOnStartupTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(storageCfg);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /**
     * Test that tmp checkpoints are getting deleted on startup
     * @throws Exception
     */
    @Test
    public void testRestartWithTmpCheckpoint() throws Exception {
        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        String nodeId = UUID.randomUUID().toString();

        File nodeDir = new File(new File(dbDir.getAbsolutePath(),U.maskForFileName(nodeId)), "cp");

        nodeDir.mkdirs();

        File tmpFileLeftAfterCrash = new File(nodeDir, "1586870171036-d346c814-aa03-4d66-bf1e-2951a04268f9-END.bin.tmp");

        tmpFileLeftAfterCrash.createNewFile();

        assertTrue(tmpFileLeftAfterCrash.exists());

        IgniteConfiguration cfg = getConfiguration();

        cfg.setConsistentId(nodeId);

        startGrid(cfg);

        assertFalse(tmpFileLeftAfterCrash.exists());
    }
}
