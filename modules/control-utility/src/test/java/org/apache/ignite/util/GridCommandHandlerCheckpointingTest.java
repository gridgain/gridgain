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

package org.apache.ignite.util;

import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

public class GridCommandHandlerCheckpointingTest extends GridCommandHandlerClusterByClassAbstractTest {

    /** */
    @Before
    public void init() {
        injectTestSystemOut();
    }

    /** */
    @After
    public void clear() {
    }

    /**
     * Check the command '--checkpointing help'.
     */

    @Test
    public void testForceCheckpointing() {
        final GridCacheDatabaseSharedManager gridDb = (GridCacheDatabaseSharedManager)crd.context().cache().context().database();
        final CheckpointHistory checkpointHist = gridDb.checkpointHistory();
        assert null != checkpointHist;

        final int numOfCheckpointsBefore = checkpointHist.checkpoints().size();
        final CheckpointEntry lastCheckpointBefore = checkpointHist.lastCheckpoint();

        createCacheAndPreload(crd, 1);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint"));

        final int numOfCheckpointsAfter = checkpointHist.checkpoints().size();
        final CheckpointEntry lastCheckpointAfter = checkpointHist.lastCheckpoint();

        assertTrue(numOfCheckpointsAfter > numOfCheckpointsBefore);
        assertNotSame(lastCheckpointBefore, lastCheckpointAfter);

        final String out = testOut.toString();
        final int numOfNodes = crd.cluster().forServers().nodes().size();
        assertContains(log, out, "Checkpointing completed successfully on " + numOfNodes + " nodes.");
    }

}
