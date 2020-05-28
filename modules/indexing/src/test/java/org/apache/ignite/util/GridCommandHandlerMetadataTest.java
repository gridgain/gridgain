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

package org.apache.ignite.util;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static java.lang.String.format;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.CheckIndexesInlineSizeOnNodeJoinMultiJvmTest.getSqlStatements;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Checks command line metadata commands.
 */
public class GridCommandHandlerMetadataTest extends GridCommandHandlerAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Max payload inline index size for local node. */
    private static final int INITIAL_PAYLOAD_SIZE = 1;

    /** Max payload inline index size. */
    private int payloadSize;

    /** */
    private static final UUID remoteNodeId = UUID.randomUUID();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrids(NODES_CNT).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * See class description.
     */
    @Test
    public void test() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "check_index_inline_sizes"));

        checkUtilityOutput(log, testOut.toString(), grid(0).localNode().id(), remoteNodeId);
    }

    /**
     * Checks that control.sh output as expected.
     *
     * @param log Logger.
     * @param output Uitlity output.
     * @param localNodeId Local node id.
     * @param remoteNodeId Remote node id.
     */
    public static void checkUtilityOutput(IgniteLogger log, String output, UUID localNodeId, UUID remoteNodeId) {
        assertContains(log, output, "Found 4 secondary indexes.");
        assertContains(log, output, "3 index(es) have different effective inline size on nodes. It can lead to performance degradation in SQL queries.");
        assertContains(log, output, "Index(es):");
        assertContains(log, output, "  Check that value of property IGNITE_MAX_INDEX_PAYLOAD_SIZE are the same on all nodes.");
        assertContains(log, output, "  Recreate indexes (execute DROP INDEX, CREATE INDEX commands) with different inline size.");
    }
}
