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

package org.apache.ignite.util;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static java.lang.String.format;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Checks utility output if check_index_inline_sizes command found problems.
 */
@WithSystemProperty(key = IGNITE_MAX_INDEX_PAYLOAD_SIZE, value = "1")
public class GridCommandHandlerCheckIndexesInlineSizeTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String STR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /** */
    private static final String INDEX_PROBLEM_FMT =
        "Full index name: PUBLIC#TEST_TABLE#%s nodes: [%s] inline size: 1, nodes: [%s] inline size: 2";

    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Max payload inline index size for local node. */
    private static final int INITIAL_PAYLOAD_SIZE = 1;

    /** Max payload inline index size. */
    private int payloadSize;

    /** */
    private static final UUID remoteNodeId = UUID.randomUUID();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (isRemoteJvm(igniteInstanceName))
            cfg.setNodeId(remoteNodeId);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();

        assertEquals(INITIAL_PAYLOAD_SIZE, Integer.parseInt(System.getProperty(IGNITE_MAX_INDEX_PAYLOAD_SIZE)));

        startGrids(NODES_CNT).cluster().active(true);

        executeSql(grid(0), "CREATE TABLE TEST_TABLE (i INT, l LONG, s0 VARCHAR, s1 VARCHAR, PRIMARY KEY (i, s0))");

        for (int i = 0; i < 10; i++)
            executeSql(grid(0), "INSERT INTO TEST_TABLE (i, l, s0, s1) VALUES (?, ?, ?, ?)", i, i * i, STR + i, STR + i * i);

        executeSql(grid(0), "CREATE INDEX i_idx ON TEST_TABLE(i)");
        executeSql(grid(0), "CREATE INDEX l_idx ON TEST_TABLE(l)");
        executeSql(grid(0), "CREATE INDEX s0_idx ON TEST_TABLE(s0) INLINE_SIZE 10");
        executeSql(grid(0), "CREATE INDEX s1_idx ON TEST_TABLE(s1)");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        List<String> args = super.additionalRemoteJvmArgs();

        args.add("-D" + IGNITE_MAX_INDEX_PAYLOAD_SIZE + "=" + payloadSize);

        return args;
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int idx) throws Exception {
        payloadSize = getMaxPayloadSize(idx);

        return super.startGrid(idx);
    }

    /**
     * See class description.
     */
    @Test
    public void test() {
        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "check_index_inline_sizes"));

        UUID locNodeId = grid(0).localNode().id();

        String utilityOutput = testOut.toString();

        assertContains(log, utilityOutput, "3 indexes have different effective inline size on nodes. It can lead to performance degradation in SQL queries.");
        assertContains(log, utilityOutput, "Indexes:");
        assertContains(log, utilityOutput, format(INDEX_PROBLEM_FMT, "L_IDX", locNodeId, remoteNodeId));
        assertContains(log, utilityOutput, format(INDEX_PROBLEM_FMT, "S1_IDX", locNodeId, remoteNodeId));
        assertContains(log, utilityOutput, format(INDEX_PROBLEM_FMT, "I_IDX", locNodeId, remoteNodeId));
        assertContains(log, utilityOutput, "  Check that value of property IGNITE_MAX_INDEX_PAYLOAD_SIZE are the same on all nodes.");
        assertContains(log, utilityOutput, "  Recreate indexes with different inline size.");
    }

    /** */
    private int getMaxPayloadSize(int nodeId) {
        return INITIAL_PAYLOAD_SIZE + nodeId;
    }

    /** */
    private static List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }
}
