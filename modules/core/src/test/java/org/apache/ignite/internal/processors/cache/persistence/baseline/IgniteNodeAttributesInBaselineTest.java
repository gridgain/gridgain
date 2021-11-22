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

package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.MISSING_AFF_ATTRS_WARNING;

/**
 * Tests that check grid behavior when node joins baseline topology and it's set of attributes
 * differs from baseline attributes.
 */
public class IgniteNodeAttributesInBaselineTest extends GridCommonAbstractTest {
    /** Number of server nodes in test cluster. */
    private static final int GRIDS_NUM = 3;

    /** Extra user attr name. */
    private static final String EXTRA_USER_ATTR_NAME = "extraUserAttrName";

    /** Extra user attr value. */
    private static final String EXTRA_USER_ATTR_VAL = "extraUserAttrVal";

    /** Basic user attr name. */
    private static final String BASIC_USER_ATTR_NAME = "basicUserAttrName";

    /** Basic user attr value. */
    private static final String BASIC_USER_ATTR_VAL = "basicUserAttrVal";

    /** */
    private boolean addBasicAttrs = true;

    /** */
    private boolean addExtraAttr = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(256L * 1024 * 1024)
                    .setPersistenceEnabled(true)));

        Map<String, String> userAttrs = new HashMap<>();

        if (addBasicAttrs)
            userAttrs.put(BASIC_USER_ATTR_NAME, BASIC_USER_ATTR_VAL);

        if (addExtraAttr)
            userAttrs.put(EXTRA_USER_ATTR_NAME, EXTRA_USER_ATTR_VAL);

        cfg.setAffinityAttributes(userAttrs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        Ignite ignite = startGridsMultiThreaded(GRIDS_NUM);

        ignite.cluster().state(ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        addBasicAttrs = true;

        addExtraAttr = false;

        super.afterTest();
    }

    /**
     * Checks that in case of node joining baseline topology and containing new attribute, this attribute will be
     * propagated to all nodes' baseline topologies.
     *
     * @throws Exception If failed to start node.
     */
    @Test
    public void testNewAttribute() throws Exception {
        stopGrid(GRIDS_NUM - 1);

        addExtraAttr = true;

        IgniteEx lastGrid = startGrid(GRIDS_NUM - 1);

        Object consId2 = lastGrid.localNode().consistentId();

        for (int i = 0; i < GRIDS_NUM; i++) {
            Collection<BaselineNode> baselineNodes = grid(i).cluster().currentBaselineTopology();

            BaselineNode bsNode = baselineNodes.stream()
                .filter(node -> node.consistentId().equals(consId2))
                .findAny()
                .orElse(null);

            assertNotNull(bsNode);

            String userAttr = bsNode.affinityAttribute(EXTRA_USER_ATTR_NAME);

            assertEquals(EXTRA_USER_ATTR_VAL, userAttr);
        }
    }

    /**
     * Checks that in case of node joining baseline topology and lacking attribute that is present in other nodes'
     * baseline topologies, this node will not be allowed to join the grid.
     */
    @Test
    public void testExistingAttribute() {
        stopGrid(GRIDS_NUM - 1);

        addBasicAttrs = false;

        try {
            startGrid(GRIDS_NUM - 1);
        }
        catch (Exception e) {
            IgniteCheckedException cause = (IgniteCheckedException)e.getCause();

            assertNotNull(cause);

            IgniteSpiException rootCause = (IgniteSpiException)cause.getCause();

            assertNotNull(rootCause);

            String msg = rootCause.getMessage();

            String expMsg = MISSING_AFF_ATTRS_WARNING +
                "\n" +
                "Missing options:\n" +
                "Attr name: basicUserAttrName; Attr val: basicUserAttrVal";

            assertEquals(expMsg, msg);
        }
    }
}
