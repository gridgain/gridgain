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

package org.apache.ignite.testframework.junits.common;

import java.io.FileReader;
import java.util.Properties;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Base class for examples test.
 */
public abstract class GridAbstractExamplesTest extends GridCommonAbstractTest {
    /** */
    protected static final String[] EMPTY_ARGS = new String[0];

    /** */
    protected static final int RMT_NODES_CNT = 3;

    /** */
    protected static final String RMT_NODE_CFGS = "modules/core/src/test/config/examples.properties";

    /** */
    protected static final String DFLT_CFG = "examples/config/example-ignite.xml";

    /** */
    private static final Properties rmtCfgs = new Properties();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Starts remote nodes.
     *
     * @throws Exception If failed.
     */
    protected final void startRemoteNodes() throws Exception {
        String name = getName().replaceFirst("test", "");

        if (rmtCfgs.isEmpty()) {
            info("Loading remote configs properties from file: " + RMT_NODE_CFGS);

            try (FileReader reader = new FileReader(U.resolveIgnitePath(RMT_NODE_CFGS))) {
                rmtCfgs.load(reader);
            }
        }

        String cfg = rmtCfgs.getProperty(name, defaultConfig());

        info("Config for remote nodes [name=" + name + ", cfg=" + cfg + ", dflt=" + defaultConfig() + "]");

        for (int i = 0; i < RMT_NODES_CNT; i++)
            startGrid(getTestIgniteInstanceName(i), cfg);
    }

    /**
     * Starts grid using provided Ignite instance name and spring config location.
     * <p>
     * Note that grids started this way should be stopped with {@code G.stop(..)} methods.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param springCfgPath Path to config file.
     * @return Grid Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGrid(String igniteInstanceName, String springCfgPath) throws Exception {
        IgniteConfiguration cfg = Ignition.loadSpringBean(springCfgPath, "ignite.cfg");

        cfg.setFailureHandler(getFailureHandler(igniteInstanceName));

        cfg.setGridLogger(getTestResources().getLogger());

        return startGrid(igniteInstanceName, cfg);
    }

    /**
     * @return Default config for this test.
     */
    protected String defaultConfig() {
        return DFLT_CFG;
    }
}
