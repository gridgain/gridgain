/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.rest;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.junit.Test;

/**
 * End-to-end HTTP tests for the distributed-property REST commands.
 */
public class JettyRestProcessorPropertyTest extends JettyRestProcessorCommonSelfTest {
    /** {@inheritDoc} */
    @Override protected String signature() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // checkpoint.deviation is registered only when persistence is enabled
        // (GridCacheDatabaseSharedManager is constructed only in that case).
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100 * 1024 * 1024)
                .setPersistenceEnabled(true))
            .setWalMode(WALMode.NONE);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        grid(0).cluster().state(ClusterState.ACTIVE);
    }

    /**
     * `listproperties` over HTTP returns a JSON array of property descriptors.
     */
    @Test
    public void testListProperties() throws Exception {
        String content = content(null, GridRestCommand.PROPERTY_LIST);

        JsonNode response = assertResponseSucceeded(content, false);

        assertTrue("Expected JSON array, got: " + response, response.isArray());

        boolean found = false;

        for (JsonNode prop : response) {
            assertNotNull(prop.get("name"));

            if ("checkpoint.deviation".equals(prop.get("name").asText()))
                found = true;
        }

        assertTrue("checkpoint.deviation must appear in listproperties output", found);
    }

    /**
     * `setproperty` followed by `getproperty` over HTTP — verifies the full round trip
     * including Jetty param parsing and JSON serialization of GridPropertyCommandResponse.
     */
    @Test
    public void testSetThenGetCheckpointDeviation() throws Exception {
        String setResp = content(null, GridRestCommand.PROPERTY_SET,
            "name", "checkpoint.deviation",
            "val", "23");

        JsonNode setNode = assertResponseSucceeded(setResp, false);

        assertEquals("checkpoint.deviation", setNode.get("name").asText());
        assertEquals("23", setNode.get("value").asText());
        assertEquals("Integer", setNode.get("type").asText());

        String getResp = content(null, GridRestCommand.PROPERTY_GET,
            "name", "checkpoint.deviation");

        JsonNode getNode = assertResponseSucceeded(getResp, false);

        assertEquals("checkpoint.deviation", getNode.get("name").asText());
        assertEquals("23", getNode.get("value").asText());
    }

    /**
     * `getproperty` for an unknown name surfaces a failure status with a useful error.
     */
    @Test
    public void testGetUnknownProperty() throws Exception {
        String content = content(null, GridRestCommand.PROPERTY_GET,
            "name", "does.not.exist");

        assertResponseContainsError(content, "does.not.exist");
    }
}
