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

package org.apache.ignite.agent.utils;

import java.util.UUID;
import org.junit.Test;

import static org.apache.ignite.agent.utils.AgentUtils.monitoringUri;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

/**
 * Test for agent utils.
 */
public class AgentUtilsTest {
    /**
     * Should return monitoring irl.
     */
    @Test
    public void shouldBuildMonitoringUri() {
        UUID clusterId = UUID.randomUUID();

        String expUri = "http://host:80/clusters/" + clusterId + "/monitoring-dashboard";

        String uriWithTrailingSlash = monitoringUri("http://host:80/", clusterId);

        assertEquals(expUri, uriWithTrailingSlash);

        String uriWithoutTrailingSlash = monitoringUri("http://host:80", clusterId);

        assertEquals(expUri, uriWithoutTrailingSlash);

        String uriWithTrailingSlashes = monitoringUri("http://host:80///", clusterId);

        assertEquals(expUri, uriWithTrailingSlashes);

        assertThrows(null, () -> {
            monitoringUri("http://host super:80", clusterId);
            return null;
        }, IllegalArgumentException.class, "Illegal character in authority at index 7: " +
            "http://host super:80/clusters/" + clusterId + "/monitoring-dashboard");
    }
}
