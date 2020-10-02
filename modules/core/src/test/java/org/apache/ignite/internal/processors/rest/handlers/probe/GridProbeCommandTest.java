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

package org.apache.ignite.internal.processors.rest.handlers.probe;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * REST probe cmd test.
 */
public class GridProbeCommandTest extends GridCommonAbstractTest {

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(false);
    }

    /**
     * <p>Test for the REST probe command
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestProbeCommand() throws Exception {

        startGrids(1);

        GridRestCommandHandler hnd = new GridProbeCommandHandler((grid(0)).context());

        GridRestCacheRequest req = new GridRestCacheRequest();

        req.command(GridRestCommand.PROBE);
        GridRestResponse resp = hnd.handleAsync(req).get(10 * 1000);
        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.getSuccessStatus());
        assertEquals("true", resp.getResponse());

    }

}
