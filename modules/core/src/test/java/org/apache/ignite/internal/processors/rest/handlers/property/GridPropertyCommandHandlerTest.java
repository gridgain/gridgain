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

package org.apache.ignite.internal.processors.rest.handlers.property;

import java.util.List;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.request.GridRestPropertyRequest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.PROPERTY_GET;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.PROPERTY_LIST;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.PROPERTY_SET;

/**
 * Tests for {@link GridPropertyCommandHandler}.
 */
public class GridPropertyCommandHandlerTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // checkpoint.deviation is only registered when persistence is enabled
        // (GridCacheDatabaseSharedManager is only constructed in that case).
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrids(2);

        grid(0).cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /**
     * `listproperties` should return one entry per registered distributed property,
     * each with a non-null name. The checkpoint.deviation property must appear.
     */
    @Test
    public void testListProperties() throws Exception {
        GridPropertyCommandHandler hnd = new GridPropertyCommandHandler(grid(0).context());

        GridRestPropertyRequest req = new GridRestPropertyRequest();
        req.command(PROPERTY_LIST);

        GridRestResponse res = hnd.handleAsync(req).get();

        assertNull("Unexpected error: " + res.getError(), res.getError());
        assertEquals(GridRestResponse.STATUS_SUCCESS, res.getSuccessStatus());

        List<GridPropertyCommandResponse> props = (List<GridPropertyCommandResponse>)res.getResponse();

        assertNotNull(props);
        assertFalse("Expected at least one registered distributed property", props.isEmpty());
        assertTrue("checkpoint.deviation should be registered",
            props.stream().anyMatch(p -> "checkpoint.deviation".equals(p.getName())));

        for (GridPropertyCommandResponse p : props)
            assertNotNull("Property name must be non-null", p.getName());
    }

    /**
     * `getproperty` should return the descriptor for the named property.
     */
    @Test
    public void testGetProperty() throws Exception {
        GridPropertyCommandHandler hnd = new GridPropertyCommandHandler(grid(0).context());

        GridRestPropertyRequest req = new GridRestPropertyRequest();
        req.command(PROPERTY_GET);
        req.name("checkpoint.deviation");

        GridRestResponse res = hnd.handleAsync(req).get();

        assertNull("Unexpected error: " + res.getError(), res.getError());
        assertEquals(GridRestResponse.STATUS_SUCCESS, res.getSuccessStatus());

        GridPropertyCommandResponse dto = (GridPropertyCommandResponse)res.getResponse();

        assertNotNull(dto);
        assertEquals("checkpoint.deviation", dto.getName());
    }

    /**
     * `getproperty` without `name` must fail with a missing-parameter error.
     */
    @Test
    public void testGetPropertyMissingName() throws Exception {
        GridPropertyCommandHandler hnd = new GridPropertyCommandHandler(grid(0).context());

        GridRestPropertyRequest req = new GridRestPropertyRequest();
        req.command(PROPERTY_GET);

        GridRestResponse res = hnd.handleAsync(req).get();

        assertEquals(GridRestResponse.STATUS_FAILED, res.getSuccessStatus());
        assertNotNull(res.getError());
        assertTrue("Error should mention 'name', got: " + res.getError(),
            res.getError().contains("name"));
    }

    /**
     * `getproperty` with an unknown property name must fail with a clear error.
     */
    @Test
    public void testGetPropertyUnknown() throws Exception {
        GridPropertyCommandHandler hnd = new GridPropertyCommandHandler(grid(0).context());

        GridRestPropertyRequest req = new GridRestPropertyRequest();
        req.command(PROPERTY_GET);
        req.name("does.not.exist");

        GridRestResponse res = hnd.handleAsync(req).get();

        assertEquals(GridRestResponse.STATUS_FAILED, res.getSuccessStatus());
        assertNotNull(res.getError());
        assertTrue("Error should mention the unknown property, got: " + res.getError(),
            res.getError().contains("does.not.exist"));
    }

    /**
     * `setproperty` must propagate cluster-wide. Setting on grid(0) must be
     * observable from grid(1) — this is the core guarantee of the feature.
     */
    @Test
    public void testSetPropertyPropagatesClusterWide() throws Exception {
        GridPropertyCommandHandler hnd = new GridPropertyCommandHandler(grid(0).context());

        // Use a value that won't collide with other tests.
        String testVal = "37";

        GridRestPropertyRequest req = new GridRestPropertyRequest();
        req.command(PROPERTY_SET);
        req.name("checkpoint.deviation");
        req.value(testVal);

        GridRestResponse res = hnd.handleAsync(req).get();

        assertNull("Unexpected error: " + res.getError(), res.getError());
        assertEquals(GridRestResponse.STATUS_SUCCESS, res.getSuccessStatus());

        GridPropertyCommandResponse dto = (GridPropertyCommandResponse)res.getResponse();

        assertEquals("checkpoint.deviation", dto.getName());
        assertEquals(testVal, dto.getValue());
        assertEquals("Integer", dto.getType());

        // Cluster-wide propagation: read the property from the OTHER node directly.
        Object valFromOtherNode = grid(1).context().distributedConfiguration()
            .property("checkpoint.deviation").get();

        assertEquals(Integer.parseInt(testVal), valFromOtherNode);
    }

    /**
     * `setproperty` without `name` must fail with a missing-parameter error.
     */
    @Test
    public void testSetPropertyMissingName() throws Exception {
        GridPropertyCommandHandler hnd = new GridPropertyCommandHandler(grid(0).context());

        GridRestPropertyRequest req = new GridRestPropertyRequest();
        req.command(PROPERTY_SET);
        req.value("42");

        GridRestResponse res = hnd.handleAsync(req).get();

        assertEquals(GridRestResponse.STATUS_FAILED, res.getSuccessStatus());
        assertTrue("Error should mention 'name', got: " + res.getError(),
            res.getError().contains("name"));
    }

    /**
     * `setproperty` without `val` must fail with a missing-parameter error.
     */
    @Test
    public void testSetPropertyMissingVal() throws Exception {
        GridPropertyCommandHandler hnd = new GridPropertyCommandHandler(grid(0).context());

        GridRestPropertyRequest req = new GridRestPropertyRequest();
        req.command(PROPERTY_SET);
        req.name("checkpoint.deviation");

        GridRestResponse res = hnd.handleAsync(req).get();

        assertEquals(GridRestResponse.STATUS_FAILED, res.getSuccessStatus());
        assertTrue("Error should mention 'val', got: " + res.getError(),
            res.getError().contains("val"));
    }

    /**
     * `setproperty` with a value that fails to parse must fail with a clear error.
     * checkpoint.deviation is Integer; "abc" cannot be parsed.
     */
    @Test
    public void testSetPropertyUnparseableValue() throws Exception {
        GridPropertyCommandHandler hnd = new GridPropertyCommandHandler(grid(0).context());

        GridRestPropertyRequest req = new GridRestPropertyRequest();
        req.command(PROPERTY_SET);
        req.name("checkpoint.deviation");
        req.value("abc");

        GridRestResponse res = hnd.handleAsync(req).get();

        assertEquals(GridRestResponse.STATUS_FAILED, res.getSuccessStatus());
        assertNotNull(res.getError());
    }
}
