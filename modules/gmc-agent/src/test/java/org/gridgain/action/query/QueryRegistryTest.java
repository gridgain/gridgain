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

package org.gridgain.action.query;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.ClusterNodeLocalMapImpl;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Query regixtry test.
 */
public class QueryRegistryTest {
    /**
     * Should remove expired holders.
     */
    @Test
    public void shouldRemoveExpiredHolders() throws InterruptedException {
        QueryHolderRegistry registry = new QueryHolderRegistry(getMockedContext(), Duration.ofSeconds(1));

        String qryId = "qry";
        registry.createQueryHolder(qryId);
        String curId = registry.addCursor(qryId, new QueryCursorImpl<>(new ArrayList<>())).getCursorId();

        Thread.sleep(3000);

        assertNull(registry.findCursor(qryId, curId));
    }

    /**
     * Should not remove holder if they was fetched.
     */
    @Test
    public void shouldNotRemoveExpiredHoldersIfTheyWasFetched() throws InterruptedException {
        QueryHolderRegistry registry = new QueryHolderRegistry(getMockedContext(), Duration.ofSeconds(2));

        String qryId = "qry";
        registry.createQueryHolder(qryId);
        String curId = registry.addCursor(qryId, new QueryCursorImpl<>(new ArrayList<>())).getCursorId();

        for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
            assertNotNull(registry.findCursor(qryId, curId));
        }

        Thread.sleep(3000);

        assertNull(registry.findCursor(qryId, curId));
    }

    /**
     * @return Mocked context.
     */
    private GridKernalContext getMockedContext() {
        GridKernalContext ctx = mock(GridKernalContext.class);
        IgniteEx ignite = mock(IgniteEx.class);
        IgniteClusterEx cluster = mock(IgniteClusterEx.class);

        when(ctx.grid()).thenReturn(ignite);
        when(ignite.cluster()).thenReturn(cluster);
        when(cluster.nodeLocalMap()).thenReturn(new ClusterNodeLocalMapImpl<>());

        return ctx;
    }
}
