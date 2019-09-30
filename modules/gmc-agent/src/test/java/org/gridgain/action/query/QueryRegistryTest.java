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

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.gridgain.AbstractGridWithAgentTest;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;

/**
 * Query regixtry test.
 */
public class QueryRegistryTest extends AbstractGridWithAgentTest {
    /**
     * Should remove expired holders.
     */
    @Test
    public void shouldRemoveExpiredHolders() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();
        QueryHolderRegistry registry = new QueryHolderRegistry(ignite.context(), Duration.ofSeconds(1));

        String qryId = "qry";
        registry.createQueryHolder(qryId);
        String cursorId = registry.addCursor(qryId, new CursorHolder(new QueryCursorImpl<>(new ArrayList<>())));

        Thread.sleep(3000);

        assertNull(registry.findCursor(qryId, cursorId));
    }

    /**
     * Should not remove holder if they was fetched.
     */
    @Test
    public void shouldNotRemoveExpiredHoldersIfTheyWasFetched() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();
        QueryHolderRegistry registry = new QueryHolderRegistry(ignite.context(), Duration.ofSeconds(2));

        String qryId = "qry";
        registry.createQueryHolder(qryId);
        String curId = registry.addCursor(qryId, new CursorHolder(new QueryCursorImpl<>(new ArrayList<>())));

        for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
            assertNotNull(registry.findCursor(qryId, curId));
        }

        Thread.sleep(6000);

        assertNull(registry.findCursor(qryId, curId));
    }
}
