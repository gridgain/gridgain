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
package org.apache.ignite.internal.processors.query.stat;

import java.util.Collections;

import org.apache.ignite.internal.processors.query.stat.config.IgniteStatisticsConfigurationManager;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsCollectConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 * Tests for statistics schema.
 */
public class StatisticsSchemaTest extends StatisticsStorageAbstractTest {
    /** */
    @Test
    public void updateStat() throws Exception {
        ((IgniteStatisticsManagerImpl)grid(0).context().query().getIndexing().statsManager())
            .statisticSchemaManager()
            .updateStatistics(
                Collections.singletonList(new StatisticsTarget("PUBLIC", "SMALL")),
                new StatisticsCollectConfiguration()
            );

        IgniteStatisticsConfigurationManager mgr = ((IgniteStatisticsManagerImpl)grid(0).context().query().getIndexing().statsManager())
            .statisticSchemaManager();

        U.sleep(500);

        System.out.println("+++ RESTART");

        stopGrid(0);

        U.sleep(500);

        startGrid(0);
    }
}
