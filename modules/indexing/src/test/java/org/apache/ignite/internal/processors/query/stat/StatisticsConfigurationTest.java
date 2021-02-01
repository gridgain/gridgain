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
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for statistics schema.
 */
public class StatisticsConfigurationTest extends StatisticsAbstractTest {
    private Consumer<List<ObjectStatisticsImpl>> checkTotalRows = stats -> {
        long rows = stats.stream()
            .mapToLong(s -> {
                log.info("+++ STAT " + s);

                assertNotNull(s);

                return s.rowCount();
            })
            .sum();

        assertEquals(SMALL_SIZE, rows);
    };

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void updateStatOnChangeTopology() throws Exception {
        startGrid(0);

        createSmallTable("");

        ((IgniteStatisticsManagerImpl)grid(0).context().query().getIndexing().statsManager())
            .statisticSchemaManager()
            .updateStatistics(
                Collections.singletonList(new StatisticsTarget("PUBLIC", "SMALL"))
            );

        waitForStats("PUBLIC", "SMALL", 5000, checkTotalRows);

        System.out.println("+++ START 1");
        startGrid(1);

        createSmallTable("A");
        createSmallTable("B");

        waitForStats("PUBLIC", "SMALL", 5000, checkTotalRows);

        System.out.println("+++ START 2");
        startGrid(2);

        waitForStats("PUBLIC", "SMALL", 5000, checkTotalRows);

        System.out.println("+++ START 3");
        startGrid(3);

        waitForStats("PUBLIC", "SMALL", 5000, checkTotalRows);

        System.out.println("+++ STOP 0");
        stopGrid(0);

        waitForStats("PUBLIC", "SMALL", 5000, checkTotalRows);

        System.out.println("+++ STOP 2");
        stopGrid(2);

        waitForStats("PUBLIC", "SMALL", 5000, checkTotalRows);

        System.out.println("+++ STOP 3");
        stopGrid(3);

        waitForStats("PUBLIC", "SMALL", 5000, checkTotalRows);
    }

    /** */
    private void waitForStats(String schema, String objName, long timeout, Consumer<List<ObjectStatisticsImpl>>... statsCheckers) {
        long t0 = U.currentTimeMillis();

        while (true) {
            try {
                List<IgniteStatisticsManager> mgrs = G.allGrids().stream()
                    .map(ign -> ((IgniteEx)ign).context().query().getIndexing().statsManager())
                    .collect(Collectors.toList());

                List<ObjectStatisticsImpl> stats = mgrs.stream()
                    .map(m -> (ObjectStatisticsImpl)m.getLocalStatistics(schema, objName))
                    .collect(Collectors.toList());


                for (Consumer<List<ObjectStatisticsImpl>> statChecker : statsCheckers)
                    statChecker.accept(stats);

                return;
            } catch (Throwable ex) {
                if (t0 + timeout < U.currentTimeMillis())
                    throw ex;
                else {
                    try {
                        U.sleep(200);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        // No-op.
                    }
                }
            }
        }
    }
}
