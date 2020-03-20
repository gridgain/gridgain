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
package org.apache.ignite.internal.processors.query.oom;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.QueryMemoryManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

/**
 * Test for query offloading logging.
 */
public class DiskSpillingLoggingTest extends DiskSpillingAbstractTest {
    /** {@inheritDoc} */
    @Override protected boolean startClient() {
        return false;
    }

    /** */
    @Test
    public void testLogsWithOffloading() {
        LogListener logLsnr = LogListener
            .matches("User's query started")
            .andMatches("Offloading started for query")
            .andMatches("Created spill file")
            .andMatches("Deleted spill file")
            .andMatches("User's query completed")
            .build();

        testLog(grid(0)).registerListener(logLsnr);

        setGlobalQuota("60%");
        setDefaultQueryQuota(SMALL_MEM_LIMIT);
        setOffloadingEnabled(true);

        checkQuery(Result.SUCCESS_WITH_OFFLOADING,
            "SELECT depId, code, age, COUNT(*), SUM(salary),  LISTAGG(uuid) " +
            "FROM person GROUP BY age, depId, code ");

        assertTrue(logLsnr.check());
    }

    /** */
    @Test
    public void testLogsNoOffloading() {
        LogListener logLsnr = LogListener
            .matches("User's query started")
            .andMatches("User's query completed")
            .build();

        testLog(grid(0)).registerListener(logLsnr);

        setGlobalQuota("60%");
        setDefaultQueryQuota(HUGE_MEM_LIMIT);
        setOffloadingEnabled(true);

        checkQuery(Result.SUCCESS_NO_OFFLOADING,
            "SELECT depId, code, age, COUNT(*), SUM(salary),  LISTAGG(uuid) " +
                "FROM person GROUP BY age, depId, code ");

        assertTrue(logLsnr.check());
    }

    /** */
    @Test
    public void testLogsOnDynamicConfiguration() {
        LogListener logLsnr = LogListener
            .matches("SQL query global quota was set to 20.")
            .andMatches("SQL query memory quota was set to 10.")
            .andMatches("SQL query query offloading enabled flag was set to false")
            .build();

        testLog(grid(0)).registerListener(logLsnr);

        setGlobalQuota("20");
        setDefaultQueryQuota(10);
        setOffloadingEnabled(false);

        assertTrue(logLsnr.check());

        logLsnr = LogListener
            .matches("SQL query global quota was set to 21.")
            .andMatches("SQL query memory quota was set to 11.")
            .andMatches("SQL query query offloading enabled flag was set to true")
            .build();

        testLog(grid(0)).registerListener(logLsnr);

        setGlobalQuota("21");
        setDefaultQueryQuota(11);
        setOffloadingEnabled(true);

        assertTrue(logLsnr.check());
    }

    /**
     * Setup and return test log.
     *
     * @return Test logger.
     */
    private ListeningTestLogger testLog(IgniteEx node) {
        ListeningTestLogger testLog = new ListeningTestLogger(true, log);

        GridTestUtils.setFieldValue(memoryManager(node), "log", testLog);
        GridTestUtils.setFieldValue(runningQueryManager(node), "log", testLog);

        return testLog;
    }

    /** */
    private void setDefaultQueryQuota(long newQuota) {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memMgr = memoryManager((IgniteEx)node);

            memMgr.setQueryQuota(String.valueOf(newQuota));
        }
    }

    /** */
    private void setOffloadingEnabled(boolean enabled) {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memMgr = memoryManager((IgniteEx)node);

            memMgr.setOffloadingEnabled(enabled);
        }
    }

    /** */
    private void setGlobalQuota(String newQuota) {
        for (Ignite node : G.allGrids()) {
            QueryMemoryManager memMgr = memoryManager((IgniteEx)node);

            memMgr.setGlobalQuota(newQuota);
        }
    }
}
