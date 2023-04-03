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

package org.apache.ignite.util;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.TimeZone;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.Checkpointer;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedProperty;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.DFLT_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.HISTORICAL_REBALANCE_THRESHOLD_DMS_KEY;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

/**
 * Checks command line property commands.
 */
public class GridCommandHandlerPropertiesTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** */
    @Before
    public void init() {
        injectTestSystemOut();
    }

    /** */
    @After
    public void clear() {
    }

    /**
     * Check the command '--property help'.
     * Steps:
     */
    @Test
    public void testHelp() {
        assertEquals(EXIT_CODE_OK, execute("--property", "help"));

        String out = testOut.toString();

        assertContains(log, out, "Print property command help:");
        assertContains(log, out, "control.(sh|bat) --property help");

        assertContains(log, out, "Print list of available properties:");
        assertContains(log, out, "control.(sh|bat) --property list");

        assertContains(log, out, "Get the property value:");
        assertContains(log, out, "control.(sh|bat) --property get --name <property_name>");

        assertContains(log, out, "Set the property value:");
        assertContains(log, out, "control.(sh|bat) --property set --name <property_name> --val <property_value>");
    }

    /**
     * Check the command '--property list'.
     * Steps:
     */
    @Test
    public void testList() {
        assertEquals(EXIT_CODE_OK, execute("--property", "list"));

        String out = testOut.toString();

        for (DistributedChangeableProperty<Serializable> pd : crd.context()
            .distributedConfiguration().properties())
            assertContains(log, out, pd.getName());
    }

    /**
     * Check the command '--property get'.
     * Steps:
     */
    @Test
    public void testGet() {
        for (DistributedChangeableProperty<Serializable> pd : crd.context()
            .distributedConfiguration().properties()) {
            assertEquals(EXIT_CODE_OK, execute("--property", "get", "--name", pd.getName()));
            String out = testOut.toString();

            assertContains(log, out, pd.getName() + " = " + pd.get());
        }
    }

    /**
     * Check the set command for property 'sql.disabledFunctions'.
     * Steps:
     */
    @Test
    public void testPropertyDisabledSqlFunctions() {
        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", "sql.disabledFunctions",
                "--val", "LENGTH, SESSION_ID"
            )
        );

        for (Ignite ign : G.allGrids()) {
            Set<String> disabledFuncs = ((IgniteH2Indexing)((IgniteEx)ign).context().query().getIndexing())
                .distributedConfiguration().disabledFunctions();

            assertEquals(2, disabledFuncs.size());

            assertTrue(disabledFuncs.contains("LENGTH"));
            assertTrue(disabledFuncs.contains("SESSION_ID"));
        }
    }

    /**
     * Checks the set command for property 'checkpoint.deviation'.
     */
    @Test
    public void testPropertyCheckpointDeviation() {
        for (Ignite ign : G.allGrids()) {
            if (ign.configuration().isClientMode())
                continue;

            DistributedProperty<Integer> cpFreqDeviation = U.field(((IgniteEx)ign).context().cache().context().database(),
                "cpFreqDeviation");

            assertNull(cpFreqDeviation.get());
        }

        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", "checkpoint.deviation",
                "--val", "20"
            )
        );

        for (Ignite ign : G.allGrids()) {
            if (ign.configuration().isClientMode())
                continue;

            DistributedProperty<Integer> cpFreqDeviation = U.field(((IgniteEx)ign).context().cache().context().database(),
                "cpFreqDeviation");

            assertNotNull(cpFreqDeviation.get());

            assertEquals(20, cpFreqDeviation.get().intValue());
        }
    }

    /**
     * Checks the set command for property 'checkpoint.frequency'.
     */
    @Test
    public void testPropertyCheckpointFrequency() throws Exception {
        for (Ignite ign : G.allGrids()) {
            if (ign.configuration().isClientMode())
                continue;

            DistributedProperty<Long> cpFreq = U.field(((IgniteEx)ign).context().cache().context().database(),
                "cpFreq");

            assertNull(cpFreq.get());
        }

        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", "checkpoint.frequency",
                "--val", "21000"
            )
        );

        for (Ignite ign : G.allGrids()) {
            if (ign.configuration().isClientMode())
                continue;

            GridCacheDatabaseSharedManager database = (GridCacheDatabaseSharedManager) ((IgniteEx) ign).context()
                .cache().context().database();

            // Check that the value gets distributed to all nodes.
            DistributedProperty<Long> cpFreq = U.field(database, "cpFreq");

            assertNotNull(cpFreq.get());

            assertEquals(21000L, cpFreq.get().longValue());

            // Check that the dynamic property overrides the value from configuration in calculations.
            long nextCpInterval = nextCpInterval((IgniteEx) ign);

            assertEquals(21000L, nextCpInterval);
        }
    }

    /**
     * Calculates next CP interval (in ms).
     *
     * @param ignite Ignite instance from which to get the interval.
     * @return Next checkpoint interval.
     * @throws ReflectiveOperationException If something goes wrong.
     */
    private static long nextCpInterval(IgniteEx ignite)
        throws ReflectiveOperationException {
        GridCacheDatabaseSharedManager database = (GridCacheDatabaseSharedManager) ignite.context()
            .cache().context().database();

        Method nextCheckpointIntervalMtd = Checkpointer.class.getDeclaredMethod("nextCheckpointInterval");
        nextCheckpointIntervalMtd.setAccessible(true);

        return (long) (Long) nextCheckpointIntervalMtd.invoke(database.getCheckpointer());
    }

    /**
     * Makes sure that non-positive values of 'checkpoint.frequency' are ignored for the purposes of next CP interval
     * calculation.
     */
    @Test
    public void nonPositiveCheckpointFrequencyOverrideIsIgnored() throws Exception {
        // Check zero value.

        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", "checkpoint.frequency",
                "--val", "0"
            )
        );

        assertThatNextCpIntervalIsPositive();

        // Check negative value.

        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", "checkpoint.frequency",
                "--val", "-1"
            )
        );

        assertThatNextCpIntervalIsPositive();
    }

    /**
     * Makes sure that next CP interval is positive on all started Ignite instances.
     *
     * @throws ReflectiveOperationException If something goes wrong.
     */
    private static void assertThatNextCpIntervalIsPositive() throws ReflectiveOperationException {
        for (Ignite ign : G.allGrids()) {
            if (ign.configuration().isClientMode())
                continue;

            long nextCpInterval = nextCpInterval((IgniteEx) ign);

            assertThat(nextCpInterval, is(greaterThan(0L)));
        }
    }

    /**
     * Check the set command for property 'sql.defaultQueryTimeout'.
     * Steps:
     */
    @Test
    public void testPropertyDefaultQueryTimeout() {
        int dfltVal = ((IgniteH2Indexing)crd.context().query().getIndexing())
            .distributedConfiguration().defaultQueryTimeout();

        int newVal = dfltVal + 1000;

        assertEquals(EXIT_CODE_OK, execute("--property", "set", "--name", "sql.defaultQueryTimeout", "--val",
            Integer.toString(newVal)));

        for (Ignite ign : G.allGrids()) {
            assertEquals(
                "Invalid default query timeout on node: " + ign.name(),
                newVal,
                ((IgniteH2Indexing)((IgniteEx)ign).context().query().getIndexing())
                    .distributedConfiguration().defaultQueryTimeout()
            );
        }

        assertEquals(
            EXIT_CODE_UNEXPECTED_ERROR,
            execute(
                "--property", "set",
                "--name", "sql.defaultQueryTimeout",
                "--val", "invalidVal"
            )
        );
    }

    /**
     * Check the set command for property 'sql.timeZone'.
     * Steps:
     */
    @Test
    public void testPropertyTimeZone() {
        TimeZone newVal = TimeZone.getTimeZone("PST");

        assertEquals(EXIT_CODE_OK, execute("--property", "set", "--name", "sql.timeZone", "--val",
            newVal.getID()));

        for (Ignite ign : G.allGrids()) {
            assertEquals(
                "Invalid time zone on node: " + ign.name(),
                newVal,
                ((IgniteH2Indexing)((IgniteEx)ign).context().query().getIndexing())
                    .distributedConfiguration().timeZone()
            );
        }
    }

    /**
     * Check the set command for property 'wal.rebalance.threshold'.
     */
    @Test
    public void testPropertyWalRebalanceThreshold() {
        assertDistributedPropertyEquals(HISTORICAL_REBALANCE_THRESHOLD_DMS_KEY, DFLT_PDS_WAL_REBALANCE_THRESHOLD, true);

        int newVal = DFLT_PDS_WAL_REBALANCE_THRESHOLD * 2;

        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", HISTORICAL_REBALANCE_THRESHOLD_DMS_KEY,
                "--val", Integer.toString(newVal)
            )
        );

        assertDistributedPropertyEquals(HISTORICAL_REBALANCE_THRESHOLD_DMS_KEY, newVal, true);
    }

    /**
     * Validates that distributed property has specified value across all nodes.
     *
     * @param propName Distributed property name.
     * @param expected Expected property value.
     * @param onlyServerMode Ignore client nodes.
     * @param <T> Property type.
     */
    private <T extends Serializable> void assertDistributedPropertyEquals(String propName, T expected, boolean onlyServerMode) {
        for (Ignite ign : G.allGrids()) {
            IgniteEx ignEx = (IgniteEx) ign;

            if (onlyServerMode && ign.configuration().isClientMode())
                continue;

            DistributedChangeableProperty<Serializable> prop =
                ignEx.context().distributedConfiguration().property(propName);

            assertEquals(prop.get(), expected);
        }
    }
}
