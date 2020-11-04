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
import java.util.Set;
import java.util.TimeZone;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

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
}
