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

package org.apache.ignite.internal;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests warnings emitted for known {@link UnsupportedSystemProperty unsupported} Ignite system properties.
 */
public class UnsupportedSystemPropertyWarningTest extends GridCommonAbstractTest {
    /** Logger that node start is checked against. */
    private ListeningTestLogger testLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (testLog != null)
            cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        testLog = null;

        stopAllGrids();

        super.afterTest();
    }

    /** Verifies the warning message matches the format mandated by the ticket. */
    @Test
    public void testWarningMessageFormat() {
        assertEquals(
            "Property IGNITE_SQL_FORCE_LAZY_RESULT_SET is not supported in GridGain " +
                "(reason: Not available in GridGain's H2-based SQL engine). " +
                "See https://www.gridgain.com/docs/gridgain8/latest/installation-guide/migrating-from-ignite.",
            UnsupportedSystemProperty.IGNITE_SQL_FORCE_LAZY_RESULT_SET.warning());
    }

    /** A configured unsupported property is warned about; an unset one is not. */
    @Test
    @WithSystemProperty(key = "IGNITE_USE_BINARY_ARRAYS", value = "true")
    public void testWarnForConfigured() {
        ListeningTestLogger log = new ListeningTestLogger();

        LogListener set = LogListener
            .matches("Property IGNITE_USE_BINARY_ARRAYS is not supported in GridGain")
            .times(1)
            .build();

        LogListener unset = LogListener
            .matches("IGNITE_CALCITE_EXEC_IN_BUFFER_SIZE")
            .build();

        log.registerListener(set);
        log.registerListener(unset);

        UnsupportedSystemProperty.warnForConfigured(log);

        assertTrue("Expected a warning for the configured unsupported property.", set.check());
        assertFalse("Did not expect a warning for a property that is not set.", unset.check());
    }

    /** The warning is emitted during node start (verifies the {@code IgniteKernal} hook). */
    @Test
    @WithSystemProperty(key = "IGNITE_PERF_STAT_FLUSH_SIZE", value = "1024")
    public void testWarnOnNodeStart() throws Exception {
        testLog = new ListeningTestLogger();

        LogListener lsnr = LogListener
            .matches("Property IGNITE_PERF_STAT_FLUSH_SIZE is not supported in GridGain")
            .atLeast(1)
            .build();

        testLog.registerListener(lsnr);

        startGrid(0);

        assertTrue("Expected an unsupported-property warning during node start.", lsnr.check());
    }
}
