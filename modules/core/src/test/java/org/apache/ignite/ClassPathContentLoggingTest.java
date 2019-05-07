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
package org.apache.ignite;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP;

/**
 *
 */
public class ClassPathContentLoggingTest extends GridCommonAbstractTest {
    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(false, log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP, "true");
    }

    /**
     * Checks the presence of class path content message in log when enabled.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testClassPathContentLogging() throws Exception {
        LogListener lsnr = LogListener
            .matches("List of files containing in classpath")
            .build();

        listeningLog.registerListener(lsnr);

        startGrid(0);

        assertTrue(lsnr.check());
    }
}
