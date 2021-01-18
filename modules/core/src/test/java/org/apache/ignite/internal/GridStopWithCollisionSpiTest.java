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

package org.apache.ignite.internal;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Collision job context test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridStopWithCollisionSpiTest extends GridCommonAbstractTest {
    /** */
    private LogListener lsnr = null;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        FifoQueueCollisionSpi collision = new FifoQueueCollisionSpi();
        collision.setParallelJobsNumber(1);

        cfg.setCollisionSpi(collision);

        lsnr = LogListener.matches(logStr -> logStr.contains("UnsupportedOperationException")
            && logStr.contains("ConcurrentLinkedHashMap.clear")).build();

        ListeningTestLogger listener = new ListeningTestLogger(log, lsnr);

        cfg.setGridLogger(listener);

        return cfg;
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testCollisionJobContext() throws Exception {
        startGrid(0);

        stopGrid(0);

        assertFalse(lsnr.check());
    }
}
