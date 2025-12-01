/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.service;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTaskNameHashKey;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.continuous.GridContinuousMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICES_SET_REMOTE_FILTER_ON_START;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests that unnecessary CQ events are not transferred to the thick client
 * when cache-based implementation os the service framework is used.
 */
public class GridServiceContinuousQueryNotificationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests that system cache based service framework does not send unnecessary CQ messages to remote nodes.
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SERVICES_SET_REMOTE_FILTER_ON_START, value = "true")
    @WithSystemProperty(key = IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED, value = "false")
    public void testFilteringUnnecessaryEvents() throws Exception {
        IgniteEx ignite = startGrid(0);
        startClientGrid(1);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite);
        spi.record(GridContinuousMessage.class);

        IgniteInternalCache<GridTaskNameHashKey, String> utilityCache = ignite.context().cache().utilityCache();
        try (Transaction tx = utilityCache.txStart(PESSIMISTIC, SERIALIZABLE)) {
            utilityCache.put(new GridTaskNameHashKey(123), "test-value");

            tx.commit();
        }

        List<Object> recordedMsgs = new ArrayList<>();
        GridTestUtils.waitForCondition(() -> {
            // This call drains already recorded messages,
            // so the messages should be accumulated in an external collection.
            List<Object> msgs = spi.recordedMessages(false);

            recordedMsgs.addAll(msgs);

            return !recordedMsgs.isEmpty();
        }, 500);

        recordedMsgs.addAll(spi.recordedMessages(true));

        assertTrue("Unexpected GridContinuousMessage was delivered to the thick client.", recordedMsgs.isEmpty());
    }
}
