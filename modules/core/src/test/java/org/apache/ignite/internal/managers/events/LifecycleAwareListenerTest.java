/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.managers.events;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Tests local event listener that implements {@link LifecycleAware}.
 */
public class LifecycleAwareListenerTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartStop() throws Exception {
        TestLocalListener lsnr = new TestLocalListener();

        IgniteConfiguration cfg = getConfiguration().setLocalEventListeners(F.asMap(lsnr, new int[]{EVT_NODE_JOINED}));

        try (Ignite ignite = startGrid(cfg)) {
            assertTrue(lsnr.isStarted);
            assertFalse(lsnr.isStopped);
        }

        assertTrue(lsnr.isStopped);
    }

    /** */
    private static class TestLocalListener implements IgnitePredicate<Event>, LifecycleAware {
        /** Is started. */
        private boolean isStarted;

        /** Is stopped. */
        private boolean isStopped;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteException {
            assertFalse(isStarted);

            assertNotNull(ignite);

            isStarted = true;
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteException {
            assertFalse(isStopped);

            isStopped = true;
        }
    }
}
