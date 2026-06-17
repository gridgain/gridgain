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

package org.apache.ignite.internal.client.thin.events;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.events.ClientFailEvent;
import org.apache.ignite.client.events.ClientLifecycleEventListener;
import org.apache.ignite.client.events.ClientStartEvent;
import org.apache.ignite.client.events.ClientStopEvent;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.junit.Test;

/**
 * Tests that throwing lifecycle event listeners do not break the thin client.
 */
public class IgniteClientLifecycleEventListenerFailureTest extends AbstractThinClientTest {
    /**
     * A listener throwing an error from a lifecycle callback must not fail a started client with a spurious
     * fail event, leak the client, or break {@code close()}.
     */
    @Test
    public void testThrowingListenerDoesNotBreakClient() throws Exception {
        startGrid(0);

        AtomicInteger startCnt = new AtomicInteger();
        AtomicInteger failCnt = new AtomicInteger();
        AtomicInteger stopCnt = new AtomicInteger();

        ClientConfiguration cfg = getClientConfiguration(grid(0).cluster().localNode())
            .setEventListeners(new ClientLifecycleEventListener() {
                @Override public void onClientStart(ClientStartEvent evt) {
                    startCnt.incrementAndGet();

                    throw new AssertionError("Listener failure on start");
                }

                @Override public void onClientFail(ClientFailEvent evt) {
                    failCnt.incrementAndGet();
                }

                @Override public void onClientStop(ClientStopEvent evt) {
                    stopCnt.incrementAndGet();

                    throw new AssertionError("Listener failure on stop");
                }
            });

        try (IgniteClient client = Ignition.startClient(cfg)) {
            assertNotNull(client.cacheNames());
        }

        assertEquals(1, startCnt.get());
        assertEquals(0, failCnt.get());
        assertEquals(1, stopCnt.get());
    }
}
