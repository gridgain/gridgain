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

package org.apache.ignite.console.agent;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.console.agent.handlers.WebSocketRouter;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

/**
 * Web socket router tests.
 */
public class WebSocketRouterTest {
    /**
     * Should close HttpClient thread.
     */
    @Test
    public void shouldCloseHttpClient() throws IgniteInterruptedCheckedException {
        try (WebSocketRouter websocket = new WebSocketRouter(AgentLauncher.parseArgs(new String[] {"-t", "token"}))) {
            websocket.start();

            AtomicInteger reconnectCnt = U.field(websocket, "reconnectCnt");

            while (reconnectCnt.get() <= 2)
                U.sleep(100L);
        }

        List<String> httpClientThreads = Thread.getAllStackTraces().keySet().stream()
            .filter(thread -> thread.getName().startsWith("http-client"))
            .map(Thread::getName)
            .collect(toList());

        assertEquals(emptyList(), httpClientThreads);
    }
}
