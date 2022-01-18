/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.util;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link IgniteUtils}.
 */
public class IgniteUtilsUnitTest {
    /***/
    private static final int PORT = 5555;

    /***/
    private final List<String> logMessages = new CopyOnWriteArrayList<>();

    /***/
    @Test
    public void shouldNotProduceWarningsWhenClosingAnAlreadyClosedSocket() throws Exception {
        try (EchoServer server = new EchoServer(PORT)) {
            server.start();

            try (SocketChannel channel = connectTo(server)) {
                // closing first time
                channel.close();

                // now close second time and collect logs
                IgniteUtils.close(channel.socket(), logMessagesCollector());
            }
        }

        assertThat(logMessages, is(empty()));
    }

    /***/
    private SocketChannel connectTo(EchoServer server) throws IOException {
        return SocketChannel.open(server.localSocketAddress());
    }

    /***/
    private ListeningTestLogger logMessagesCollector() {
        ListeningTestLogger log = new ListeningTestLogger();

        log.registerListener(logMessages::add);

        return log;
    }
}
