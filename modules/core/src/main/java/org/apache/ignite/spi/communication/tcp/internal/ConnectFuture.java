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

package org.apache.ignite.spi.communication.tcp.internal;

import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connect future which uses as a marker of type connection related with TCP, no SHMEM.
 */
public class ConnectFuture extends GridFutureAdapter<GridCommunicationClient> {
    /**
     * Counter which allows to pause attempts to establish outgoing connection in case when attempt was failed
     * and multiple incoming connections were detected.
     * This may speed-up cluster recovery after network problems.
     */
    private final AtomicInteger incomingConnectionAttempts = new AtomicInteger();

    public int incomingConnectionAttempts() {
        return incomingConnectionAttempts.get();
    }

    public void resetIncomingConnectionAttempts() {
        incomingConnectionAttempts.set(0);
    }

    public int registerIncomingConnectionAttempt() {
        return incomingConnectionAttempts.incrementAndGet();
    }
}
