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

package org.apache.ignite.spi.communication.tcp;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationWorker;

/**
 * Utils to work with communication worker threads.
 */
class CommunicationWorkerThreadUtils {
    /**
     * We need to interrupt communication worker client nodes so that
     * closed connection won't automatically reopen when we don't expect it.
     *
     * @param clientName The name of the client whose threads we want to interrupt.
     * @param log        The logger to use while joining the interrupted threads.
     */
    static void interruptCommWorkerThreads(String clientName, IgniteLogger log) {
        List<Thread> tcpCommWorkerThreads = Thread.getAllStackTraces().keySet().stream()
            .filter(t -> t.getName().contains(CommunicationWorker.WORKER_NAME))
            .filter(t -> t.getName().contains(clientName))
            .collect(Collectors.toList());

        for (Thread tcpCommWorkerThread : tcpCommWorkerThreads) {
            U.interrupt(tcpCommWorkerThread);

            U.join(tcpCommWorkerThread, log);
        }
    }
}
