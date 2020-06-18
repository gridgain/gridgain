/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

/**
 * Marker future implementation, just like {@link ConnectFuture}, but meaning that we're waiting for the inverse
 * connection.
 */
public class ConnectTriggerFuture extends GridFutureAdapter<GridCommunicationClient> {
    /**
     * Construct the future and link it with the parameter so that when current future is completed, parameter future
     * will be completed as well.
     *
     * @param fut Actual current connect future.
     */
    public ConnectTriggerFuture(ConnectFuture fut) {
        listen(f -> {
            try {
                fut.onDone(f.get());
            }
            catch (Throwable t) {
                fut.onDone(t);
            }
        });
    }
}
