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

package org.apache.ignite.agent.processor.sender;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.agent.WebSocketManager;
import org.apache.ignite.internal.GridKernalContext;

/**
 * Sender to Management Console.
 */
public class ManagementConsoleSender<T> extends RetryableSender<T> {
    /** Max sleep time seconds between retries. */
    private static final int MAX_SLEEP_TIME_SECONDS = 10;

    /** Manager. */
    private final WebSocketManager mgr;

    /** Topic name. */
    private final String dest;

    /** Retry count. */
    private int retryCnt;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     * @param dest Destination.
     * @param threadNamePrefix the prefix to use for the names of newly created threads.
     */
    public ManagementConsoleSender(GridKernalContext ctx, WebSocketManager mgr, String dest, String threadNamePrefix) {
        this(ctx, mgr, dest, threadNamePrefix, DEFAULT_QUEUE_CAP);
    }

    /**
     * @param ctx Context.
     * @param mgr Manager.
     * @param dest Destination.
     * @param threadNamePrefix the prefix to use for the names of newly created threads.
     * @param cap Capacity.
     */
    public ManagementConsoleSender(GridKernalContext ctx, WebSocketManager mgr, String dest, String threadNamePrefix, int cap) {
        super(ctx.log(ManagementConsoleSender.class), threadNamePrefix, cap);

        this.mgr = mgr;
        this.dest = dest;
    }

    /** {@inheritDoc} */
    @Override protected void sendInternal(List<T> elements) throws Exception {
        Thread.sleep(Math.min(MAX_SLEEP_TIME_SECONDS, retryCnt) * 1000);

        if (!mgr.send(dest, elements)) {
            retryCnt++;

            if (retryCnt == 1)
                log.warning("Failed to send message to Management Console, will retry in " + retryCnt * 1000 + " ms");

            throw new IgniteException("Failed to send message to Management Console");
        }
        else
            retryCnt = 0;
    }
}
