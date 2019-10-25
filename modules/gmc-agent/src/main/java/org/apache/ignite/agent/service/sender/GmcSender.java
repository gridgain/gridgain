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

package org.apache.ignite.agent.service.sender;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.agent.WebSocketManager;

import java.util.List;

/**
 * Sender to GMC.
 */
public class GmcSender<T> extends RetryableSender<T> {
    /** Max sleep time seconds. */
    private static final int MAX_SLEEP_TIME_SECONDS = 10;

    /** Manager. */
    private final WebSocketManager mgr;

    /** Topic name. */
    private final String dest;

    /** Logger. */
    private final IgniteLogger log;

    /** Retry count. */
    private int retryCnt;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     * @param cap Capacity.
     * @param dest Destination.
     */
    public GmcSender(GridKernalContext ctx, WebSocketManager mgr, int cap, String dest) {
        super(cap);
        this.mgr = mgr;
        this.dest = dest;
        this.log = ctx.log(GmcSender.class);
    }

    /** {@inheritDoc} */
    @Override protected void sendInternal(List<T> elements) throws Exception {
        Thread.sleep(Math.min(MAX_SLEEP_TIME_SECONDS, retryCnt) * 1000);

        if (!mgr.send(dest, elements)) {
            retryCnt++;

            if (retryCnt == 1)
                log.warning("Failed to send message to GMC, will retry in " + retryCnt * 1000 + " ms");

            throw new IgniteException("Failed to send message to GMC");
        } else
            retryCnt = 0;
    }
}
