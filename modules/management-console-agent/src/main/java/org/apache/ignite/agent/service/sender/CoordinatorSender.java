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

import java.util.List;
import org.apache.ignite.internal.GridKernalContext;

/**
 * Sender to coordinator.
 */
public class CoordinatorSender<T> extends RetryableSender<T> {
    /** Context. */
    private final GridKernalContext ctx;

    /** Topic name. */
    private final String topicName;

    /**
     * @param ctx Context.
     * @param topicName Topic name.
     */
    public CoordinatorSender(GridKernalContext ctx, String topicName) {
        this(ctx, topicName, DEFAULT_QUEUE_CAP);
    }

    /**
     * @param ctx Context.
     * @param topicName Topic name.
     * @param cap Capacity.
     */
    public CoordinatorSender(GridKernalContext ctx, String topicName, int cap) {
        super(cap);

        this.ctx = ctx;
        this.topicName = topicName;
    }

    /** {@inheritDoc} */
    @Override protected void sendInternal(List<T> elements) {
        ctx.grid().message(ctx.grid().cluster().forOldest()).send(topicName, (Object) elements);
    }
}
