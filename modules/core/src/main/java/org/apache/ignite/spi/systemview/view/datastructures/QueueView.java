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

package org.apache.ignite.spi.systemview.view.datastructures;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.datastructures.GridCacheQueueProxy;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * Queue representation for a {@link SystemView}.
 */
public class QueueView {
    /** Queue. */
    private final GridCacheQueueProxy<?> queue;

    /**
     * @param queue Queue to view.
     */
    public QueueView(GridCacheQueueProxy<?> queue) {
        this.queue = queue;
    }

    /** @return Queue id. */
    @Order
    public IgniteUuid id() {
        return queue.delegate().id();
    }

    /** @return Queue name. */
    @Order(1)
    public String name() {
        return queue.name();
    }

    /** @return Queue capacity. */
    @Order(2)
    public int capacity() {
        return queue.capacity();
    }

    /** @return Queue size. */
    @Order(3)
    public int size() {
        return queue.size();
    }

    /** @return Cache group name where values for queue stored. */
    @Order(4)
    public String groupName() {
        return queue.groupName();
    }

    /** @return Cache group id where values for queue stored. */
    @Order(5)
    public int groupId() {
        return queue.groupId();
    }

    /** @return If {@code true} then queue capacity is bounded. */
    public boolean bounded() {
        return queue.bounded();
    }

    /** Collocated flag. */
    public boolean collocated() {
        return queue.collocated();
    }

    /** @return If {@code true} then this queue removed. */
    public boolean removed() {
        return queue.removed();
    }
}
