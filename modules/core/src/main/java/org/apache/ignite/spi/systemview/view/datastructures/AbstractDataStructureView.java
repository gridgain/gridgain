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
import org.apache.ignite.internal.processors.datastructures.AtomicDataStructureProxy;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.systemview.view.SystemView;

/** Abstract class for a {@link SystemView} representation of data structures. */
abstract class AbstractDataStructureView<T extends AtomicDataStructureProxy> {
    /** Data structure instance. */
    protected final T ds;

    /** @param ds Data structure instance. */
    AbstractDataStructureView(T ds) {
        this.ds = ds;
    }

    /** @return Name. */
    @Order
    public String name() {
        return ds.name();
    }

    /** @return Group name. */
    @Order(10)
    public String groupName() {
        return ds.key().groupName();
    }

    /** @return Group id. */
    @Order(11)
    public int groupId() {
        return CU.cacheId(groupName());
    }

    /** @return {@code True} is data structure removed. */
    @Order(12)
    public boolean removed() {
        return ds.removed();
    }
}
