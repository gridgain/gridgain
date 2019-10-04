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

package org.apache.ignite.internal.managers.systemview;

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;

/** Abstract system view. */
abstract class AbstractSystemView<R> implements SystemView<R> {
    /** Name of the view. */
    private final String name;

    /** Description of the view. */
    private final String desc;

    /** Class of the row */
    private final Class<R> rowCls;

    /**
     * Row attribute walker.
     *
     * @see "org.apache.ignite.codegen.MonitoringRowAttributeWalkerGenerator"
     */
    private final SystemViewRowAttributeWalker<R> walker;

    /**
     * @param name Name.
     * @param desc Description.
     * @param rowCls Row class.
     * @param walker Walker.
     */
    AbstractSystemView(String name, String desc, Class<R> rowCls, SystemViewRowAttributeWalker<R> walker) {
        A.notNull(rowCls, "rowCls");
        A.notNull(walker, "walker");

        this.name = name;
        this.desc = desc;
        this.rowCls = rowCls;
        this.walker = walker;
    }

    /** {@inheritDoc} */
    @Override public Class<R> rowClass() {
        return rowCls;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public SystemViewRowAttributeWalker<R> walker() {
        return walker;
    }
}
