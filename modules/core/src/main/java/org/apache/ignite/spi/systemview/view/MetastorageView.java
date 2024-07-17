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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;

/**
 * Metastorage key representation for a {@link SystemView}.
 */
public class MetastorageView {
    /** */
    private final String name;

    /** */
    private final String value;

    /**
     * @param name Name.
     * @param value Value
     */
    public MetastorageView(String name, String value) {
        this.name = name;
        this.value = value;
    }

    /** */
    @Order
    public String name() {
        return name;
    }

    /** */
    @Order(1)
    public String value() {
        return value;
    }
}
