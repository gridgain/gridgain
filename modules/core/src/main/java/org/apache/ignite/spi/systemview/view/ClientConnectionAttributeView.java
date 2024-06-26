/*
 * Copyright 2023 GridGain Systems, Inc. and Contributors.
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

import org.apache.ignite.internal.managers.systemview.walker.Filtrable;
import org.apache.ignite.internal.managers.systemview.walker.Order;

/**
 * Client connection attribute system view row.
 */
public class ClientConnectionAttributeView {
    /** Connection id. */
    private final long connId;

    /** Attribute name. */
    private final String name;

    /** Attribute value. */
    private final String val;

    /**
     * @param connId Connection id.
     * @param name Attribute name.
     * @param val Attribute value.
     */
    public ClientConnectionAttributeView(long connId, String name, String val) {
        this.connId = connId;
        this.name = name;
        this.val = val;
    }

    /** @return Connection id. */
    @Order()
    @Filtrable
    public long connectionId() {
        return connId;
    }

    /** @return Attribute name. */
    @Order(1)
    @Filtrable
    public String name() {
        return name;
    }

    /** @return Attribute value. */
    @Order(2)
    public String value() {
        return val;
    }
}
