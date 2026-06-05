/*
 * Copyright 2026 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.rest.request;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * REST request for distributed property commands ({@code listproperties}, {@code getproperty}, {@code setproperty}).
 */
public class GridRestPropertyRequest extends GridRestRequest {
    /** Property name. */
    private String name;

    /** Property value (set only). */
    private String value;

    /** @return Property name. */
    public String name() {
        return name;
    }

    /** @param name Property name. */
    public void name(String name) {
        this.name = name;
    }

    /** @return Property value (set only). */
    public String value() {
        return value;
    }

    /** @param value Property value (set only). */
    public void value(String value) {
        this.value = value;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestPropertyRequest.class, this, super.toString());
    }
}
