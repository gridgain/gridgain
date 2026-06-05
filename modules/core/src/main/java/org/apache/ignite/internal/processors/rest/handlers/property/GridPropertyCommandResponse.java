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

package org.apache.ignite.internal.processors.rest.handlers.property;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * REST response payload describing a single distributed property.
 */
public class GridPropertyCommandResponse implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Property name. */
    private String name;

    /** String form of current value, or {@code null} if unset. */
    private String value;

    /** Simple class name of the current value, or {@code null} if unset. */
    private String type;

    /** @return Property name. */
    public String getName() {
        return name;
    }

    /** @param name Property name. */
    public void setName(String name) {
        this.name = name;
    }

    /** @return Current value as a string. */
    public String getValue() {
        return value;
    }

    /** @param value Current value as a string. */
    public void setValue(String value) {
        this.value = value;
    }

    /** @return Simple class name of the value. */
    public String getType() {
        return type;
    }

    /** @param type Simple class name of the value. */
    public void setType(String type) {
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPropertyCommandResponse.class, this);
    }
}
