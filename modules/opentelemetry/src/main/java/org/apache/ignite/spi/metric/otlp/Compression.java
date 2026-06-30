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

package org.apache.ignite.spi.metric.otlp;

import org.jetbrains.annotations.Nullable;

/**
 * Defines supported compression type.
 */
public enum Compression {
    /** No compression. */
    NONE("none"),

    /** Gzip compression. */
    GZIP("gzip");

    /**
     * Returns enumerated value for the given {@code type}.
     * Returns {@code null} if the given {@code type} does not match any compression type.
     *
     * @param type Type of the protocol.
     * @return Enumerated value.
     */
    @Nullable public static Compression of(String type) {
        for (Compression c : values()) {
            if (c.type().equalsIgnoreCase(type))
                return c;
        }

        return null;
    }

    /** String representation of the compression type. */
    private final String type;

    Compression(String name) {
        this.type = name;
    }

    /**
     * Returns string representation of the compression type.
     *
     * @return String representation of the compression type.
     */
    public String type() {
        return type;
    }
}
