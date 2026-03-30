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
 * Defines supported protocols to export metrics.
 */
public enum Protocol {
    /**
     * Exports metrics using OTLP via HTTP, using OpenTelemetry's protobuf model.
     */
    HTTP("http"),

    /**
     * Exports metrics using OTLP via gRPC, using OpenTelemetry's protobuf model.
     */
    GRPC("grpc");

    /**
     * Returns enumerated value for the given {@code type}.
     * Returns {@code null} if the given {@code type} does not match any supported protocol.
     *
     * @param type Type of the protocol.
     * @return Enumerated value.
     */
    @Nullable public static Protocol of(String type) {
        for (Protocol p : values()) {
            if (p.type().equalsIgnoreCase(type))
                return p;
        }

        return null;
    }

    /** String representation of the protocol. */
    private final String type;

    Protocol(String name) {
        this.type = name;
    }

    /**
     * Returns string representation of the protocol.
     *
     * @return String representation of the protocol
     */
    public String type() {
        return type;
    }
}
