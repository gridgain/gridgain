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

package org.apache.ignite.client;

import org.apache.ignite.internal.client.thin.ProtocolBitmaskFeature;

/**
 * Feature not supported by server exception.
 */
public class ClientFeatureNotSupportedByServerException extends ClientException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param msg the detail message.
     */
    public ClientFeatureNotSupportedByServerException(String msg) {
        super(msg);
    }

    /**
     * @param feature Feature.
     */
    public ClientFeatureNotSupportedByServerException(ProtocolBitmaskFeature feature) {
        super("Feature " + feature.name() + " is not supported by the server");
    }

    /**
     * Constructs a new exception with the specified cause and detail message.
     *
     * @param msg the detail message.
     * @param cause the cause.
     */
    public ClientFeatureNotSupportedByServerException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
