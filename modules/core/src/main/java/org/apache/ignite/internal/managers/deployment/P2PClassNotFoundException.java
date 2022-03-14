/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.managers.deployment;

/**
 * A specialication of {@link ClassNotFoundException} thrown when a class cannot be loaded during P2P class-loading.
 * The idea is to distinguish P2P class loading issues (which are usually non-critical) from the rest class loading
 * issues (which are critical).
 */
public class P2PClassNotFoundException extends ClassNotFoundException {
    /***/
    private static final long serialVersionUID = 0L;

    /***/
    public P2PClassNotFoundException(String message) {
        super(message);
    }

    /***/
    public P2PClassNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
