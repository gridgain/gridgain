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

/**
 * SSL Protocol.
 */
public enum SslProtocol {
    /** Supports some version of TLS; may support other versions. */
    TLS,

    /** Supports RFC 2246: TLS version 1.0 ; may support other versions. */
    TLSv1,

    /** Supports RFC 4346: TLS version 1.1 ; may support other versions. */

    TLSv1_1,

    /** Supports RFC 5246: TLS version 1.2 ; may support other versions. */
    TLSv1_2,

    /** Supports RFC 8446: TLS version 1.3 ; may support other versions. */
    TLSv1_3
}
