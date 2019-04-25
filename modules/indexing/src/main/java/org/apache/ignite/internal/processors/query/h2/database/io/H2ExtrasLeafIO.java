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

package org.apache.ignite.internal.processors.query.h2.database.io;

/**
 * Leaf page for H2 row references.
 */
public class H2ExtrasLeafIO extends AbstractH2ExtrasLeafIO {
    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param payloadSize Payload size.
     */
    H2ExtrasLeafIO(short type, int ver, int payloadSize) {
        super(type, ver, 8, payloadSize);
    }
}
