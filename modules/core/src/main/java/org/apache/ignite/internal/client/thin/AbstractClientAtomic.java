/*
 * Copyright 2024 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for client atomics.
 */
class AbstractClientAtomic {
    @GridToStringInclude
    protected final String name;

    @GridToStringInclude
    protected final String groupName;

    protected final ReliableChannel ch;

    @GridToStringInclude
    protected final int cacheId;

    protected AbstractClientAtomic(
            String name,
            @Nullable String groupName,
            ReliableChannel ch) {
        this.name = name;
        this.groupName = groupName;
        this.ch = ch;

        cacheId = ClientUtils.atomicsCacheId(name, groupName);
    }

    /**
     * Gets the name of the atomic.
     *
     * @return Name of the atomic.
     */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractClientAtomic.class, this);
    }

    /**
     * Writes the name.
     *
     * @param out Output channel.
     */
    protected void writeName(PayloadOutputChannel out) {
        try (BinaryRawWriterEx w = new BinaryWriterExImpl(null, out.out(), null, null)) {
            w.writeString(name);
            w.writeString(groupName);
        }
    }

    /**
     * Gets the affinity key for this data structure.
     *
     * @return Affinity key.
     */
    protected String affinityKey() {
        // GridCacheInternalKeyImpl uses name as AffinityKeyMapped.
        return name;
    }
}
