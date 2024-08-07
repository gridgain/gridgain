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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.IgniteException;
import org.apache.ignite.client.ClientAtomicSequence;
import org.jetbrains.annotations.Nullable;

/**
 * Client atomic sequence.
 */
class ClientAtomicSequenceImpl extends AbstractClientAtomic implements ClientAtomicSequence {
    // TODO: Manage batches locally.
    private int batchSize;

    /**
     * Constructor.
     *
     * @param name Atomic long name.
     * @param groupName Cache group name.
     * @param ch Channel.
     */
    public ClientAtomicSequenceImpl(String name, @Nullable String groupName, ReliableChannel ch) {
        super(name, groupName, ch);
    }

    /** {@inheritDoc} */
    @Override public long get() throws IgniteException {
        return ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_SEQUENCE_VALUE_GET, this::writeName, in -> in.in().readLong());
    }

    /** {@inheritDoc} */
    @Override public long incrementAndGet() throws IgniteException {
        return addAndGet(1);
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrement() throws IgniteException {
        return incrementAndGet() - 1;
    }

    /** {@inheritDoc} */
    @Override public long addAndGet(long l) throws IgniteException {
        // TODO: Local increment
        return ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_VALUE_ADD_AND_GET, out -> {
            writeName(out);
            out.out().writeLong(l);
        }, in -> in.in().readLong());
    }

    /** {@inheritDoc} */
    @Override public long getAndAdd(long l) throws IgniteException {
        return addAndGet(l) - l;
    }

    @Override
    public int batchSize() {
        return batchSize;
    }

    @Override
    public void batchSize(int size) {
        batchSize = size;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_SEQUENCE_EXISTS, this::writeName,
                in -> !in.in().readBoolean());
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_SEQUENCE_REMOVE, this::writeName, null);
    }
}
