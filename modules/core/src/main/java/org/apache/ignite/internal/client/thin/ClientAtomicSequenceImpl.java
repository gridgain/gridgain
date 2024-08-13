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

import org.apache.ignite.IgniteException;
import org.apache.ignite.client.ClientAtomicSequence;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Client atomic sequence.
 */
class ClientAtomicSequenceImpl extends AbstractClientAtomic implements ClientAtomicSequence {
    /** Local value of sequence. */
    @GridToStringInclude(sensitive = true)
    private volatile long locVal;

    /**  Upper bound of local counter. */
    @GridToStringExclude
    private long upBound;

    /**  Sequence batch size */
    private volatile int batchSize;

    /** Removed flag. */
    private volatile boolean rmvd;

    /**
     * Constructor.
     *
     * @param name Atomic long name.
     * @param groupName Cache group name.
     * @param batchSize Batch size (reserved range).
     * @param ch Channel.
     */
    public ClientAtomicSequenceImpl(String name, @Nullable String groupName, int batchSize, ReliableChannel ch) {
        super(name, groupName, ch);

        this.batchSize = batchSize;

        locVal = ch.affinityService(
                cacheId,
                affinityKey(),
                ClientOperation.ATOMIC_SEQUENCE_VALUE_GET,
                this::writeName,
                in -> in.in().readLong());

        upBound = locVal;
    }

    /** {@inheritDoc} */
    @Override public long get() throws IgniteException {
        checkRemoved();

        return locVal;
    }

    /** {@inheritDoc} */
    @Override public long incrementAndGet() throws IgniteException {
        return internalUpdate(1, true);
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrement() throws IgniteException {
        return internalUpdate(1, false);
    }

    /** {@inheritDoc} */
    @Override public long addAndGet(long l) throws IgniteException {
        A.ensure(l > 0, " Parameter can't be less then 1: " + l);

        return internalUpdate(l, true);
    }

    /** {@inheritDoc} */
    @Override public long getAndAdd(long l) throws IgniteException {
        A.ensure(l > 0, " Parameter can't be less then 1: " + l);

        return internalUpdate(l, false);
    }

    /** {@inheritDoc} */
    @Override public int batchSize() {
        return batchSize;
    }

    /** {@inheritDoc} */
    @Override public synchronized void batchSize(int size) {
        A.ensure(size > 0, " Batch size can't be less then 0: " + size);

        batchSize = size;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        boolean exists = ch.affinityService(
                cacheId,
                affinityKey(),
                ClientOperation.ATOMIC_SEQUENCE_EXISTS,
                this::writeName,
                in -> in.in().readBoolean());

        rmvd = !exists;

        return !exists;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ch.affinityService(
                cacheId,
                affinityKey(),
                ClientOperation.ATOMIC_SEQUENCE_REMOVE,
                this::writeName,
                null);

        rmvd = true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientAtomicSequenceImpl.class, this, super.toString());
    }

    /**
     * Performs an update.
     *
     * @param l Value to be added.
     * @param updated If {@code true}, will return updated value, if {@code false}, will return previous value.
     * @return Previous or updated value.
     */
    private synchronized long internalUpdate(final long l, final boolean updated) {
        assert l > 0 : "l > 0";
        checkRemoved();

        long locVal0 = locVal;
        long newLocVal = locVal0 + l;

        if (newLocVal <= upBound) {
            // Local update within reserved range.
            locVal = newLocVal;

            return updated ? newLocVal : locVal0;
        }

        // Update is out of reserved range - reserve new range remotely.
        // Remaining values in old range are accounted for.
        // E.g. if old range is 10-20, locVal is 15, we add 10:
        // locVal = 25
        // upBound = 35
        // globalVal = 36
        long remainingOldRange = upBound - locVal0;
        long newRangeOffset = batchSize + l - remainingOldRange;

        long globalVal = remoteAddAndGet(newRangeOffset);
        long oldGlobalVal = globalVal - newRangeOffset;

        locVal = globalVal - batchSize;
        upBound = globalVal - 1;

        if (oldGlobalVal > locVal0) {
            // Not an initial reservation, adjust.
            locVal--;
        }

        return updated ? locVal : locVal0;
    }

    private long remoteAddAndGet(long l) {
        try {
            return ch.affinityService(
                    cacheId,
                    affinityKey(),
                    ClientOperation.ATOMIC_SEQUENCE_VALUE_ADD_AND_GET,
                    out -> {
                        writeName(out);
                        out.out().writeLong(l);
                    },
                    r -> r.in().readLong());
        } catch (ClientException e) {
            Throwable cause = e.getCause();

            if (cause instanceof ClientServerError &&
                    ((ClientServerError) cause).getCode() == ClientStatus.RESOURCE_DOES_NOT_EXIST) {
                rmvd = true;
            }

            throw e;
        }
    }

    private void checkRemoved() {
        if (rmvd)
            throw new IgniteException("Sequence was removed from cache: " + name);
    }
}
