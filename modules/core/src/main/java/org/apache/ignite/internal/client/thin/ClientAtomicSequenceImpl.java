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
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.internal.client.thin.ClientUtils.syncResult;

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

    @GridToStringExclude
    private final AtomicReference<IgniteClientFuture<Long>> lastOp = new AtomicReference<>(IgniteClientFutureImpl.completedFuture(null));

    /**
     * Constructor. Fetches the current sequence value from the server.
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

    /**
     * Constructor with a pre-fetched initial value. Used by the async factory path to avoid a blocking network call.
     *
     * @param name Sequence name.
     * @param groupName Cache group name.
     * @param batchSize Batch size (reserved range).
     * @param ch Channel.
     * @param locVal Initial local value already obtained from the server.
     */
    ClientAtomicSequenceImpl(String name, @Nullable String groupName, int batchSize, ReliableChannel ch, long locVal) {
        super(name, groupName, ch);

        this.batchSize = batchSize;
        this.locVal = locVal;
        this.upBound = locVal;
    }

    /** {@inheritDoc} */
    @Override public long get() throws IgniteException {
        checkRemoved();

        return locVal;
    }

    /** {@inheritDoc} */
    @Override public long incrementAndGet() throws IgniteException {
        return syncResult(incrementAndGetAsync());
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> incrementAndGetAsync() throws IgniteException {
        return internalUpdateAsync(1, true);
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrement() throws IgniteException {
        return syncResult(getAndIncrementAsync());
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> getAndIncrementAsync() throws IgniteException {
        return internalUpdateAsync(1, false);
    }

    /** {@inheritDoc} */
    @Override public long addAndGet(long l) throws IgniteException {
        return syncResult(addAndGetAsync(l));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> addAndGetAsync(long l) throws IgniteException {
        A.ensure(l > 0, " Parameter can't be less then 1: " + l);

        return internalUpdateAsync(l, true);
    }

    /** {@inheritDoc} */
    @Override public long getAndAdd(long l) throws IgniteException {
        return syncResult(getAndAddAsync(l));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> getAndAddAsync(long l) throws IgniteException {
        A.ensure(l > 0, " Parameter can't be less then 1: " + l);

        return internalUpdateAsync(l, false);
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
     * Performs an async update.
     *
     * @param l Value to be added.
     * @param updated If {@code true}, will return updated value, if {@code false}, will return previous value.
     * @return Future that resolves to previous or updated value.
     */
    private IgniteClientFuture<Long> internalUpdateAsync(final long l, final boolean updated) {
        assert l > 0 : "l > 0";

        CompletableFuture<Long> ret0 = new CompletableFuture<>();
        IgniteClientFuture<Long> ret = new IgniteClientFutureImpl<>(ret0);

        if (rmvd) {
            ret0.completeExceptionally(removedError());
            return ret;
        }

        IgniteClientFuture<Long> tail = lastOp.get();
        while (!lastOp.compareAndSet(tail, ret)) {
            tail = lastOp.get();
        }

        tail.whenComplete((v, ignored) -> {
            if (rmvd) {
                ret0.completeExceptionally(removedError());
                return;
            }

            long locVal0 = locVal;
            long newLocVal = locVal0 + l;

            if (newLocVal <= upBound) {
                // Local update within reserved range.
                locVal = newLocVal;

                long val = updated ? newLocVal : locVal0;
                ret0.complete(val);
                return;
            }

            // Update is out of reserved range - reserve new range remotely.
            // Remaining values in old range are accounted for.
            // E.g. if old range is 10-20, locVal is 15, we add 10:
            // locVal = 25
            // upBound = 35
            // globalVal = 36
            long remainingOldRange = upBound - locVal0;
            long newRangeOffset = batchSize + l - remainingOldRange;

            // Remote add and get.
            ch.affinityServiceAsync(
                    cacheId,
                    affinityKey(),
                    ClientOperation.ATOMIC_SEQUENCE_VALUE_ADD_AND_GET,
                    out -> {
                        writeName(out);
                        out.out().writeLong(newRangeOffset);
                    },
                    r -> r.in().readLong()
            )
                    .thenApply(globalVal -> {
                        long oldGlobalVal = globalVal - newRangeOffset;

                        locVal = globalVal - batchSize;
                        upBound = globalVal - 1;

                        if (oldGlobalVal > locVal0) {
                            // Not an initial reservation, adjust.
                            locVal--;
                        }

                        return updated ? locVal : locVal0;
                    }).whenComplete((v2, err) -> {
                        if (err != null) {
                            Throwable actual = err instanceof CompletionException ? err.getCause() : err;
                            if (actual instanceof ClientException) {
                                Throwable cause = actual.getCause();

                                if (cause instanceof ClientServerError &&
                                        ((ClientServerError) cause).getCode() == ClientStatus.RESOURCE_DOES_NOT_EXIST) {
                                    rmvd = true;
                                }
                            }

                            ret0.completeExceptionally(err);
                        } else {
                            ret0.complete(v2);
                        }
                    });
        });

        return ret;
    }

    private void checkRemoved() {
        if (rmvd)
            throw removedError();
    }

    private IgniteException removedError() {
        return new IgniteException("Sequence was removed from cache: " + name);
    }
}
