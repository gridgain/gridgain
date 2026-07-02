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
import org.apache.ignite.client.ClientAtomicLong;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.client.thin.ClientUtils.syncResult;

/**
 * Client atomic long.
 */
class ClientAtomicLongImpl extends AbstractClientAtomic implements ClientAtomicLong {
    /**
     * Constructor.
     *
     * @param name Atomic long name.
     * @param groupName Cache group name.
     * @param ch Channel.
     */
    public ClientAtomicLongImpl(String name, @Nullable String groupName, ReliableChannel ch) {
        super(name, groupName, ch);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public long get() throws IgniteException {
        return syncResult(getAsync());
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> getAsync() throws IgniteException {
        return ch.affinityServiceAsync(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_VALUE_GET, this::writeName, in -> in.in().readLong());
    }

    /** {@inheritDoc} */
    @Override public long incrementAndGet() throws IgniteException {
        return syncResult(incrementAndGetAsync());
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> incrementAndGetAsync() throws IgniteException {
        return addAndGetAsync(1);
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrement() throws IgniteException {
        return syncResult(getAndIncrementAsync());
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> getAndIncrementAsync() throws IgniteException {
        // Same implementation as the sync version.
        return new IgniteClientFutureImpl<>(incrementAndGetAsync().thenApply(v -> v - 1));
    }

    /** {@inheritDoc} */
    @Override public long addAndGet(long l) throws IgniteException {
        return syncResult(addAndGetAsync(l));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> addAndGetAsync(long l) throws IgniteException {
        return ch.affinityServiceAsync(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_VALUE_ADD_AND_GET, out -> {
            writeName(out);
            out.out().writeLong(l);
        }, in -> in.in().readLong());
    }

    /** {@inheritDoc} */
    @Override public long getAndAdd(long l) throws IgniteException {
        return syncResult(getAndAddAsync(l));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> getAndAddAsync(long l) throws IgniteException {
        return new IgniteClientFutureImpl<>(addAndGetAsync(l).thenApply(v -> v - l));
    }

    /** {@inheritDoc} */
    @Override public long decrementAndGet() throws IgniteException {
        return syncResult(decrementAndGetAsync());
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> decrementAndGetAsync() throws IgniteException {
        return addAndGetAsync(-1);
    }

    /** {@inheritDoc} */
    @Override public long getAndDecrement() throws IgniteException {
        return syncResult(getAndDecrementAsync());
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> getAndDecrementAsync() throws IgniteException {
        return new IgniteClientFutureImpl<>(decrementAndGetAsync().thenApply(v -> v + 1));
    }

    /** {@inheritDoc} */
    @Override public long getAndSet(long l) throws IgniteException {
        return syncResult(getAndSetAsync(l));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Long> getAndSetAsync(long l) throws IgniteException {
        return ch.affinityServiceAsync(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_VALUE_GET_AND_SET, out -> {
            writeName(out);
            out.out().writeLong(l);
        }, in -> in.in().readLong());
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(long expVal, long newVal) throws IgniteException {
        return syncResult(compareAndSetAsync(expVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> compareAndSetAsync(long expVal, long newVal) throws IgniteException {
        return ch.affinityServiceAsync(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_VALUE_COMPARE_AND_SET, out -> {
            writeName(out);
            out.out().writeLong(expVal);
            out.out().writeLong(newVal);
        }, in -> in.in().readBoolean());
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return syncResult(removedAsync());
    }

    /** {@inheritDoc} */
    @Override public IgniteClientFuture<Boolean> removedAsync() {
        return ch.affinityServiceAsync(
                cacheId,
                affinityKey(),
                ClientOperation.ATOMIC_LONG_EXISTS,
                this::writeName,
                in -> !in.in().readBoolean()
        );
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_REMOVE, this::writeName, null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientAtomicLongImpl.class, this, super.toString());
    }
}
