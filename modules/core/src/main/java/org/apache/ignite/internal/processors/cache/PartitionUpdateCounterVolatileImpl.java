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

package org.apache.ignite.internal.processors.cache;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.Nullable;

/**
 * Partition update counter for volatile cache groups.
 * Doesn't track gaps in update sequence.
 */
public class PartitionUpdateCounterVolatileImpl implements PartitionUpdateCounter {
    /** Counter of applied updates in partition. */
    private final AtomicLong cntr = new AtomicLong();

    /**
     * Initial counter is set to update with max sequence number after WAL recovery.
     */
    private volatile long initCntr;

    /** */
    private final CacheGroupContext grp;

    /**
     * @param grp Group.
     */
    public PartitionUpdateCounterVolatileImpl(CacheGroupContext grp) {
        this.grp = grp;
    }

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        cntr.set(initUpdCntr);

        initCntr = initUpdCntr;
    }

    /** {@inheritDoc} */
    @Override public long initial() {
        return initCntr;
    }

    /** {@inheritDoc} */
    @Override public long get() {
        return cntr.get();
    }

    /** {@inheritDoc} */
    @Override public long next() {
        return cntr.incrementAndGet();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("StatementWithEmptyBody")
    @Override public void update(long val) {
        long cur;

        while (val > (cur = cntr.get()) && !cntr.compareAndSet(cur, val));
    }

    /** {@inheritDoc} */
    @Override public boolean update(long start, long delta) {
        long cur, val = start + delta;

        while (true) {
            if (val <= (cur = cntr.get()))
                return false;

            if (cntr.compareAndSet(cur, val))
                return true;
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void updateInitial(long start, long delta) {
        update(start + delta);

        initCntr = get();
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        return new GridLongList();
    }

    /** {@inheritDoc} */
    @Override public long reserve(long delta) {
        return next(delta);
    }

    /** {@inheritDoc} */
    @Override public long next(long delta) {
        return cntr.getAndAdd(delta);
    }

    /** {@inheritDoc} */
    @Override public boolean sequential() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public @Nullable byte[] getBytes() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public synchronized void reset() {
        initCntr = 0;

        cntr.set(0);
    }

    /** {@inheritDoc} */
    @Override public void resetInitialCounter() {
        initCntr = 0;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionUpdateCounterVolatileImpl cntr = (PartitionUpdateCounterVolatileImpl)o;

        return this.cntr.get() == cntr.cntr.get();
    }

    /** {@inheritDoc} */
    @Override public long reserved() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return get() == 0;
    }

    /** {@inheritDoc} */
    @Override public Iterator<long[]> iterator() {
        return new GridEmptyIterator<>();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Counter [init=" + initCntr + ", val=" + get() + ']';
    }

    /** {@inheritDoc} */
    @Override public CacheGroupContext context() {
        return grp;
    }
}
