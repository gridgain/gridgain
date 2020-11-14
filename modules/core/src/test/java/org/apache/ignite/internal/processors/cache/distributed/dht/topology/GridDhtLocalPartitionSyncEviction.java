/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Extends a DHT partition adding a support for blocking capabilities on clearing.
 */
public class GridDhtLocalPartitionSyncEviction extends GridDhtLocalPartition {
    /** */
    static final int TIMEOUT = 30_000;

    /** */
    private boolean delayed;

    /** */
    private int mode;

    /** */
    private CountDownLatch lock;

    /** */
    private CountDownLatch unlock;

    /**
     * @param ctx Context.
     * @param grp Group.
     * @param id Id.
     * @param recovery Recovery.
     * @param mode Delay mode: 0 - delay before rent, 1 - delay in the middle of clearing, 2 - delay after tryFinishEviction
     *             3 - delay before clearing.
     * @param lock Clearing lock latch.
     * @param unlock Clearing unlock latch.
     */
    public GridDhtLocalPartitionSyncEviction(
        GridCacheSharedContext ctx,
        CacheGroupContext grp,
        int id,
        boolean recovery,
        int mode,
        CountDownLatch lock,
        CountDownLatch unlock
    ) {
        super(ctx, grp, id, recovery);
        this.mode = mode;
        this.lock = lock;
        this.unlock = unlock;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rent() {
        if (mode == 0)
            sync();

        return super.rent();
    }

    /** {@inheritDoc} */
    @Override protected long clearAll(BooleanSupplier stopClo, PartitionsEvictManager.PartitionEvictionTask task) throws NodeStoppingException {
        BooleanSupplier realClo = mode == 1 ? new BooleanSupplier() {
            @Override public boolean getAsBoolean() {
                if (!delayed) {
                    sync();

                    delayed = true;
                }

                return stopClo.getAsBoolean();
            }
        } : stopClo;

        if (mode == 3)
            sync();

        long cnt = super.clearAll(realClo, task);

        if (mode == 2)
            sync();

        return cnt;
    }

    /** */
    protected void sync() {
        lock.countDown();

        try {
            if (!U.await(unlock, TIMEOUT, TimeUnit.MILLISECONDS))
                throw new AssertionError("Failed to wait for lock release");
        } catch (IgniteInterruptedCheckedException e) {
            throw new AssertionError(X.getFullStackTrace(e));
        }
    }
}
