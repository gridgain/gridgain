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

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Eagerly removes expired entries from cache when
 * {@link CacheConfiguration#isEagerTtl()} flag is set.
 */
public class GridCacheTtlManager extends GridCacheManagerAdapter {
    /** @see IgniteSystemProperties#IGNITE_TTL_EXPIRE_BATCH_SIZE */
    public static final int DFLT_TTL_EXPIRE_BATCH_SIZE = 5;

    /** Each cache operation removes this amount of entries with expired TTL. */
    private final int ttlBatchSize = IgniteSystemProperties.getInteger(
        IgniteSystemProperties.IGNITE_TTL_EXPIRE_BATCH_SIZE, DFLT_TTL_EXPIRE_BATCH_SIZE);

    /** Entries pending removal. This collection tracks entries for near cache only. */
    private GridConcurrentSkipListSetEx pendingEntries;

    /** */
    private GridCacheContext dhtCtx;

    /** */
    private final ReentrantReadWriteLock topChangeGuard = new ReentrantReadWriteLock();

    /** */
    private volatile boolean hasRowsToEvict;

    /** */
    private volatile boolean hasTombstonesToEvict;

    /** */
    private final IgniteClosure2X<GridCacheEntryEx, Long, Boolean> expireC =
        new IgniteClosure2X<GridCacheEntryEx, Long, Boolean>() {
            @Override public Boolean applyx(GridCacheEntryEx entry, Long expireTime) {
                boolean touch = !entry.isNear();

                while (true) {
                    try {
                        if (log.isTraceEnabled())
                            log.trace("Trying to remove expired entry from cache: " + entry);

                        if (entry.onTtlExpired(expireTime)) // A successful call will remove an entry from a heap.
                            return true;

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        entry = entry.context().cache().entryEx(entry.key());

                        touch = true;
                    }
                }

                if (touch)
                    entry.touch();

                return false;
            }
        };

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        dhtCtx = cctx.isNear() ? cctx.near().dht().context() : cctx;

        if (cctx.kernalContext().isDaemon() ||
            (cctx.kernalContext().clientNode() && cctx.config().getNearConfiguration() == null))
            return;

        cctx.shared().ttl().register(this);

        pendingEntries = (!cctx.isLocal() && cctx.config().getNearConfiguration() != null) ? new GridConcurrentSkipListSetEx() : null;
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        if (pendingEntries != null)
            pendingEntries.clear();
    }

    /**
     * Unregister this TTL manager of cache from periodical check on expired entries.
     */
    public void unregister() {
        // Ignoring attempt to unregister manager that has never been started.
        if (!starting.get())
            return;

        cctx.shared().ttl().unregister(this);
    }

    /**
     * Adds tracked entry to ttl processor.
     *
     * @param entry Entry to add.
     */
    void addTrackedEntry(GridNearCacheEntry entry) {
        assert entry.lockedByCurrentThread();

        EntryWrapper e = new EntryWrapper(entry);

        pendingEntries.add(e);
    }

    /**
     * @param entry Entry to remove.
     */
    void removeTrackedEntry(GridNearCacheEntry entry) {
        assert entry.lockedByCurrentThread();

        pendingEntries.remove(new EntryWrapper(entry));
    }

    /**
     * @return The size of pending entries.
     * @throws IgniteCheckedException If failed.
     */
    public long pendingSize() throws IgniteCheckedException {
        return (pendingEntries != null ? pendingEntries.sizex() : 0) + cctx.offheap().expiredSize();
    }

    /**
     * @return {@code True} if the underlying pending tree has entries to process.
     * @param tombstone {@code True} is a tombstone check.
     */
    public boolean hasPendingEntries(boolean tombstone) {
        return tombstone ? hasTombstonesToEvict : hasRowsToEvict;
    }

    /**
     * @param tombstone {@code True} is a tombstone check.
     */
    public void setHasPendingEntries(boolean tombstone) {
        if (tombstone)
            hasTombstonesToEvict = true;
        else
            hasRowsToEvict = true;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        try {
            X.println(">>>");
            X.println(">>> TTL processor memory stats [igniteInstanceName=" + cctx.igniteInstanceName() +
                ", cache=" + cctx.name() + ']');
            X.println(">>>   pendingEntriesSize: " + pendingSize());
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to print statistics: " + e, e);
        }
    }

    /**
     * Processes expired entries with default batch size.
     * @return {@code True} if unprocessed expired entries remains.
     */
    public boolean expire() {
        return expire(ttlBatchSize);
    }

    /**
     * Processes specified amount of expired entries.
     * <p>
     * Both TTL cleanup (if TTL is configured and eager TTL is enabled)
     * and tombstones cleanup is executed, so the real amount of deleted entries can be up to {@code amount * 2}.
     *
     * @param amount Limit of processed entries by single call, {@code -1} for no limit.
     * @return {@code True} if unprocessed expired entries remains.
     */
    public boolean expire(int amount) {
        assert cctx != null;

        if (amount == 0)
            return false;

        long now = U.currentTimeMillis();

        if (!topChangeGuard.readLock().tryLock())
            return false;

        try {
            if (pendingEntries != null) {
                GridNearCacheAdapter nearCache = cctx.near();

                int limit = (-1 != amount) ? amount : pendingEntries.sizex();

                for (int cnt = limit; cnt > 0; cnt--) {
                    EntryWrapper e = pendingEntries.firstx();

                    if (e == null || e.expireTime > now)
                        break; // All expired entries are processed.

                    if (pendingEntries.remove(e)) {
                        GridNearCacheEntry nearEntry = nearCache.peekExx(e.key);

                        if (nearEntry != null)
                            expireC.apply(nearEntry, now);
                    }
                }
            }

            if (!cctx.affinityNode())
                return false;  /* Pending tree never contains entries for that cache */

            boolean hasRows = false;
            boolean hasTombstones = false;

            if (cctx.config().isEagerTtl() && !cctx.shared().evict().evictQueue(false).isEmpty())
                hasRows = cctx.offheap().expireRows(expireC, amount, now);

            if (!cctx.shared().evict().evictQueue(true).isEmpty())
                hasTombstones = cctx.offheap().expireTombstones(expireC, amount, now);

            if (amount != -1 && pendingEntries != null) {
                EntryWrapper e = pendingEntries.firstx();

                return e != null && e.expireTime <= now;
            }

            return hasRows || hasTombstones;
        }
        catch (GridDhtInvalidPartitionException e) {
            if (log.isDebugEnabled())
                log.debug("Partition became invalid during rebalancing (will ignore): " + e.partition());

            return false;
        }
        catch (IgniteException e) {
            if (e.hasCause(NodeStoppingException.class)) {
                if (log.isDebugEnabled())
                    log.debug("Failed to expire because node is stopped: " + e);
            }
            else
                throw e;
        }
        finally {
            topChangeGuard.readLock().unlock();
        }

        return false;
    }

    /**
     * @param cctx1 First cache context.
     * @param key1 Left key to compare.
     * @param cctx2 Second cache context.
     * @param key2 Right key to compare.
     * @return Comparison result.
     */
    private static int compareKeys(GridCacheContext cctx1, CacheObject key1, GridCacheContext cctx2, CacheObject key2) {
        int key1Hash = key1.hashCode();
        int key2Hash = key2.hashCode();

        int res = Integer.compare(key1Hash, key2Hash);

        if (res == 0) {
            key1 = (CacheObject)cctx1.unwrapTemporary(key1);
            key2 = (CacheObject)cctx2.unwrapTemporary(key2);

            try {
                byte[] key1ValBytes = key1.valueBytes(cctx1.cacheObjectContext());
                byte[] key2ValBytes = key2.valueBytes(cctx2.cacheObjectContext());

                // Must not do fair array comparison.
                res = Integer.compare(key1ValBytes.length, key2ValBytes.length);

                if (res == 0) {
                    for (int i = 0; i < key1ValBytes.length; i++) {
                        res = Byte.compare(key1ValBytes[i], key2ValBytes[i]);

                        if (res != 0)
                            break;
                    }
                }

                if (res == 0)
                    res = Boolean.compare(cctx1.isNear(), cctx2.isNear());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        return res;
    }

    /**
     * @param fut Future.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public void blockExpire(GridDhtPartitionsExchangeFuture fut) {
        if (log.isDebugEnabled())
            log.debug("Block expiration [initVer=" + fut.initialVersion() + ", ctx=" + dhtCtx + ']');

        topChangeGuard.writeLock().lock();
    }

    /**
     * @param fut Future.
     */
    public void unblockExpire(GridDhtPartitionsExchangeFuture fut) {
        if (log.isDebugEnabled())
            log.debug("Unblock expiration [initVer=" + fut.initialVersion() + ", ctx=" + dhtCtx + ']');

        topChangeGuard.writeLock().unlock();
    }

    /**
     * Entry wrapper.
     */
    private static class EntryWrapper implements Comparable<EntryWrapper> {
        /** Entry expire time. */
        private final long expireTime;

        /** Cache Object Context */
        private final GridCacheContext ctx;

        /** Cache Object Key */
        private final KeyCacheObject key;

        /**
         * @param entry Cache entry to create wrapper for.
         */
        private EntryWrapper(GridCacheMapEntry entry) {
            expireTime = entry.expireTimeUnlocked();

            assert expireTime != 0;

            this.ctx = entry.context();
            this.key = entry.key();
        }

        /** {@inheritDoc} */
        @Override public int compareTo(EntryWrapper o) {
            int res = Long.compare(expireTime, o.expireTime);

            if (res == 0)
                res = compareKeys(ctx, key, o.ctx, o.key);

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof EntryWrapper))
                return false;

            EntryWrapper that = (EntryWrapper)o;

            return expireTime == that.expireTime && compareKeys(ctx, key, that.ctx, that.key) == 0;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = (int)(expireTime ^ (expireTime >>> 32));

            res = 31 * res + key.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EntryWrapper.class, this);
        }
    }

    /**
     * Provides additional method {@code #sizex()}. NOTE: Only the following methods supports this addition:
     * <ul>
     *     <li>{@code #add()}</li>
     *     <li>{@code #remove()}</li>
     *     <li>{@code #pollFirst()}</li>
     * <ul/>
     */
    private static class GridConcurrentSkipListSetEx extends GridConcurrentSkipListSet<EntryWrapper> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Size. */
        private final LongAdder size = new LongAdder();

        /**
         * @return Size based on performed operations.
         */
        public int sizex() {
            return size.intValue();
        }

        /** {@inheritDoc} */
        @Override public boolean add(EntryWrapper e) {
            boolean res = super.add(e);

            if (res)
                size.increment();

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            boolean res = super.remove(o);

            if (res)
                size.decrement();

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public EntryWrapper pollFirst() {
            EntryWrapper e = super.pollFirst();

            if (e != null)
                size.decrement();

            return e;
        }
    }
}
