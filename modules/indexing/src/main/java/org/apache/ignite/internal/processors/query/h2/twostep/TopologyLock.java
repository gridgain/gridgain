package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import javax.cache.CacheException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Topology locker abstract class.
 * Note: TopologyLocker objects are single-threaded objects.
 */
public abstract class TopologyLock {
    /**
     * Creates cache group topology locker for given caches ids.
     *
     * @param ctx      Kernal context.
     * @param cacheIds Cache ids list.
     * @return Topology locker.
     */
    public static TopologyLock forCaches(GridKernalContext ctx, List<Integer> cacheIds) {
        assert !F.isEmpty(cacheIds);

        if (cacheIds.size() == 1) {
            return new SingleGroupTopologyLock(ctx, cacheContext(ctx, cacheIds.get(0)).groupId());
        }

        TreeSet<Integer> sortedGroupIdSet = new TreeSet<>();

        for (int cacheId : cacheIds)
            sortedGroupIdSet.add(cacheContext(ctx, cacheId).groupId());

        if (sortedGroupIdSet.size() == 1) {
            return new SingleGroupTopologyLock(ctx, sortedGroupIdSet.first());
        }

        return new MultiGroupTopologyLock(ctx, sortedGroupIdSet);
    }

    /** Kernal context. */
    protected final GridKernalContext ctx;

    /**
     * Either locks all topologies or fails unlocking previously locked.
     * Topologies are locked/unlocked in order.
     */
    public abstract void lock();

    /**
     * Unlocked topologies that were previously locked.
     */
    public abstract void unlock();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    protected TopologyLock(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Topology locker for single cache group.
     */
    private static class SingleGroupTopologyLock extends TopologyLock {
        /** Group id. */
        private final int groupId;

        /**
         * Constructor.
         *
         * @param ctx     Kernal context.
         * @param groupId Group id.
         */
        SingleGroupTopologyLock(GridKernalContext ctx, int groupId) {
            super(ctx);

            this.groupId = groupId;
        }

        /** {@inheritDoc} */
        @Override public void lock() {
            groupContext(ctx, groupId).topology().readLock();
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            groupContext(ctx, groupId).topology().readUnlock();
        }
    }

    /**
     * Multi group topologies locker.
     */
    private static class MultiGroupTopologyLock extends TopologyLock {
        /** Group ids. */
        private final Collection<Integer> groupIds;

        /** Locked groups. */
        private final List<Integer> locked;

        /**
         * Constructor.
         *
         * @param ctx      Kernal context.
         * @param groupIds Groups ids.
         */
        MultiGroupTopologyLock(GridKernalContext ctx, Collection<Integer> groupIds) {
            super(ctx);

            this.groupIds = groupIds;
            this.locked = new ArrayList<>(groupIds.size());
        }

        /** {@inheritDoc} */
        @Override public void lock() {
            try {
                for (int id : groupIds) {
                    CacheGroupContext grp = groupContext(ctx, id);

                    grp.topology().readLock();

                    locked.add(id);
                }
            }
            catch (Throwable th) {
                unlock();

                throw th;
            }
        }

        /** {@inheritDoc} */
        @Override public void unlock() {
            for (int id : locked) {
                CacheGroupContext grp = ctx.cache().cacheGroup(id);

                if (grp != null)
                    grp.topology().readUnlock();
            }

            locked.clear();
        }
    }

    /**
     * Returns cache context for given cache Id.
     *
     * @param ctx     Kernal context.
     * @param cacheId Cache id.
     * @return Cache context.
     * @throws CacheException If cache context wasn't found.
     */
    private static GridCacheContext<Object, Object> cacheContext(GridKernalContext ctx, Integer cacheId) {
        GridCacheContext<Object, Object> cctx = ctx.cache().context().cacheContext(cacheId);

        if (cctx != null)
            return cctx;

        throw new CacheException(String.format("Cache not found on local node (was concurrently destroyed?) " +
            "[cacheId=%d]", cacheId));
    }

    /**
     * Returns cache group context for given cache Id.
     *
     * @param ctx     Kernal context.
     * @param groupId Cache group id.
     * @return Cache group context.
     * @throws CacheException If cache group context wasn't found.
     */
    private static CacheGroupContext groupContext(GridKernalContext ctx, int groupId) {
        CacheGroupContext grp = ctx.cache().cacheGroup(groupId);

        if (grp != null)
            return grp;

        throw new CacheException(String.format("CacheGroup not found on local node (was concurrently destroyed?) " +
            "[groupId=%d]", groupId));
    }
}
