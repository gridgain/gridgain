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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.EntryLockCallback;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMappedVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedLockFuture;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Cache lock future.
 */
public final class GridDhtLockFuture extends GridCacheFutureAdapter<Boolean>
    implements GridCacheVersionedFuture<Boolean>, GridCacheMappedVersion, DhtLockFuture<Boolean>, GridDhtFuture<Boolean>, EntryLockCallback {
    /** Logger. */
    private static IgniteLogger log;
    /** Cache registry. */
    @GridToStringExclude
    private GridCacheContext<?, ?> cctx;
    /** Transaction. */
    private GridDhtTxLocalAdapter tx;
    /** Lock version. */
    private GridCacheVersion lockVer;
    /** Near node ID. */
    private UUID nearNodeId;
    /** Near lock version. */
    private GridCacheVersion nearLockVer;
    /** Topology version. */
    private AffinityTopologyVersion topVer;
    /** */
    @GridToStringExclude
    private List<GridDhtCacheEntry> entries;
    /** Read flag. */
    private boolean read;
    /** Future ID. */
    private IgniteUuid futId;
    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;
    /** TTL for create operation. */
    private long createTtl;
    /** Lock timeout. */
    private final long timeout;
    /** Skip store flag. */
    private final boolean skipStore;
    /** */
    private volatile boolean locksReady;
    /** */
    private final AtomicInteger locksCntr = new AtomicInteger();

    /**
     * @param cctx Cache context.
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param tx Transaction.
     * @param cnt Number of keys to lock.
     * @param read Read flag.
     * @param timeout Lock acquisition timeout.
     * @param threadId Thread ID.
     * @param createTtl Ttl.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     */
    public GridDhtLockFuture(
        GridCacheContext<?, ?> cctx,
        UUID nearNodeId,
        GridCacheVersion nearLockVer,
        @NotNull AffinityTopologyVersion topVer,
        GridDhtTxLocalAdapter tx,
        int cnt,
        boolean read,
        long timeout,
        long threadId,
        long createTtl,
        boolean skipStore,
        boolean keepBinary) {

        assert nearNodeId != null;
        assert nearLockVer != null;
        assert topVer.topologyVersion() > 0;
        assert tx == null || timeout >= 0;

        this.cctx = cctx;
        this.nearNodeId = nearNodeId;
        this.nearLockVer = nearLockVer;
        this.topVer = topVer;
        this.read = read;
        this.timeout = timeout;
        this.tx = tx;
        this.createTtl = createTtl;
        this.skipStore = skipStore;

        if (tx != null)
            tx.topologyVersion(topVer);

        assert tx == null || threadId == tx.threadId();

        if (tx != null)
            lockVer = tx.xidVersion();
        else {
            lockVer = cctx.mvcc().mappedVersion(nearLockVer);

            if (lockVer == null)
                lockVer = nearLockVer;
        }

        futId = IgniteUuid.randomUuid();

        entries = new ArrayList<>(cnt);

        if (log == null)
            log = cctx.kernalContext().log(getClass());

        if (tx != null) {
            while (true) {
                IgniteInternalFuture fut = tx.lockFut;

                if (fut != null) {
                    if (fut == GridDhtTxLocalAdapter.ROLLBACK_FUT)
                        onError(tx.timedOut() ? tx.timeoutException() : tx.rollbackException());
                    else {
                        // Wait for collocated lock future.
                        assert fut instanceof GridDhtColocatedLockFuture : fut;

                        // Terminate this future if parent(collocated) future is terminated by rollback.
                        fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                            @Override public void apply(IgniteInternalFuture fut) {
                                try {
                                    fut.get();
                                }
                                catch (IgniteCheckedException e) {
                                    onError(e);
                                }
                            }
                        });
                    }

                    return;
                }

                if (tx.updateLockFuture(null, this))
                    return;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @return Entries.
     */
    public List<GridDhtCacheEntry> entries() {
        return entries;
    }

    /**
     * @return Future ID.
     */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Near lock version.
     */
    public GridCacheVersion nearLockVersion() {
        return nearLockVer;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheVersion mappedVersion() {
        return tx == null ? nearLockVer : null;
    }

    /**
     * @return {@code True} if transaction is not {@code null}.
     */
    private boolean inTx() {
        return tx != null;
    }

    /**
     * Adds entry to future.
     *
     * @param entry Entry to add.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public void addEntry(GridDhtCacheEntry entry)
        throws GridCacheEntryRemovedException {
        if (log.isDebugEnabled())
            log.debug("Adding entry: " + entry);

        if (entry == null)
            return;

        entries.add(entry);

        if (!entry.lock(lockVer, this) && timeout < 0) {
            if (log.isDebugEnabled())
                log.debug("Failed to acquire lock with negative timeout: " + entry);

            undoLocks();

            onDone(false);
        }
    }

    /**
     * Undoes all locks.
     */
    private void undoLocks() {
        // Transactions will undo during rollback.
        if (tx != null) {
            if (tx.setRollbackOnly()) {
                if (log.isDebugEnabled())
                    log.debug("Marked transaction as rollback only because locks could not be acquired: " + tx);
            }
            else if (log.isDebugEnabled())
                log.debug("Transaction was not marked rollback-only while locks were not acquired: " + tx);
        }

        for (GridDhtCacheEntry e : entries) {
            try {
                e.unlock(lockVer);
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op
            }
        }
    }

    /**
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (nodeId.equals(nearNodeId)) {
            undoLocks();

            onDone(false);

            return true;
        }

        return false;
    }

    /**
     * @param t Error.
     */
    @Override public void onError(Throwable t) {
        onDone(t);
    }

    /**
     * Callback for whenever entry lock ownership changes.
     *
     * @param entry Entry whose lock ownership changed.
     */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        return onCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Boolean success, @Nullable Throwable err, boolean cancelled) {
        if (log.isDebugEnabled())
            log.debug("Received onComplete(..) callback [success=" + success + ", fut=" + this + ']');

        final boolean stopping = err instanceof NodeStoppingException;

        if (err != null && !stopping)
            undoLocks();

        boolean set = false;

        try {
            if (tx != null) {
                cctx.tm().txContext(tx);

                set = cctx.tm().setTxTopologyHint(tx.topologyVersionSnapshot());

                if (err == null && Boolean.TRUE.equals(success))
                    tx.clearLockFuture(this);
            }

            if (err == null)
                loadMissingFromStore();
        }
        finally {
            if (set)
                cctx.tm().setTxTopologyHint(null);
        }

        if (super.onDone(success, err, cancelled)) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /** */
    public void init() {
        if (log.isDebugEnabled())
            log.debug("Marking local locks as ready for DHT lock future: " + this);

        locksCntr.addAndGet(entries.size());

        locksReady = true;

        if (locksCntr.get() == 0)
            onDone(true);

        if (timeout > 0 && !isDone()) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return futId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtLockFuture.class, this, "super", super.toString());
    }

    /**
     *
     */
    private void loadMissingFromStore() {
        if (!skipStore && read && cctx.readThrough()) {
            final Map<KeyCacheObject, GridDhtCacheEntry> loadMap = new LinkedHashMap<>();

            final GridCacheVersion ver = version();

            for (GridDhtCacheEntry entry : entries) {
                try {
                    entry.unswap();

                    if (!entry.hasValue())
                        loadMap.put(entry.key(), entry);
                }
                catch (GridCacheEntryRemovedException e) {
                    assert false : "Should not get removed exception while holding lock on entry " +
                        "[entry=" + entry + ", e=" + e + ']';
                }
                catch (IgniteCheckedException e) {
                    onDone(e);

                    return;
                }
            }

            try {
                cctx.store().loadAll(
                    null,
                    loadMap.keySet(),
                    new CI2<KeyCacheObject, Object>() {
                        @Override public void apply(KeyCacheObject key, Object val) {
                            // No value loaded from store.
                            if (val == null)
                                return;

                            GridDhtCacheEntry entry0 = loadMap.get(key);

                            try {
                                CacheObject val0 = cctx.toCacheObject(val);

                                long ttl = createTtl;
                                long expireTime;

                                if (ttl == CU.TTL_ZERO)
                                    expireTime = CU.expireTimeInPast();
                                else {
                                    if (ttl == CU.TTL_NOT_CHANGED)
                                        ttl = CU.TTL_ETERNAL;

                                    expireTime = CU.toExpireTime(ttl);
                                }

                                entry0.initialValue(val0,
                                    ver,
                                    ttl,
                                    expireTime,
                                    false,
                                    topVer,
                                    GridDrType.DR_LOAD,
                                    true);
                            }
                            catch (GridCacheEntryRemovedException e) {
                                assert false : "Should not get removed exception while holding lock on entry " +
                                    "[entry=" + entry0 + ", e=" + e + ']';
                            }
                            catch (IgniteCheckedException e) {
                                onDone(e);
                            }
                        }
                    });
            }
            catch (IgniteCheckedException e) {
                onDone(e);
            }
        }
    }

    @Override public Collection<Integer> invalidPartitions() {
        return Collections.emptyList();
    }

    @Override public void onLock(GridCacheMapEntry object) {
        if (locksCntr.decrementAndGet() == 0 && locksReady)
            onDone(true);
    }

    /**
     * Lock request timeout object.
     */
    private class LockTimeoutObject extends GridTimeoutObjectAdapter {
        /**
         * Default constructor.
         */
        LockTimeoutObject() {
            super(timeout);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (!(inTx() && cctx.tm().deadlockDetectionEnabled()))
                undoLocks();

            onDone(false);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }
}
