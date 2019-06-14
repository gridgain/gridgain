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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheLockCandidates;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMappedVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedLockFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Cache lock future.
 */
public final class GridDhtLockFuture extends GridCacheFutureAdapter<Boolean>
    implements GridCacheVersionedFuture<Boolean>, GridCacheMappedVersion, DhtLockFuture<Boolean>, GridDhtFuture<Boolean> {
    /** Logger. */
    private static IgniteLogger log;
    /** Lock timeout. */
    private final long timeout;
    /** Pending locks. */
    private final Collection<KeyCacheObject> pendingLocks;
    /** Skip store flag. */
    private final boolean skipStore;
    /** Cache registry. */
    @GridToStringExclude
    private GridCacheContext<?, ?> cctx;
    /** Near node ID. */
    private UUID nearNodeId;
    /** Near lock version. */
    private GridCacheVersion nearLockVer;
    /** Topology version. */
    private AffinityTopologyVersion topVer;
    /** Thread. */
    private long threadId;
    /**
     * Keys locked so far.
     *
     * Thread created this object iterates over entries and tries to lock each of them.
     * If it finds some entry already locked by another thread it registers callback which will be executed
     * by the thread owning the lock.
     *
     * Thus access to this collection must be synchronized except cases
     * when this object is yet local to the thread created it.
     */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @GridToStringExclude
    private List<GridDhtCacheEntry> entries;
    /** Future ID. */
    private IgniteUuid futId;
    /** Lock version. */
    private GridCacheVersion lockVer;
    /** Read flag. */
    private boolean read;
    /** Error. */
    private Throwable err;
    /** Timed out flag. */
    private volatile boolean timedOut;
    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;
    /** Transaction. */
    private GridDhtTxLocalAdapter tx;
    /** TTL for create operation. */
    private long createTtl;

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

        this.threadId = threadId;

        if (tx != null)
            lockVer = tx.xidVersion();
        else {
            lockVer = cctx.mvcc().mappedVersion(nearLockVer);

            if (lockVer == null)
                lockVer = nearLockVer;
        }

        futId = IgniteUuid.randomUuid();

        entries = new ArrayList<>(cnt);
        pendingLocks = U.newHashSet(cnt);

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
    public Collection<GridDhtCacheEntry> entries() {
        return F.view(entries, F.notNull());
    }

    /**
     * Need of synchronization here is explained in the field's {@link GridDhtLockFuture#entries} comment.
     *
     * @return Copy of entries collection.
     */
    private synchronized Collection<GridDhtCacheEntry> entriesCopy() {
        return new ArrayList<>(entries());
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
     * @return {@code True} if transaction is implicit.
     */
    private boolean implicitSingle() {
        return tx != null && tx.implicitSingle();
    }

    /**
     * Adds entry to future.
     *
     * @param entry Entry to add.
     * @return Lock candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     * @throws GridDistributedLockCancelledException If lock is canceled.
     */
    @Nullable public GridCacheMvccCandidate addEntry(GridDhtCacheEntry entry)
        throws GridCacheEntryRemovedException, GridDistributedLockCancelledException {
        if (log.isDebugEnabled())
            log.debug("Adding entry: " + entry);

        if (entry == null)
            return null;

        // Check if the future is timed out.
        if (timedOut)
            return null;

        // Add local lock first, as it may throw GridCacheEntryRemovedException.
        GridCacheMvccCandidate c = entry.addDhtLocal(
            nearNodeId,
            nearLockVer,
            topVer,
            threadId,
            lockVer,
            null,
            timeout,
            /*reenter*/false,
            inTx(),
            implicitSingle(),
            false
        );

        if (c == null && timeout < 0) {
            if (log.isDebugEnabled())
                log.debug("Failed to acquire lock with negative timeout: " + entry);

            onComplete(false, false, true);

            return null;
        }

        synchronized (this) {
            entries.add(c == null || c.reentry() ? null : entry);

            if (c != null && !c.reentry())
                pendingLocks.add(entry.key());
        }

        // Double check if the future has already timed out.
        if (timedOut) {
            entry.removeLock(lockVer);

            return null;
        }

        return c;
    }

    /**
     * Undoes all locks.
     */
    private void undoLocks() {
        // Transactions will undo during rollback.
        Collection<GridDhtCacheEntry> entriesCp = entriesCopy();

        if (tx != null) {
            if (tx.setRollbackOnly()) {
                if (log.isDebugEnabled())
                    log.debug("Marked transaction as rollback only because locks could not be acquired: " + tx);
            }
            else if (log.isDebugEnabled())
                log.debug("Transaction was not marked rollback-only while locks were not acquired: " + tx);
        }

        for (GridCacheEntryEx e : entriesCp) {
            try {
                e.removeLock(lockVer);
            }
            catch (GridCacheEntryRemovedException ignored) {
                while (true) {
                    try {
                        e = cctx.cache().peekEx(e.key());

                        if (e != null)
                            e.removeLock(lockVer);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Attempted to remove lock on removed entry (will retry) [ver=" +
                                lockVer + ", entry=" + e + ']');
                    }
                }
            }
        }
    }

    /**
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (nodeId.equals(nearNodeId)) {
            onComplete(false, false, true);

            return true;
        }

        return false;
    }

    /**
     * Sets all local locks as ready. After local locks are acquired, lock requests will be sent to remote nodes.
     * Thus, no reordering will occur for remote locks as they are added after local locks are acquired.
     */
    private void readyLocks() {
        if (log.isDebugEnabled())
            log.debug("Marking local locks as ready for DHT lock future: " + this);

        for (int i = 0; i < entries.size(); i++) {
            while (true) {
                GridDistributedCacheEntry entry = entries.get(i);

                if (entry == null)
                    break; // While.

                try {
                    CacheLockCandidates owners = entry.readyLock(lockVer);

                    if (timeout < 0) {
                        if (owners == null || !owners.hasCandidate(lockVer)) {
                            // We did not send any requests yet.
                            onComplete(false, false, true);

                            return;
                        }
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Current lock owners [entry=" + entry +
                            ", owners=" + owners +
                            ", fut=" + this + ']');
                    }

                    break; // Inner while loop.
                }
                // Possible in concurrent cases, when owner is changed after locks
                // have been released or cancelled.
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to ready lock because entry was removed (will renew).");

                    entries.set(i, (GridDhtCacheEntry)cctx.cache().entryEx(entry.key(), topVer));
                }
            }
        }
    }

    /**
     * @param t Error.
     */
    @Override public void onError(Throwable t) {
        synchronized (this) {
            if (err != null)
                return;

            err = t;
        }

        onComplete(false, false, true);
    }

    /**
     * Callback for whenever entry lock ownership changes.
     *
     * @param entry Entry whose lock ownership changed.
     */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (isDone() || (inTx() && (tx.remainingTime() == -1 || tx.isRollbackOnly())))
            return false; // Check other futures.

        if (log.isDebugEnabled())
            log.debug("Received onOwnerChanged() callback [entry=" + entry + ", owner=" + owner + "]");

        if (owner != null && owner.version().equals(lockVer)) {
            boolean isEmpty;

            synchronized (this) {
                if (!pendingLocks.remove(entry.key()))
                    return false;

                isEmpty = pendingLocks.isEmpty();
            }

            if (isEmpty)
                onComplete(true, false, true);

            return true;
        }

        return false;
    }

    /**
     * @return {@code True} if locks have been acquired.
     */
    private synchronized boolean checkLocks() {
        return pendingLocks.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        if (onCancelled())
            onComplete(false, false, true);

        return isCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Boolean success, @Nullable Throwable err) {
        // Protect against NPE.
        if (success == null) {
            assert err != null;

            success = false;
        }

        assert err == null || !success;

        if (log.isDebugEnabled())
            log.debug("Received onDone(..) callback [success=" + success + ", err=" + err + ", fut=" + this + ']');

        // If locks were not acquired yet, delay completion.
        if (isDone() || (err == null && success && !checkLocks()))
            return false;

        synchronized (this) {
            if (this.err == null)
                this.err = err;
        }

        return onComplete(success, err instanceof NodeStoppingException, true);
    }

    /**
     * Completeness callback.
     *
     * @param success {@code True} if lock was acquired.
     * @param stopping {@code True} if node is stopping.
     * @param unlock {@code True} if locks should be released.
     * @return {@code True} if complete by this operation.
     */
    private synchronized boolean onComplete(boolean success, boolean stopping, boolean unlock) {
        if (log.isDebugEnabled())
            log.debug("Received onComplete(..) callback [success=" + success + ", fut=" + this + ']');

        if (!success && !stopping && unlock)
            undoLocks();

        boolean set = false;

        if (tx != null) {
            cctx.tm().txContext(tx);

            set = cctx.tm().setTxTopologyHint(tx.topologyVersionSnapshot());

            if (success)
                tx.clearLockFuture(this);
        }

        try {
            if (err == null && !stopping)
                loadMissingFromStore();
        }
        finally {
            if (set)
                cctx.tm().setTxTopologyHint(null);
        }

        if (super.onDone(success, err)) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            // Clean up.
            cctx.mvcc().removeVersionedFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /**
     *
     */
    public void init() {
        if (F.isEmpty(entries)) {
            onComplete(true, false, true);

            return;
        }

        readyLocks();

        if (timeout > 0 && !isDone()) { // Prevent memory leak if future is completed by call to readyLocks.
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
        Collection<KeyCacheObject> locks;

        synchronized (this) {
            locks = new HashSet<>(pendingLocks);
        }

        return S.toString(GridDhtLockFuture.class, this,
            "pendingLocks", locks,
            "super", super.toString());
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
            long longOpsDumpTimeout = cctx.tm().longOperationsDumpTimeout();

            synchronized (GridDhtLockFuture.this) {
                if (log.isDebugEnabled() || timeout >= longOpsDumpTimeout) {
                    String msg = dumpPendingLocks();

                    if (log.isDebugEnabled())
                        log.debug(msg);
                    else
                        log.warning(msg);
                }

                timedOut = true;

                // Stop locks and responses processing.
                pendingLocks.clear();
            }

            boolean releaseLocks = !(inTx() && cctx.tm().deadlockDetectionEnabled());

            onComplete(false, false, releaseLocks);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }

        /**
         * NB! Should be called in synchronized block on {@link GridDhtLockFuture} instance.
         *
         * @return String representation of pending locks.
         */
        private String dumpPendingLocks() {
            StringBuilder sb = new StringBuilder();

            sb.append("Transaction tx=").append(tx.getClass().getSimpleName());
            sb.append(" [xid=").append(tx.xid());
            sb.append(", xidVer=").append(tx.xidVersion());
            sb.append(", nearXid=").append(tx.nearXidVersion().asGridUuid());
            sb.append(", nearXidVer=").append(tx.nearXidVersion());
            sb.append(", nearNodeId=").append(tx.nearNodeId());
            sb.append(", label=").append(tx.label());
            sb.append("] timed out, can't acquire lock for ");

            Iterator<KeyCacheObject> locks = pendingLocks.iterator();

            boolean found = false;

            while (!found && locks.hasNext()) {
                KeyCacheObject key = locks.next();

                GridCacheEntryEx entry = cctx.cache().entryEx(key, topVer);

                while (true) {
                    try {
                        Collection<GridCacheMvccCandidate> candidates = entry.localCandidates();

                        for (GridCacheMvccCandidate candidate : candidates) {
                            IgniteInternalTx itx = cctx.tm().tx(candidate.version());

                            if (itx != null && candidate.owner() && !candidate.version().equals(tx.xidVersion())) {
                                sb.append("key=").append(key).append(", owner=");
                                sb.append("[xid=").append(itx.xid()).append(", ");
                                sb.append("xidVer=").append(itx.xidVersion()).append(", ");
                                sb.append("nearXid=").append(itx.nearXidVersion().asGridUuid()).append(", ");
                                sb.append("nearXidVer=").append(itx.nearXidVersion()).append(", ");
                                sb.append("label=").append(itx.label()).append(", ");
                                sb.append("nearNodeId=").append(candidate.otherNodeId()).append("]");
                                sb.append(", queueSize=").append(candidates.isEmpty() ? 0 : candidates.size() - 1);

                                found = true;

                                break;
                            }
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException e) {
                        entry = cctx.cache().entryEx(key, topVer);
                    }
                }
            }

            return sb.toString();
        }
    }
}
