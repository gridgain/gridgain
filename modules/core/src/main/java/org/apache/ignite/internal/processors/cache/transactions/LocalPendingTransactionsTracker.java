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
package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PENDING_TX_TRACKER_ENABLED;

/**
 * Tracks pending transactions for purposes of consistent cut algorithm.
 */
public class LocalPendingTransactionsTracker {
    /** Cctx. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Currently committing transactions. */
    private final Set<GridCacheVersion> currentlyCommittingTxs = U.newConcurrentHashSet();

    /** Tracker enabled. */
    private volatile boolean enabled = IgniteSystemProperties.getBoolean(IGNITE_PENDING_TX_TRACKER_ENABLED, false);

    /** Currently prepared transactions. Counters are incremented on prepare, decremented on commit/rollback. */
    private final ConcurrentHashMap<GridCacheVersion, Integer> preparedCommittedTxsCounters = new ConcurrentHashMap<>();

    /**
     * Transactions that were transitioned to pending state since last {@link #startTrackingPrepared()} call.
     * Transaction remains in this map after commit/rollback.
     */
    private volatile GridConcurrentHashSet<GridCacheVersion> trackedPreparedTxs = new GridConcurrentHashSet<>();

    /** Transactions that were transitioned to committed state since last {@link #startTrackingCommitted()} call. */
    private volatile GridConcurrentHashSet<GridCacheVersion> trackedCommittedTxs = new GridConcurrentHashSet<>();

    /** Written keys to near xid version. */
    private volatile ConcurrentHashMap<KeyCacheObject, Set<GridCacheVersion>> writtenKeysToNearXidVer = new ConcurrentHashMap<>();

    /** Graph of dependent (by keys) transactions. */
    private volatile ConcurrentHashMap<GridCacheVersion, Set<GridCacheVersion>> dependentTransactionsGraph = new ConcurrentHashMap<>();
    // todo GG-13416: maybe handle local sequential consistency with threadId

    /** State rw-lock. */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    /** Track prepared flag. */
    private final AtomicBoolean trackPrepared = new AtomicBoolean(false);

    /** Track committed flag. */
    private final AtomicBoolean trackCommitted = new AtomicBoolean(false);

    /** Tx finish awaiting. */
    private volatile TxFinishAwaiting txFinishAwaiting;

    /**
     * Tx finish awaiting facility.
     */
    private class TxFinishAwaiting {
        /** Future. */
        private final GridFutureAdapter<Set<GridCacheVersion>> fut;

        /** Not committed in timeout txs. */
        private final Set<GridCacheVersion> notCommittedInTimeoutTxs;

        /** Committing txs. */
        private final Set<GridCacheVersion> committingTxs;

        /** Global committing txs added. */
        private volatile boolean globalCommittingTxsAdded;

        /** Awaiting prepared is done. */
        private volatile boolean awaitingPreparedIsDone;

        /** Timeout. */
        private volatile boolean timeout;

        /**
         * @param preparedTxsTimeout Prepared txs timeout.
         * @param committingTxsTimeout Committing txs timeout.
         */
        private TxFinishAwaiting(final long preparedTxsTimeout, final long committingTxsTimeout) {
            assert preparedTxsTimeout > 0 : preparedTxsTimeout;
            assert committingTxsTimeout > 0 : committingTxsTimeout;
            assert committingTxsTimeout >= preparedTxsTimeout : committingTxsTimeout + " < " + preparedTxsTimeout;

            fut = new GridFutureAdapter<>();

            notCommittedInTimeoutTxs = new GridConcurrentHashSet<>(preparedCommittedTxsCounters.keySet());

            committingTxs = U.newConcurrentHashSet(currentlyCommittingTxs);

            if (committingTxsTimeout > preparedTxsTimeout) {
                cctx.time().addTimeoutObject(new GridTimeoutObjectAdapter(preparedTxsTimeout) {
                    @Override public void onTimeout() {
                        awaitingPreparedIsDone = true;

                        if (TxFinishAwaiting.this != txFinishAwaiting || fut.isDone())
                            return;

                        stateLock.readLock().lock();

                        try {
                            if (allCommittingIsFinished())
                                finish();
                            else
                                log.warning("Committing transactions not completed in " + preparedTxsTimeout + " ms: "
                                    + committingTxs);
                        }
                        finally {
                            stateLock.readLock().unlock();
                        }
                    }
                });
            }

            cctx.time().addTimeoutObject(new GridTimeoutObjectAdapter(committingTxsTimeout) {
                @Override public void onTimeout() {
                    timeout = true;

                    if (committingTxsTimeout == preparedTxsTimeout)
                        awaitingPreparedIsDone = true;

                    if (TxFinishAwaiting.this != txFinishAwaiting || fut.isDone())
                        return;

                    stateLock.readLock().lock();

                    try {
                        if (!allCommittingIsFinished())
                            log.warning("Committing transactions not completed in " + committingTxsTimeout + " ms: "
                                + committingTxs);

                        finish();
                    }
                    finally {
                        stateLock.readLock().unlock();
                    }
                }
            });
        }

        /**
         * @param nearXidVer Near xid version.
         */
        void onTxFinished(GridCacheVersion nearXidVer) {
            notCommittedInTimeoutTxs.remove(nearXidVer);

            checkTxsFinished();
        }

        /**
         *
         */
        void checkTxsFinished() {
            if (notCommittedInTimeoutTxs.isEmpty() || awaitingPreparedIsDone && allCommittingIsFinished())
                finish();
        }

        /**
         *
         */
        void finish() {
            if (globalCommittingTxsAdded || timeout) {
                txFinishAwaiting = null;

                fut.onDone(notCommittedInTimeoutTxs.isEmpty() ?
                    Collections.emptySet() :
                    U.sealSet(notCommittedInTimeoutTxs));
            }
        }

        /**
         * @return {@code true} if the set of committing transactions {@code committingTxs} is empty.
         */
        boolean allCommittingIsFinished() {
            committingTxs.retainAll(notCommittedInTimeoutTxs);

            return committingTxs.isEmpty();
        }

        /**
         * @param globalCommittingTxs Global committing txs.
         */
        void addGlobalCommittingTxs(Set<GridCacheVersion> globalCommittingTxs) {
            assert stateLock.writeLock().isHeldByCurrentThread();

            notCommittedInTimeoutTxs.addAll(preparedCommittedTxsCounters.keySet());

            Set<GridCacheVersion> pendingTxs = new HashSet<>(notCommittedInTimeoutTxs);

            pendingTxs.retainAll(globalCommittingTxs);

            committingTxs.addAll(pendingTxs);

            globalCommittingTxsAdded = true;

            assert !fut.isDone() || timeout;

            checkTxsFinished();
        }
    }

    /**
     * @param cctx Cctx.
     */
    public LocalPendingTransactionsTracker(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;

        log = cctx.logger(getClass());
    }

    /**
     * Enable pending transactions tracking.
     */
    void enable() {
        assert cctx.kernalContext().gateway().getState() == GridKernalState.STARTING;

        enabled = true;
    }

    /**
     * @return whether this tracker is enabled or not.
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * Returns a collection of  transactions {@code P2} that are prepared but yet not committed
     * between phase {@code Cut1} and phase {@code Cut2}.
     *
     * @return Collection of prepared transactions.
     */
    public Set<GridCacheVersion> currentlyPreparedTxs() {
        assert stateLock.writeLock().isHeldByCurrentThread();

        return U.sealSet(preparedCommittedTxsCounters.keySet());
    }

    /**
     * Starts tracking transactions that will form a set of transactions {@code P23}
     * that were prepared since phase {@code Cut2} to phase {@code Cut3}.
     */
    public void startTrackingPrepared() {
        assert stateLock.writeLock().isHeldByCurrentThread();
        assert !trackPrepared.get() : "Tracking prepared transactions is already initialized.";

        trackPrepared.set(true);
    }

    /**
     * @return nearXidVer -> prepared WAL ptr
     */
    public Set<GridCacheVersion> stopTrackingPrepared() {
        assert stateLock.writeLock().isHeldByCurrentThread();
        assert trackPrepared.get() : "Tracking prepared transactions is not initialized yet.";

        trackPrepared.set(false);

        Set<GridCacheVersion> res = U.sealSet(trackedPreparedTxs);

        trackedPreparedTxs = new GridConcurrentHashSet<>();

        return res;
    }

    /**
     * Starts tracking committed transactions {@code C12} between phase {@code Cut1} and phase {@code Cut2}.
     */
    public void startTrackingCommitted() {
        assert stateLock.writeLock().isHeldByCurrentThread();
        assert !trackCommitted.get() : "Tracking committed transactions is already initialized.";

        trackCommitted.set(true);
    }

    /**
     * @return nearXidVer -> prepared WAL ptr
     */
    public TrackCommittedResult stopTrackingCommitted() {
        assert stateLock.writeLock().isHeldByCurrentThread();
        assert trackCommitted.get() : "Tracking committed transactions is not initialized yet.";

        trackCommitted.set(false);

        Set<GridCacheVersion> committedTxs = U.sealSet(trackedCommittedTxs);

        Map<GridCacheVersion, Set<GridCacheVersion>> dependentTxs = U.sealMap(dependentTransactionsGraph);

        trackedCommittedTxs = new GridConcurrentHashSet<>();

        writtenKeysToNearXidVer = new ConcurrentHashMap<>();

        dependentTransactionsGraph = new ConcurrentHashMap<>();

        return new TrackCommittedResult(committedTxs, dependentTxs);
    }

    /**
     * @param preparedTxsTimeout Timeout in milliseconds for awaiting of prepared transactions.
     * @param committingTxsTimeout Timeout in milliseconds for awaiting of committing transactions.
     * @return Collection of local transactions in committing state.
     */
    public Set<GridCacheVersion> startTxFinishAwaiting(
        long preparedTxsTimeout, long committingTxsTimeout) {

        assert stateLock.writeLock().isHeldByCurrentThread();

        assert txFinishAwaiting == null : txFinishAwaiting;

        TxFinishAwaiting awaiting = new TxFinishAwaiting(preparedTxsTimeout, committingTxsTimeout);

        txFinishAwaiting = awaiting;

        return awaiting.committingTxs;
    }

    /**
     * @param globalCommittingTxs Global committing transactions.
     * @return Future with collection of transactions that failed to finish within timeout.
     */
    public IgniteInternalFuture<Set<GridCacheVersion>> awaitPendingTxsFinished(
        Set<GridCacheVersion> globalCommittingTxs
    ) {
        assert stateLock.writeLock().isHeldByCurrentThread();

        TxFinishAwaiting awaiting = txFinishAwaiting;

        assert awaiting != null;

        awaiting.addGlobalCommittingTxs(globalCommittingTxs);

        return awaiting.fut;
    }

    /**
     * Freezes state of all tracker collections. Any active transactions that modify collections will
     * wait on readLock().
     * Can be used to obtain consistent snapshot of several collections.
     */
    public void writeLockState() {
        stateLock.writeLock().lock();
    }

    /**
     * Unfreezes state of all tracker collections, releases waiting transactions.
     */
    public void writeUnlockState() {
        stateLock.writeLock().unlock();
    }

    /**
     * @param nearXidVer Near xid version.
     */
    public void onTxPrepared(GridCacheVersion nearXidVer) {
        if (!enabled)
            return;

        stateLock.readLock().lock();

        try {
            preparedCommittedTxsCounters.compute(nearXidVer, (key, value) -> value == null ? 1 : value + 1);

            if (trackPrepared.get())
                trackedPreparedTxs.add(nearXidVer);
        }
        finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * @param nearXidVer Near xid version.
     */
    public void onTxCommitted(GridCacheVersion nearXidVer) {
        if (!enabled)
            return;

        stateLock.readLock().lock();

        try {
            Integer newCntr = preparedCommittedTxsCounters.compute(nearXidVer, (key, value) -> {
                if (value == null || value <= 0) {
                    throw new AssertionError("Committing transaction that was rolled back or concurrently committed " +
                        "[nearXidVer=" + nearXidVer + ", currentCntr=" + value + ']');
                }

                if (value == 1)
                    return null;

                return value - 1;
            });

            if (newCntr == null) {
                currentlyCommittingTxs.remove(nearXidVer);

                if (trackCommitted.get())
                    trackedCommittedTxs.add(nearXidVer);

                checkTxFinishFutureDone(nearXidVer);
            }
        }
        finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * @param nearXidVer Near xid version.
     */
    public void onTxRolledBack(GridCacheVersion nearXidVer) {
        if (!enabled)
            return;

        stateLock.readLock().lock();

        try {
            Integer newCntr = preparedCommittedTxsCounters.compute(nearXidVer, (key, value) -> {
                if (value == null || value <= 1)
                    return null;

                return value - 1;
            });

            if (newCntr == null) {
                currentlyCommittingTxs.remove(nearXidVer);

                checkTxFinishFutureDone(nearXidVer);
            }
        }
        finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * @param nearXidVer Near xid version.
     * @param keys Keys.
     */
    public void onKeysWritten(GridCacheVersion nearXidVer, List<KeyCacheObject> keys) {
        if (!enabled)
            return;

        stateLock.readLock().lock();

        try {
            if (!preparedCommittedTxsCounters.containsKey(nearXidVer))
                throw new AssertionError("Tx should be in PREPARED state when logging data records: " + nearXidVer);

            currentlyCommittingTxs.add(nearXidVer);

            if (!trackCommitted.get())
                return;

            for (KeyCacheObject key : keys) {
                writtenKeysToNearXidVer.compute(key, (keyObj, keyTxsSet) -> {
                    Set<GridCacheVersion> keyTxs = keyTxsSet == null ? new HashSet<>() : keyTxsSet;

                    for (GridCacheVersion previousTx : keyTxs) {
                        dependentTransactionsGraph.compute(previousTx, (tx, depTxsSet) -> {
                            Set<GridCacheVersion> dependentTxs = depTxsSet == null ? new HashSet<>() : depTxsSet;

                            dependentTxs.add(nearXidVer);

                            return dependentTxs;
                        });
                    }

                    keyTxs.add(nearXidVer);

                    return keyTxs;
                });
            }
        }
        finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * @param nearXidVer Near xid version.
     * @param keys Keys.
     */
    public void onKeysRead(GridCacheVersion nearXidVer, List<KeyCacheObject> keys) {
        if (!enabled)
            return;

        stateLock.readLock().lock();

        try {
            if (!preparedCommittedTxsCounters.containsKey(nearXidVer))
                throw new AssertionError("Tx should be in PREPARED state when logging data records: " + nearXidVer);

            currentlyCommittingTxs.add(nearXidVer);

            if (!trackCommitted.get())
                return;

            for (KeyCacheObject key : keys) {
                writtenKeysToNearXidVer.computeIfPresent(key, (keyObj, keyTxsSet) -> {
                    for (GridCacheVersion previousTx : keyTxsSet) {
                        dependentTransactionsGraph.compute(previousTx, (tx, depTxsSet) -> {
                            Set<GridCacheVersion> dependentTxs = depTxsSet == null ? new HashSet<>() : depTxsSet;

                            dependentTxs.add(nearXidVer);

                            return dependentTxs;
                        });
                    }

                    return keyTxsSet;
                });
            }
        }
        finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Resets the state of this tracker.
     */
    public void reset() {
        stateLock.writeLock().lock();

        try {
            txFinishAwaiting = null;

            trackCommitted.set(false);

            trackedCommittedTxs = new GridConcurrentHashSet<>();

            trackPrepared.set(false);

            trackedPreparedTxs = new GridConcurrentHashSet<>();

            writtenKeysToNearXidVer = new ConcurrentHashMap<>();

            dependentTransactionsGraph = new ConcurrentHashMap<>();
        }
        finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * @param nearXidVer Near xid version.
     */
    private void checkTxFinishFutureDone(GridCacheVersion nearXidVer) {
        if (!enabled)
            return;

        TxFinishAwaiting awaiting = txFinishAwaiting;

        if (awaiting != null)
            awaiting.onTxFinished(nearXidVer);
    }
}
