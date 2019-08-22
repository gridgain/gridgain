/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.ignite.internal.processors.cache;

import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.UnregisteredBinaryTypeException;
import org.apache.ignite.internal.UnregisteredClassException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheLazyPlainVersionedEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_EXPIRED;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 *
 */
class AtomicCacheUpdateClosure implements IgniteCacheOffheapManager.OffheapInvokeClosure {
    /** */
    private final GridCacheMapEntry entry;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private GridCacheVersion newVer;

    /** */
    private GridCacheOperation op;

    /** */
    private Object writeObj;

    /** */
    private Object[] invokeArgs;

    /** */
    private final boolean readThrough;

    /** */
    private final boolean writeThrough;

    /** */
    private final boolean keepBinary;

    /** */
    private final IgniteCacheExpiryPolicy expiryPlc;

    /** */
    private final boolean primary;

    /** */
    private final boolean verCheck;

    /** */
    private final CacheEntryPredicate[] filter;

    /** */
    private final long explicitTtl;

    /** */
    private final long explicitExpireTime;

    /** */
    private GridCacheVersion conflictVer;

    /** */
    private final boolean conflictResolve;

    /** */
    private final boolean intercept;

    /** */
    private final Long updateCntr;

    /** */
    private final boolean skipInterceptorOnConflict;

    /** */
    private GridCacheUpdateAtomicResult updateRes;

    /** */
    private IgniteTree.OperationType treeOp;

    /** */
    private CacheDataRow newRow;

    /** */
    private CacheDataRow oldRow;

    /** OldRow expiration flag. */
    private boolean oldRowExpiredFlag;

    /** Disable interceptor invocation onAfter* methods flag. */
    private boolean wasIntercepted;

    /** */
    AtomicCacheUpdateClosure(
        GridCacheMapEntry entry,
        AffinityTopologyVersion topVer,
        GridCacheVersion newVer,
        GridCacheOperation op,
        Object writeObj,
        Object[] invokeArgs,
        boolean readThrough,
        boolean writeThrough,
        boolean keepBinary,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean primary,
        boolean verCheck,
        @Nullable CacheEntryPredicate[] filter,
        long explicitTtl,
        long explicitExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean conflictResolve,
        boolean intercept,
        @Nullable Long updateCntr,
        boolean skipInterceptorOnConflict) {
        assert op == UPDATE || op == DELETE || op == TRANSFORM : op;

        this.entry = entry;
        this.topVer = topVer;
        this.newVer = newVer;
        this.op = op;
        this.writeObj = writeObj;
        this.invokeArgs = invokeArgs;
        this.readThrough = readThrough;
        this.writeThrough = writeThrough;
        this.keepBinary = keepBinary;
        this.expiryPlc = expiryPlc;
        this.primary = primary;
        this.verCheck = verCheck;
        this.filter = filter;
        this.explicitTtl = explicitTtl;
        this.explicitExpireTime = explicitExpireTime;
        this.conflictVer = conflictVer;
        this.conflictResolve = conflictResolve;
        this.intercept = intercept;
        this.updateCntr = updateCntr;
        this.skipInterceptorOnConflict = skipInterceptorOnConflict;

        switch (op) {
            case UPDATE:
                treeOp = IgniteTree.OperationType.PUT;

                break;

            case DELETE:
                treeOp = IgniteTree.OperationType.REMOVE;

                break;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheDataRow oldRow() {
        return oldRow;
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow newRow() {
        return newRow;
    }

    /** {@inheritDoc} */
    @Override public IgniteTree.OperationType operationType() {
        return treeOp;
    }

    public GridCacheUpdateAtomicResult updateResult() {
        return updateRes;
    }

    public CacheDataRow oldDataRow() {
        return oldRow;
    }

    public boolean oldRowExpiredFlag() {
        return oldRowExpiredFlag;
    }

    public GridCacheVersion newVersion() {
        return newVer;
    }

    public GridCacheOperation cacheOperation() {
        return op;
    }

    public boolean wasIntercepted() {
        return wasIntercepted;
    }

    /** {@inheritDoc} */
    @Override public void call(@Nullable CacheDataRow oldRow) throws IgniteCheckedException {
        assert entry.isNear() || oldRow == null || oldRow.link() != 0 : oldRow;

        GridCacheContext cctx = entry.context();

        CacheObject oldVal;
        CacheObject storeLoadedVal = null;

        this.oldRow = oldRow;

        if (oldRow != null) {
            oldRow.key(entry.key());

            // unswap
            entry.update(oldRow.value(), oldRow.expireTime(), 0, oldRow.version(), false);

            if (checkRowExpired(oldRow)) {
                oldRowExpiredFlag = true;

                oldRow = null;
            }
        }

        oldVal = (oldRow != null) ? oldRow.value() : null;

        if (oldVal == null && readThrough) {
            storeLoadedVal = cctx.toCacheObject(cctx.store().load(null, entry.key));

            if (storeLoadedVal != null) {
                oldVal = cctx.kernalContext().cacheObjects().prepareForCache(storeLoadedVal, cctx);

                entry.val = oldVal;

                if (entry.deletedUnlocked())
                    entry.deletedUnlocked(false);
            }
        }

        CacheInvokeEntry<Object, Object> invokeEntry = null;
        IgniteBiTuple<Object, Exception> invokeRes = null;

        boolean invoke = op == TRANSFORM;

        boolean transformed = false;

        if (invoke) {
            invokeEntry = new CacheInvokeEntry<>(entry.key, oldVal, entry.ver, keepBinary, entry);

            invokeRes = runEntryProcessor(invokeEntry);

            op = writeObj == null ? DELETE : UPDATE;

            transformed = true;
        }

        CacheObject newVal = (CacheObject)writeObj;

        GridCacheVersionConflictContext<?, ?> conflictCtx = null;

        if (conflictResolve) {
            conflictCtx = resolveConflict(newVal, invokeRes);

            if (updateRes != null) {
                assert conflictCtx != null && conflictCtx.isUseOld() : conflictCtx;
                assert treeOp == IgniteTree.OperationType.NOOP : treeOp;

                return;
            }
        }

        if (conflictCtx == null) {
            // Perform version check only in case there was no explicit conflict resolution.
            versionCheck(invokeRes);

            if (updateRes != null) {
                assert treeOp == IgniteTree.OperationType.NOOP : treeOp;

                return;
            }
        }

        if (!F.isEmptyOrNulls(filter)) {
            boolean pass = cctx.isAllLocked(entry, filter);

            if (!pass) {
                initResultOnCancelUpdate(storeLoadedVal, !cctx.putIfAbsentFilter(filter));

                updateRes = new GridCacheUpdateAtomicResult(GridCacheUpdateAtomicResult.UpdateOutcome.FILTER_FAILED,
                    oldVal,
                    null,
                    invokeRes,
                    CU.TTL_ETERNAL,
                    CU.EXPIRE_TIME_ETERNAL,
                    null,
                    null,
                    0,
                    false);

                return;
            }
        }

        if (invoke) {
            if (!invokeEntry.modified()) {
                initResultOnCancelUpdate(storeLoadedVal, true);

                updateRes = new GridCacheUpdateAtomicResult(GridCacheUpdateAtomicResult.UpdateOutcome.INVOKE_NO_OP,
                    oldVal,
                    null,
                    invokeRes,
                    CU.TTL_ETERNAL,
                    CU.EXPIRE_TIME_ETERNAL,
                    null,
                    null,
                    0,
                    true);

                return;
            }
            else if ((invokeRes == null || invokeRes.getValue() == null) && writeObj != null) {
                try {
                    cctx.validateKeyAndValue(entry.key, (CacheObject)writeObj);
                }
                catch (Exception e) {
                    initResultOnCancelUpdate(null, true);

                    updateRes = new GridCacheUpdateAtomicResult(GridCacheUpdateAtomicResult.UpdateOutcome.INVOKE_NO_OP,
                        oldVal,
                        null,
                        new IgniteBiTuple<>(null, e),
                        CU.TTL_ETERNAL,
                        CU.EXPIRE_TIME_ETERNAL,
                        null,
                        null,
                        0,
                        false);

                    return;
                }
            }

            op = writeObj == null ? DELETE : UPDATE;
        }

        // Incorporate conflict version into new version if needed.
        if (conflictVer != null && conflictVer != newVer) {
            newVer = new GridCacheVersionEx(newVer.topologyVersion(),
                newVer.order(),
                newVer.nodeOrder(),
                newVer.dataCenterId(),
                conflictVer);
        }

        if (op == UPDATE) {
            assert writeObj != null;

            update(conflictCtx, invokeRes, storeLoadedVal != null, transformed);
        }
        else {
            assert op == DELETE && writeObj == null : op;

            remove(conflictCtx, invokeRes, storeLoadedVal != null, transformed);
        }

        assert updateRes != null && treeOp != null;
    }

    /**
     * Check row expiration and fire expire events if needed.
     *
     * @param row Old row.
     * @return {@code True} if row was expired, {@code False} otherwise.
     * @throws IgniteCheckedException if failed.
     */
    private boolean checkRowExpired(CacheDataRow row) throws IgniteCheckedException {
        assert row != null;

        if (!(row.expireTime() > 0 && row.expireTime() <= U.currentTimeMillis()))
            return false;

        GridCacheContext cctx = entry.context();

        CacheObject expiredVal = row.value();

        if (cctx.deferredDelete() && !entry.detached() && !entry.isInternal()) {
            entry.update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, entry.ver, true);

            if (!entry.deletedUnlocked())
                entry.deletedUnlocked(true);
        }
        else
            entry.markObsolete0(cctx.versions().next(), true, null);

        if (cctx.events().isRecordable(EVT_CACHE_OBJECT_EXPIRED)) {
            cctx.events().addEvent(entry.partition(),
                entry.key(),
                cctx.localNodeId(),
                null,
                EVT_CACHE_OBJECT_EXPIRED,
                null,
                false,
                expiredVal,
                expiredVal != null,
                null,
                null,
                null,
                true);
        }

        cctx.continuousQueries().onEntryExpired(entry, entry.key(), expiredVal);

        return true;
    }

    /**
     * @param storeLoadedVal Value loaded from store.
     * @param updateExpireTime {@code True} if need update expire time.
     * @throws IgniteCheckedException If failed.
     */
    private void initResultOnCancelUpdate(@Nullable CacheObject storeLoadedVal, boolean updateExpireTime)
        throws IgniteCheckedException {
        boolean needUpdate = false;

        if (storeLoadedVal != null) {
            long initTtl;
            long initExpireTime;

            if (expiryPlc != null) {
                IgniteBiTuple<Long, Long> initTtlAndExpireTime = GridCacheMapEntry.initialTtlAndExpireTime(expiryPlc);

                initTtl = initTtlAndExpireTime.get1();
                initExpireTime = initTtlAndExpireTime.get2();
            }
            else {
                initTtl = CU.TTL_ETERNAL;
                initExpireTime = CU.EXPIRE_TIME_ETERNAL;
            }

            entry.update(storeLoadedVal, initExpireTime, initTtl, entry.ver, true);

            needUpdate = true;
        }
        else if (updateExpireTime && expiryPlc != null && entry.val != null) {
            long ttl = expiryPlc.forAccess();

            if (ttl != CU.TTL_NOT_CHANGED) {
                long expireTime;

                if (ttl == CU.TTL_ZERO) {
                    ttl = CU.TTL_MINIMUM;
                    expireTime = CU.expireTimeInPast();
                }
                else
                    expireTime = CU.toExpireTime(ttl);

                if (entry.expireTimeExtras() != expireTime) {
                    entry.update(entry.val, expireTime, ttl, entry.ver, true);

                    expiryPlc.ttlUpdated(entry.key, entry.ver, null);

                    needUpdate = true;
                    storeLoadedVal = entry.val;
                }
            }
        }

        if (needUpdate) {
            newRow = entry.localPartition().dataStore().createRow(
                entry.cctx,
                entry.key,
                storeLoadedVal,
                newVer,
                entry.expireTimeExtras(),
                oldRow);

            treeOp = IgniteTree.OperationType.PUT;
        }
        else
            treeOp = IgniteTree.OperationType.NOOP;
    }

    /**
     * @param conflictCtx Conflict context.
     * @param invokeRes Entry processor result (for invoke operation).
     * @param readFromStore {@code True} if initial entry value was {@code null} and it was read from store.
     * @param transformed {@code True} if update caused by transformation operation.
     * @throws IgniteCheckedException If failed.
     */
    private void update(@Nullable GridCacheVersionConflictContext<?, ?> conflictCtx,
        @Nullable IgniteBiTuple<Object, Exception> invokeRes,
        boolean readFromStore,
        boolean transformed)
        throws IgniteCheckedException {
        GridCacheContext cctx = entry.context();

        final CacheObject oldVal = entry.val;
        CacheObject updated = (CacheObject)writeObj;

        long newSysTtl;
        long newSysExpireTime;

        long newTtl;
        long newExpireTime;

        // Conflict context is null if there were no explicit conflict resolution.
        if (conflictCtx == null) {
            // Calculate TTL and expire time for local update.
            if (explicitTtl != CU.TTL_NOT_CHANGED) {
                // If conflict existed, expire time must be explicit.
                assert conflictVer == null || explicitExpireTime != CU.EXPIRE_TIME_CALCULATE;

                newSysTtl = newTtl = explicitTtl;
                newSysExpireTime = explicitExpireTime;

                newExpireTime = explicitExpireTime != CU.EXPIRE_TIME_CALCULATE ?
                    explicitExpireTime : CU.toExpireTime(explicitTtl);
            }
            else {
                newSysTtl = expiryPlc == null ? CU.TTL_NOT_CHANGED :
                    entry.val != null ? expiryPlc.forUpdate() : expiryPlc.forCreate();

                if (newSysTtl == CU.TTL_NOT_CHANGED) {
                    newSysExpireTime = CU.EXPIRE_TIME_CALCULATE;
                    newTtl = entry.ttlExtras();
                    newExpireTime = entry.expireTimeExtras();
                }
                else if (newSysTtl == CU.TTL_ZERO) {
                    op = DELETE;

                    writeObj = null;

                    remove(conflictCtx, invokeRes, readFromStore, false);

                    return;
                }
                else {
                    newSysExpireTime = CU.EXPIRE_TIME_CALCULATE;
                    newTtl = newSysTtl;
                    newExpireTime = CU.toExpireTime(newTtl);
                }
            }
        }
        else {
            newSysTtl = newTtl = conflictCtx.ttl();
            newSysExpireTime = newExpireTime = conflictCtx.expireTime();
        }

        if (intercept && (conflictVer == null || !skipInterceptorOnConflict)) {
            Object updated0 = cctx.unwrapBinaryIfNeeded(updated, keepBinary, false);

            CacheLazyEntry<Object, Object> interceptEntry =
                new CacheLazyEntry<>(cctx, entry.key, null, oldVal, null, keepBinary);

            Object interceptorVal = cctx.config().getInterceptor().onBeforePut(interceptEntry, updated0);

            wasIntercepted = true;

            if (interceptorVal == null) {
                treeOp = IgniteTree.OperationType.NOOP;

                updateRes = new GridCacheUpdateAtomicResult(GridCacheUpdateAtomicResult.UpdateOutcome.INTERCEPTOR_CANCEL,
                    oldVal,
                    null,
                    invokeRes,
                    CU.TTL_ETERNAL,
                    CU.EXPIRE_TIME_ETERNAL,
                    null,
                    null,
                    0,
                    false);

                return;
            }
            else if (interceptorVal != updated0) {
                updated0 = cctx.unwrapTemporary(interceptorVal);

                updated = cctx.toCacheObject(updated0);
            }
        }

        updated = cctx.kernalContext().cacheObjects().prepareForCache(updated, cctx);

        if (writeThrough)
            // Must persist inside synchronization in non-tx mode.
            cctx.store().put(null, entry.key, updated, newVer);

        if (entry.val == null) {
            boolean new0 = entry.isStartVersion();

            assert entry.deletedUnlocked() || new0 || entry.isInternal() : "Invalid entry [entry=" + entry +
                ", locNodeId=" + cctx.localNodeId() + ']';

            if (!new0 && !entry.isInternal())
                entry.deletedUnlocked(false);
        }
        else {
            assert !entry.deletedUnlocked() : "Invalid entry [entry=" + this +
                ", locNodeId=" + cctx.localNodeId() + ']';
        }

        long updateCntr0 = entry.nextPartitionCounter(topVer, primary, false, updateCntr);

        entry.logUpdate(op, updated, newVer, newExpireTime, updateCntr0);

        if (!entry.isNear()) {
            newRow = entry.localPartition().dataStore().createRow(
                entry.cctx,
                entry.key,
                updated,
                newVer,
                newExpireTime,
                oldRow);

            treeOp = oldRow != null && oldRow.link() == newRow.link() ?
                IgniteTree.OperationType.NOOP : IgniteTree.OperationType.PUT;
        }
        else
            treeOp = IgniteTree.OperationType.PUT;

        entry.update(updated, newExpireTime, newTtl, newVer, true);

        if (entry.isNear()) {
            boolean updatedDht = ((GridNearCacheEntry)entry).recordDhtVersion(newVer);
            assert updatedDht : this;
        }

        updateRes = new GridCacheUpdateAtomicResult(GridCacheUpdateAtomicResult.UpdateOutcome.SUCCESS,
            oldVal,
            updated,
            invokeRes,
            newSysTtl,
            newSysExpireTime,
            null,
            conflictCtx,
            updateCntr0,
            transformed);
    }

    /**
     * @param conflictCtx Conflict context.
     * @param invokeRes Entry processor result (for invoke operation).
     * @param readFromStore {@code True} if initial entry value was {@code null} and it was read from store.
     * @param transformed {@code True} if remove caused by tranformation operation.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void remove(@Nullable GridCacheVersionConflictContext<?, ?> conflictCtx,
        @Nullable IgniteBiTuple<Object, Exception> invokeRes,
        boolean readFromStore,
        boolean transformed)
        throws IgniteCheckedException {
        GridCacheContext cctx = entry.context();

        CacheObject oldVal = entry.val;

        IgniteBiTuple<Boolean, Object> interceptRes = null;

        if (intercept && (conflictVer == null || !skipInterceptorOnConflict)) {
            CacheLazyEntry<Object, Object> intercepEntry =
                new CacheLazyEntry<>(cctx, entry.key, null, oldVal, null, keepBinary);

            interceptRes = cctx.config().getInterceptor().onBeforeRemove(intercepEntry);

            wasIntercepted = true;

            if (cctx.cancelRemove(interceptRes)) {
                treeOp = IgniteTree.OperationType.NOOP;

                updateRes = new GridCacheUpdateAtomicResult(GridCacheUpdateAtomicResult.UpdateOutcome.INTERCEPTOR_CANCEL,
                    cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2())),
                    null,
                    invokeRes,
                    CU.TTL_ETERNAL,
                    CU.EXPIRE_TIME_ETERNAL,
                    null,
                    null,
                    0,
                    false);

                return;
            }
        }

        if (writeThrough)
            // Must persist inside synchronization in non-tx mode.
            cctx.store().remove(null, entry.key);

        long updateCntr0 = entry.nextPartitionCounter(topVer, primary, false, updateCntr);

        entry.logUpdate(op, null, newVer, 0, updateCntr0);

        if (oldVal != null) {
            assert !entry.deletedUnlocked();

            if (!entry.isInternal())
                entry.deletedUnlocked(true);
        }
        else {
            boolean new0 = entry.isStartVersion();

            assert entry.deletedUnlocked() || new0 || entry.isInternal() : "Invalid entry [entry=" + this +
                ", locNodeId=" + cctx.localNodeId() + ']';

            if (new0) {
                if (!entry.isInternal())
                    entry.deletedUnlocked(true);
            }
        }

        GridCacheVersion enqueueVer = newVer;

        entry.update(null, CU.TTL_ETERNAL, CU.EXPIRE_TIME_ETERNAL, newVer, true);

        treeOp = (oldRow == null || readFromStore) ? IgniteTree.OperationType.NOOP :
            IgniteTree.OperationType.REMOVE;

        GridCacheUpdateAtomicResult.UpdateOutcome outcome = oldVal != null ? GridCacheUpdateAtomicResult.UpdateOutcome.SUCCESS : GridCacheUpdateAtomicResult.UpdateOutcome.REMOVE_NO_VAL;

        if (interceptRes != null)
            oldVal = cctx.toCacheObject(cctx.unwrapTemporary(interceptRes.get2()));

        updateRes = new GridCacheUpdateAtomicResult(outcome,
            oldVal,
            null,
            invokeRes,
            CU.TTL_NOT_CHANGED,
            CU.EXPIRE_TIME_CALCULATE,
            enqueueVer,
            conflictCtx,
            updateCntr0,
            transformed);
    }

    /**
     * @param newVal New entry value.
     * @param invokeRes Entry processor result (for invoke operation).
     * @return Conflict context.
     * @throws IgniteCheckedException If failed.
     */
    private GridCacheVersionConflictContext<?, ?> resolveConflict(
        CacheObject newVal,
        @Nullable IgniteBiTuple<Object, Exception> invokeRes)
        throws IgniteCheckedException {
        GridCacheContext cctx = entry.context();

        // Cache is conflict-enabled.
        if (cctx.conflictNeedResolve()) {
            GridCacheVersion oldConflictVer = entry.ver.conflictVersion();

            // Prepare old and new entries for conflict resolution.
            GridCacheVersionedEntryEx oldEntry = new GridCacheLazyPlainVersionedEntry<>(cctx,
                entry.key,
                entry.val,
                entry.ttlExtras(),
                entry.expireTimeExtras(),
                entry.ver.conflictVersion(),
                entry.isStartVersion(),
                keepBinary);

            GridTuple3<Long, Long, Boolean> expiration = entry.ttlAndExpireTime(expiryPlc,
                explicitTtl,
                explicitExpireTime);

            GridCacheVersionedEntryEx newEntry = new GridCacheLazyPlainVersionedEntry<>(
                cctx,
                entry.key,
                newVal,
                expiration.get1(),
                expiration.get2(),
                conflictVer != null ? conflictVer : newVer,
                keepBinary);

            // Resolve conflict.
            GridCacheVersionConflictContext<?, ?> conflictCtx = cctx.conflictResolve(oldEntry, newEntry, verCheck);

            assert conflictCtx != null;

            // Use old value?
            if (conflictCtx.isUseOld()) {
                GridCacheVersion newConflictVer = conflictVer != null ? conflictVer : newVer;

                // Handle special case with atomic comparator.
                if (!entry.isStartVersion() &&                                                        // Not initial value,
                    verCheck &&                                                                       // and atomic version check,
                    oldConflictVer.dataCenterId() == newConflictVer.dataCenterId() &&                 // and data centers are equal,
                    GridCacheMapEntry.ATOMIC_VER_COMPARATOR.compare(oldConflictVer, newConflictVer) == 0 && // and both versions are equal,
                    cctx.writeThrough() &&                                                            // and store is enabled,
                    primary)                                                                          // and we are primary.
                {
                    CacheObject val = entry.val;

                    if (val == null) {
                        assert entry.deletedUnlocked();

                        cctx.store().remove(null, entry.key);
                    }
                    else
                        cctx.store().put(null, entry.key, val, entry.ver);
                }

                treeOp = IgniteTree.OperationType.NOOP;

                updateRes = new GridCacheUpdateAtomicResult(GridCacheUpdateAtomicResult.UpdateOutcome.CONFLICT_USE_OLD,
                    entry.val,
                    null,
                    invokeRes,
                    CU.TTL_ETERNAL,
                    CU.EXPIRE_TIME_ETERNAL,
                    null,
                    null,
                    0,
                    false);
            }
            // Will update something.
            else {
                // Merge is a local update which override passed value bytes.
                if (conflictCtx.isMerge()) {
                    writeObj = cctx.toCacheObject(conflictCtx.mergeValue());

                    conflictVer = null;
                }
                else
                    assert conflictCtx.isUseNew();

                // Update value is known at this point, so update operation type.
                op = writeObj != null ? UPDATE : DELETE;
            }

            return conflictCtx;
        }
        else
            // Nullify conflict version on this update, so that we will use regular version during next updates.
            conflictVer = null;

        return null;
    }

    /**
     * @param invokeRes Entry processor result (for invoke operation).
     * @throws IgniteCheckedException If failed.
     */
    private void versionCheck(@Nullable IgniteBiTuple<Object, Exception> invokeRes) throws IgniteCheckedException {
        GridCacheContext cctx = entry.context();

        if (verCheck) {
            if (!entry.isStartVersion() && GridCacheMapEntry.ATOMIC_VER_COMPARATOR.compare(entry.ver, newVer) >= 0) {
                if (GridCacheMapEntry.ATOMIC_VER_COMPARATOR.compare(entry.ver, newVer) == 0 && cctx.writeThrough() && primary) {
                    if (GridCacheMapEntry.log.isDebugEnabled())
                        GridCacheMapEntry.log.debug("Received entry update with same version as current (will update store) " +
                            "[entry=" + this + ", newVer=" + newVer + ']');

                    CacheObject val = entry.val;

                    if (val == null) {
                        assert entry.deletedUnlocked();

                        cctx.store().remove(null, entry.key);
                    }
                    else
                        cctx.store().put(null, entry.key, val, entry.ver);
                }
                else {
                    if (GridCacheMapEntry.log.isDebugEnabled())
                        GridCacheMapEntry.log.debug("Received entry update with smaller version than current (will ignore) " +
                            "[entry=" + this + ", newVer=" + newVer + ']');
                }

                treeOp = IgniteTree.OperationType.NOOP;

                updateRes = new GridCacheUpdateAtomicResult(GridCacheUpdateAtomicResult.UpdateOutcome.VERSION_CHECK_FAILED,
                    entry.val,
                    null,
                    invokeRes,
                    CU.TTL_ETERNAL,
                    CU.EXPIRE_TIME_ETERNAL,
                    null,
                    null,
                    0,
                    false);
            }
        }
        else
            assert entry.isStartVersion() || GridCacheMapEntry.ATOMIC_VER_COMPARATOR.compare(entry.ver, newVer) <= 0 :
                "Invalid version for inner update [isNew=" + entry.isStartVersion() + ", entry=" + entry + ", newVer=" + newVer + ']';
    }

    /**
     * @param invokeEntry Entry for {@link EntryProcessor}.
     * @return Entry processor return value.
     */
    private IgniteBiTuple<Object, Exception> runEntryProcessor(CacheInvokeEntry<Object, Object> invokeEntry) {
        EntryProcessor<Object, Object, ?> entryProcessor = (EntryProcessor<Object, Object, ?>)writeObj;

        IgniteThread.onEntryProcessorEntered(true);

        try {
            Object computed = entryProcessor.process(invokeEntry, invokeArgs);

            if (invokeEntry.modified()) {
                GridCacheContext cctx = entry.context();

                writeObj = cctx.toCacheObject(cctx.unwrapTemporary(invokeEntry.getValue()));
            }
            else
                writeObj = invokeEntry.valObj;

            if (computed != null)
                return new IgniteBiTuple<>(entry.cctx.unwrapTemporary(computed), null);

            return null;
        }
        catch (UnregisteredClassException | UnregisteredBinaryTypeException e) {
            throw e;
        }
        catch (Exception e) {
            writeObj = invokeEntry.valObj;

            return new IgniteBiTuple<>(null, e);
        }
        finally {
            IgniteThread.onEntryProcessorLeft();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AtomicCacheUpdateClosure.class, this);
    }
}
