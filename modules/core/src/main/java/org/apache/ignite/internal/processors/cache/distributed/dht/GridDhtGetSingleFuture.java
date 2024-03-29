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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.ReaderArguments;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_DHT_SINGLE_GET_FUTURE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_GET_MAP;

/**
 *
 */
public final class GridDhtGetSingleFuture<K, V> extends GridFutureAdapter<GridCacheEntryInfo>
    implements GridDhtFuture<GridCacheEntryInfo> {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Message ID. */
    private long msgId;

    /** */
    private UUID reader;

    /** Read through flag. */
    private boolean readThrough;

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /** Key. */
    private KeyCacheObject key;

    /** */
    private final boolean addRdr;

    /** Reserved partitions. */
    private int part = -1;

    /** Future ID. */
    private IgniteUuid futId;

    /** Version. */
    private GridCacheVersion ver;

    /** Topology version .*/
    private AffinityTopologyVersion topVer;

    /** Retry because ownership changed. */
    private Integer retry;

    /** Subject ID. */
    private UUID subjId;

    /** Task name. */
    private int taskNameHash;

    /** Expiry policy. */
    private IgniteCacheExpiryPolicy expiryPlc;

    /** Skip values flag. */
    private boolean skipVals;

    /** Recovery context flag. */
    private final boolean recovery;

    /** Transaction label. */
    private final String txLbl;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** Indicates that operation requires just update the time to live value. */
    private final boolean touchTtl;

    /**
     * @param cctx Context.
     * @param msgId Message ID.
     * @param reader Reader.
     * @param key Key.
     * @param addRdr Add reader flag.
     * @param readThrough Read through flag.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param txLbl Transaction label.
     * @param mvccSnapshot Mvcc snapshot.
     * @param touchTtl If {@code true} then ttl only chnages are required.
     */
    public GridDhtGetSingleFuture(
        GridCacheContext<K, V> cctx,
        long msgId,
        UUID reader,
        KeyCacheObject key,
        boolean addRdr,
        boolean readThrough,
        @NotNull AffinityTopologyVersion topVer,
        @Nullable UUID subjId,
        int taskNameHash,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean recovery,
        @Nullable String txLbl,
        @Nullable MvccSnapshot mvccSnapshot,
        boolean touchTtl
    ) {
        assert reader != null;
        assert key != null;

        this.reader = reader;
        this.cctx = cctx;
        this.msgId = msgId;
        this.key = key;
        this.addRdr = addRdr;
        this.readThrough = readThrough;
        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.expiryPlc = expiryPlc;
        this.skipVals = skipVals;
        this.recovery = recovery;
        this.txLbl = txLbl;
        this.mvccSnapshot = mvccSnapshot;
        this.touchTtl = touchTtl;

        futId = IgniteUuid.randomUuid();

        ver = cctx.cache().nextVersion();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtGetSingleFuture.class);
    }

    /**
     * Initializes future.
     */
    void init() {
        try (TraceSurroundings ignored =
                 MTC.supportContinual(span = cctx.kernalContext().tracing().
                     create(CACHE_API_DHT_SINGLE_GET_FUTURE, MTC.span()))) {
            map();
        }
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Future version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(GridCacheEntryInfo res, Throwable err) {
        if (super.onDone(res, err)) {
            // Release all partitions reserved by this future.
            if (part != -1)
                cctx.topology().releasePartitions(part);

            return true;
        }

        return false;
    }

    /**
     *
     */
    private void map() {
        try (TraceSurroundings ignored =
                 MTC.support(cctx.kernalContext().tracing().create(CACHE_API_GET_MAP, span))) {
            MTC.span().addTag("topology.version", () -> Objects.toString(topVer));

            // TODO Get rid of force keys request https://issues.apache.org/jira/browse/IGNITE-10251.
            if (cctx.group().preloader().needForceKeys()) {
                assert !cctx.mvccEnabled();

                GridDhtFuture<Object> fut = cctx.group().preloader().request(
                    cctx,
                    Collections.singleton(key),
                    topVer);

                if (fut != null) {
                    if (!F.isEmpty(fut.invalidPartitions())) {
                        assert fut.invalidPartitions().size() == 1 : fut.invalidPartitions();

                        retry = F.first(fut.invalidPartitions());

                        onDone((GridCacheEntryInfo)null);

                        return;
                    }

                    fut.listen(
                        new IgniteInClosure<IgniteInternalFuture<Object>>() {
                            @Override public void apply(IgniteInternalFuture<Object> fut) {
                                Throwable e = fut.error();

                                if (e != null) { // Check error first.
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to request keys from preloader " +
                                            "[keys=" + key + ", err=" + e + ']');

                                    if (e instanceof NodeStoppingException)
                                        return;

                                    onDone(e);
                                }
                                else
                                    map0(true);
                            }
                        }
                    );

                    return;
                }
            }

            map0(false);
        }
    }

    /**
     *
     */
    private void map0(boolean forceKeys) {
        assert retry == null : retry;

        if (!map(key, forceKeys)) {
            retry = cctx.affinity().partition(key);

            if (!isDone())
                onDone((GridCacheEntryInfo)null);

            return;
        }

        getAsync();
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> invalidPartitions() {
        return retry == null ? Collections.<Integer>emptyList() : Collections.singletonList(retry);
    }

    /**
     * @param key Key.
     * @return {@code True} if mapped.
     */
    private boolean map(KeyCacheObject key, boolean forceKeys) {
        try {
            int keyPart = cctx.affinity().partition(key);

            if (cctx.mvccEnabled()) {
                boolean noOwners = cctx.topology().owners(keyPart, topVer).isEmpty();

                // Force key request is disabled for MVCC. So if there are no partition owners for the given key
                // (we have a not strict partition loss policy if we've got here) we need to set flag forceKeys to true
                // to avoid useless remapping to other non-owning partitions. For non-mvcc caches the force key request
                // is also useless in the such situations, so the same flow is here: allegedly we've made a force key
                // request with no results and therefore forceKeys flag may be set to true here.
                if (noOwners)
                    forceKeys = true;
            }

            GridDhtLocalPartition part = topVer.topologyVersion() > 0 ?
                cache().topology().localPartition(keyPart, topVer, true) :
                cache().topology().localPartition(keyPart);

            if (part == null)
                return false;

            assert this.part == -1;

            // By reserving, we make sure that partition won't be unloaded while processed.
            if (part.reserve()) {
                if (forceKeys || (part.state() == OWNING || part.state() == LOST)) {
                    this.part = part.id();

                    return true;
                }
                else {
                    part.release();

                    return false;
                }
            }
            else
                return false;
        }
        catch (GridDhtInvalidPartitionException ex) {
            return false;
        }
    }

    /**
     *
     */
    private void getAsync() {
        assert part != -1;

        String taskName0 = cctx.kernalContext().job().currentTaskName();

        if (taskName0 == null)
            taskName0 = cctx.kernalContext().task().resolveTaskName(taskNameHash);

        final String taskName = taskName0;

        IgniteInternalFuture<Boolean> rdrFut = null;

        ReaderArguments readerArgs = null;

        if (addRdr && !skipVals && !cctx.localNodeId().equals(reader)) {
            while (true) {
                GridDhtCacheEntry e = cache().entryExx(key, topVer);

                try {
                    if (e.obsolete())
                        continue;

                    boolean addReader = !e.deleted();

                    if (addReader) {
                        e.unswap(false);

                        // Entry will be removed on touch() if no data in cache,
                        // but they could be loaded from store,
                        // we have to add reader again later.
                        if (readerArgs == null)
                            readerArgs = new ReaderArguments(reader, msgId, topVer);
                    }

                    // Register reader. If there are active transactions for this entry,
                    // then will wait for their completion before proceeding.
                    // TODO: IGNITE-3498:
                    // TODO: What if any transaction we wait for actually removes this entry?
                    // TODO: In this case seems like we will be stuck with untracked near entry.
                    // TODO: To fix, check that reader is contained in the list of readers once
                    // TODO: again after the returned future completes - if not, try again.
                    rdrFut = addReader ? e.addReader(reader, msgId, topVer) : null;

                    break;
                }
                catch (IgniteCheckedException err) {
                    onDone(err);

                    return;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry when getting a DHT value: " + e);
                }
                finally {
                    e.touch();
                }
            }
        }

        IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> fut;

        if (rdrFut == null || rdrFut.isDone()) {
            fut = cache().getDhtAllAsync(
                Collections.singleton(key),
                readerArgs,
                readThrough,
                subjId,
                taskName,
                expiryPlc,
                skipVals,
                recovery,
                txLbl,
                mvccSnapshot,
                touchTtl);
        }
        else {
            final ReaderArguments args = readerArgs;

            rdrFut.listen(
                new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
                    @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                        Throwable e = fut.error();

                        if (e != null) {
                            onDone(e);

                            return;
                        }

                        IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> fut0 =
                            cache().getDhtAllAsync(
                                Collections.singleton(key),
                                args,
                                readThrough,
                                subjId,
                                taskName,
                                expiryPlc,
                                skipVals,
                                recovery,
                                null,
                                mvccSnapshot,
                                touchTtl);

                        fut0.listen(createGetFutureListener());
                    }
                }
            );

            return;
        }

        if (fut.isDone())
            onResult(fut);
        else
            fut.listen(createGetFutureListener());
    }

    /**
     * @return Listener for get future.
     */
    @NotNull private IgniteInClosure<IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>>>
    createGetFutureListener() {
        return new IgniteInClosure<IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>>>() {
            @Override public void apply(
                IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> fut
            ) {
                onResult(fut);
            }
        };
    }

    /**
     * @param fut Completed future to finish this process with.
     */
    private void onResult(IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> fut) {
        assert fut.isDone();

        if (fut.error() != null)
            onDone(fut.error());
        else {
            try {
                onDone(toEntryInfo(fut.get()));
            }
            catch (IgniteCheckedException ignored) {
                assert false; // Should never happen.
            }
        }
    }

    /**
     * @param map Map to convert.
     * @return List of infos.
     */
    private GridCacheEntryInfo toEntryInfo(Map<KeyCacheObject, EntryGetResult> map) {
        if (map.isEmpty())
            return null;

        EntryGetResult val = map.get(key);

        assert val != null;

        GridCacheEntryInfo info = new GridCacheEntryInfo();

        info.cacheId(cctx.cacheId());
        info.key(key);
        info.value(skipVals ? null : (CacheObject)val.value());
        info.version(val.version());
        info.expireTime(val.expireTime());
        info.ttl(val.ttl());

        return info;
    }

    /**
     * @return DHT cache.
     */
    private GridDhtCacheAdapter<K, V> cache() {
        return (GridDhtCacheAdapter<K, V>)cctx.cache();
    }
}
