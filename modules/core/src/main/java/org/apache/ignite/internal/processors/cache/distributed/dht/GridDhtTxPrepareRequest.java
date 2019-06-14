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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * DHT prepare request.
 */
public class GridDhtTxPrepareRequest extends GridDistributedTxPrepareRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** Max order. */
    private UUID nearNodeId;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private int miniId;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** Invalidate near entries flags. */
    private BitSet invalidateNearEntries;

    /** Near writes. */
    @GridToStringInclude
    @GridDirectCollection(IgniteTxEntry.class)
    private Collection<IgniteTxEntry> nearWrites;

    /** */
    @GridDirectCollection(PartitionUpdateCountersMessage.class)
    private Collection<PartitionUpdateCountersMessage> updCntrs;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Subject ID. */
    private UUID subjId;

    /** Task name hash. */
    private int taskNameHash;

    /** Preload keys. */
    private BitSet preloadKeys;

    /** */
    @GridDirectTransient
    private List<IgniteTxKey> nearMissed;

    /** */
    private MvccSnapshot mvccSnapshot;

    /** {@code True} if remote tx should skip adding itself to completed versions map on finish. */
    private boolean skipCompletedVers;

    /** Transaction label. */
    @GridToStringInclude
    @Nullable private String txLbl;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtTxPrepareRequest() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param topVer Topology version.
     * @param tx Transaction.
     * @param timeout Transaction timeout.
     * @param dhtWrites DHT writes.
     * @param nearWrites Near writes.
     * @param txNodes Transaction nodes mapping.
     * @param nearXidVer Near transaction ID.
     * @param last {@code True} if this is last prepare request for node.
     * @param addDepInfo Deployment info flag.
     * @param storeWriteThrough Cache store write through flag.
     * @param retVal Need return value flag
     * @param mvccSnapshot Mvcc snapshot.
     * @param updCntrs Update counters for mvcc Tx.
     */
    public GridDhtTxPrepareRequest(
        IgniteUuid futId,
        int miniId,
        AffinityTopologyVersion topVer,
        GridDhtTxLocalAdapter tx,
        long timeout,
        Collection<IgniteTxEntry> dhtWrites,
        Collection<IgniteTxEntry> nearWrites,
        Map<UUID, Collection<UUID>> txNodes,
        GridCacheVersion nearXidVer,
        boolean last,
        boolean onePhaseCommit,
        UUID subjId,
        int taskNameHash,
        boolean addDepInfo,
        boolean storeWriteThrough,
        boolean retVal,
        MvccSnapshot mvccSnapshot,
        Collection<PartitionUpdateCountersMessage> updCntrs) {
        super(tx,
            timeout,
            null,
            dhtWrites,
            txNodes,
            retVal,
            last,
            onePhaseCommit,
            addDepInfo);

        assert futId != null;
        assert miniId != 0;

        this.topVer = topVer;
        this.futId = futId;
        this.nearWrites = nearWrites;
        this.miniId = miniId;
        this.nearXidVer = nearXidVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.mvccSnapshot = mvccSnapshot;
        this.updCntrs = updCntrs;

        storeWriteThrough(storeWriteThrough);
        needReturnValue(retVal);

        invalidateNearEntries = new BitSet(dhtWrites == null ? 0 : dhtWrites.size());

        nearNodeId = tx.nearNodeId();

        skipCompletedVers = tx.xidVersion() == tx.nearXidVersion();

        txLbl = tx.label();
    }

    /**
     * @return Mvcc info.
     */
    public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @return Update counters list.
     */
    public Collection<PartitionUpdateCountersMessage> updateCounters() {
        return updCntrs;
    }

    /**
     * @return Near cache writes for which cache was not found (possible if client near cache was closed).
     */
    @Nullable public List<IgniteTxKey> nearMissed() {
        return nearMissed;
    }

    /**
     * @return Near transaction ID.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /**
     * @return Subject ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Near writes.
     */
    public Collection<IgniteTxEntry> nearWrites() {
        return nearWrites == null ? Collections.<IgniteTxEntry>emptyList() : nearWrites;
    }

    /**
     * @param idx Entry index to set invalidation flag.
     * @param invalidate Invalidation flag value.
     */
    void invalidateNearEntry(int idx, boolean invalidate) {
        invalidateNearEntries.set(idx, invalidate);
    }

    /**
     * @param idx Index to get invalidation flag value.
     * @return Invalidation flag value.
     */
    public boolean invalidateNearEntry(int idx) {
        return invalidateNearEntries.get(idx);
    }

    /**
     * Marks last added key for preloading.
     *
     * @param idx Key index.
     */
    void markKeyForPreload(int idx) {
        if (preloadKeys == null)
            preloadKeys = new BitSet();

        preloadKeys.set(idx, true);
    }

    /**
     * Checks whether entry info should be sent to primary node from backup.
     *
     * @param idx Index.
     * @return {@code True} if value should be sent, {@code false} otherwise.
     */
    public boolean needPreloadKey(int idx) {
        return preloadKeys != null && preloadKeys.get(idx);
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return {@code True} if remote tx should skip adding itself to completed versions map on finish.
     */
    public boolean skipCompletedVersion() {
        return skipCompletedVers;
    }

    /**
     * @return Transaction label.
     */
    @Nullable public String txLabel() {
        return txLbl;
    }

    /**
     * {@inheritDoc}
     *
     * @param ctx
     */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (nearWrites != null)
            marshalTx(nearWrites, ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (nearWrites != null) {
            for (Iterator<IgniteTxEntry> it = nearWrites.iterator(); it.hasNext();) {
                IgniteTxEntry e = it.next();

                GridCacheContext<?, ?> cacheCtx = ctx.cacheContext(e.cacheId());

                if (cacheCtx == null) {
                    it.remove();

                    if (nearMissed == null)
                        nearMissed = new ArrayList<>();

                    nearMissed.add(e.txKey());
                }
                else {
                    e.context(cacheCtx);

                    e.unmarshal(ctx, true, ldr);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 18:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeBitSet("invalidateNearEntries", invalidateNearEntries))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeInt("miniId", miniId))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeMessage("mvccSnapshot", mvccSnapshot))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeUuid("nearNodeId", nearNodeId))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeCollection("nearWrites", nearWrites, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeMessage("nearXidVer", nearXidVer))
                    return false;

                writer.incrementState();

            case 25:
                if (!writer.writeBitSet("preloadKeys", preloadKeys))
                    return false;

                writer.incrementState();

            case 26:
                if (!writer.writeBoolean("skipCompletedVers", skipCompletedVers))
                    return false;

                writer.incrementState();

            case 27:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 28:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 29:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 30:
                if (!writer.writeString("txLbl", txLbl))
                    return false;

                writer.incrementState();

            case 31:
                if (!writer.writeCollection("updCntrs", updCntrs, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 18:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                invalidateNearEntries = reader.readBitSet("invalidateNearEntries");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                miniId = reader.readInt("miniId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                mvccSnapshot = reader.readMessage("mvccSnapshot");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                nearNodeId = reader.readUuid("nearNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                nearWrites = reader.readCollection("nearWrites", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                nearXidVer = reader.readMessage("nearXidVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 25:
                preloadKeys = reader.readBitSet("preloadKeys");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 26:
                skipCompletedVers = reader.readBoolean("skipCompletedVers");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 27:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 28:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 29:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 30:
                txLbl = reader.readString("txLbl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 31:
                updCntrs = reader.readCollection("updCntrs", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxPrepareRequest.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 34;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 32;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return U.safeAbs(version().hashCode());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxPrepareRequest.class, this, "super", super.toString());
    }
}
