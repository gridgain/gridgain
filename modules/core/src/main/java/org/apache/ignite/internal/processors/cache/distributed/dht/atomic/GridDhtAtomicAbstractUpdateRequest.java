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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridDhtAtomicAbstractUpdateRequest extends GridCacheIdMessage implements GridCacheDeployable {
    /** Skip store flag bit mask. */
    protected static final int DHT_ATOMIC_SKIP_STORE_FLAG_MASK = 0x01;

    /** Keep binary flag. */
    protected static final int DHT_ATOMIC_KEEP_BINARY_FLAG_MASK = 0x02;

    /** Near cache key flag. */
    protected static final int DHT_ATOMIC_NEAR_FLAG_MASK = 0x04;

    /** */
    static final int DHT_ATOMIC_HAS_RESULT_MASK = 0x08;

    /** */
    private static final int DHT_ATOMIC_REPLY_WITHOUT_DELAY = 0x10;

    /** */
    protected static final int DHT_ATOMIC_OBSOLETE_NEAR_KEY_FLAG_MASK = 0x20;

    /** Flag indicating transformation operation was performed. */
    protected static final int DHT_ATOMIC_TRANSFORM_OP_FLAG_MASK = 0x40;

    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Future ID on primary. */
    protected long futId;

    /** Write version. */
    protected GridCacheVersion writeVer;

    /** Write synchronization mode. */
    protected CacheWriteSynchronizationMode syncMode;

    /** Topology version. */
    protected AffinityTopologyVersion topVer;

    /** Subject ID. */
    protected UUID subjId;

    /** Task name hash. */
    protected int taskNameHash;

    /** Node ID. */
    @GridDirectTransient
    protected UUID nodeId;

    /** On response flag. Access should be synced on future. */
    @GridDirectTransient
    private boolean onRes;

    /** */
    private UUID nearNodeId;

    /** */
    private long nearFutId;

    /** Additional flags. */
    protected byte flags;

    /** */
    protected UpdateErrors errs;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    protected GridDhtAtomicAbstractUpdateRequest() {
        // N-op.
    }

    /**
     * Constructor.
     *
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     */
    protected GridDhtAtomicAbstractUpdateRequest(int cacheId,
        UUID nodeId,
        long futId,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        UUID subjId,
        int taskNameHash,
        boolean addDepInfo,
        boolean keepBinary,
        boolean skipStore
    ) {
        assert topVer.topologyVersion() > 0 : topVer;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.futId = futId;
        this.writeVer = writeVer;
        this.syncMode = syncMode;
        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.addDepInfo = addDepInfo;

        if (skipStore)
            setFlag(true, DHT_ATOMIC_SKIP_STORE_FLAG_MASK);
        if (keepBinary)
            setFlag(true, DHT_ATOMIC_KEEP_BINARY_FLAG_MASK);
    }

    /** {@inheritDoc} */
    @Override public final AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param nearNodeId Near node ID.
     * @param nearFutId Future ID on near node.
     */
    void nearReplyInfo(UUID nearNodeId, long nearFutId) {
        assert nearNodeId != null;

        this.nearNodeId = nearNodeId;
        this.nearFutId = nearFutId;
    }

    /**
     * @return {@code True} if backups should reply immediately.
     */
    boolean replyWithoutDelay() {
        return isFlag(DHT_ATOMIC_REPLY_WITHOUT_DELAY);
    }

    /**
     * @param replyWithoutDelay {@code True} if backups should reply immediately.
     */
    void replyWithoutDelay(boolean replyWithoutDelay) {
        setFlag(replyWithoutDelay, DHT_ATOMIC_REPLY_WITHOUT_DELAY);
    }

    /**
     * @param res Result flag.
     */
    void hasResult(boolean res) {
        setFlag(res, DHT_ATOMIC_HAS_RESULT_MASK);
    }

    /**
     * @return Result flag.
     */
    private boolean hasResult() {
        return isFlag(DHT_ATOMIC_HAS_RESULT_MASK);
    }

    /**
     * @return Near node ID.
     */
    public UUID nearNodeId() {
        return nearNodeId;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Flags.
     */
    public final byte flags() {
        return flags;
    }

    /**
     * @return Keep binary flag.
     */
    public final boolean keepBinary() {
        return isFlag(DHT_ATOMIC_KEEP_BINARY_FLAG_MASK);
    }

    /**
     * @return Skip write-through to a persistent storage.
     */
    public final boolean skipStore() {
        return isFlag(DHT_ATOMIC_SKIP_STORE_FLAG_MASK);
    }

    /**
     * @return {@code True} if transformation operation was performed.
     */
    public final boolean transformOperation() {
        return isFlag(DHT_ATOMIC_TRANSFORM_OP_FLAG_MASK);
    }

    /**
     * @return {@code True} if on response flag changed.
     */
    public boolean onResponse() {
        return !onRes && (onRes = true);
    }

    /**
     * @return {@code True} if response was received.
     */
    boolean hasResponse() {
        return onRes;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Collection of failed keys.
     */
    public Collection<KeyCacheObject> failedKeys() {
        return errs != null ? errs.failedKeys() : null;
    }

    /**
     * Adds keys to collection of failed keys.
     *
     * @param keys Keys to add.
     * @param e Error cause.
     */
    public void addFailedKeys(Collection<KeyCacheObject> keys, Throwable e) {
        if (errs == null)
            errs = new UpdateErrors();

        errs.addFailedKeys(keys, e);
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException error() {
        return errs != null ? errs.error() : null;
    }

    /**
     * @return Force transform backups flag.
     */
    public abstract boolean forceTransformBackups();

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext ctx) {
        return ctx.atomicMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        cleanup();
    }

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL (optional).
     * @param conflictExpireTime Conflict expire time (optional).
     * @param conflictVer Conflict version (optional).
     * @param addPrevVal If {@code true} adds previous value.
     * @param prevVal Previous value.
     * @param updateCntr Update counter.
     * @param cacheOp Corresponding cache operation.
     */
    public abstract void addWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer,
        boolean addPrevVal,
        @Nullable CacheObject prevVal,
        long updateCntr,
        GridCacheOperation cacheOp);

    /**
     * @param key Key to add.
     * @param val Value, {@code null} if should be removed.
     * @param entryProcessor Entry processor.
     * @param ttl TTL.
     * @param expireTime Expire time.
     */
    public abstract void addNearWriteValue(KeyCacheObject key,
        @Nullable CacheObject val,
        EntryProcessor<Object, Object, Object> entryProcessor,
        long ttl,
        long expireTime);

    /**
     * Cleanup values not needed after message was sent.
     */
    protected abstract void cleanup();

    /**
     * @return Subject ID.
     */
    public final UUID subjectId() {
        return subjId;
    }

    /**
     * @return Task name.
     */
    public final int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @return Future ID on primary node.
     */
    public final long futureId() {
        return futId;
    }

    /**
     * @return Future ID on near node.
     */
    public final long nearFutureId() {
        return nearFutId;
    }

    /**
     * @return Write version.
     */
    public final GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @return Cache write synchronization mode.
     */
    public final CacheWriteSynchronizationMode writeSynchronizationMode() {
        return syncMode;
    }

    /**
     * @return Keys size.
     */
    public abstract int size();

    /**
     * @return Keys size.
     */
    public abstract int nearSize();

    /**
     * @param key Key to check.
     * @return {@code true} if request keys contain key.
     */
    public abstract boolean hasKey(KeyCacheObject key);

    /**
     * @param idx Key index.
     * @return Key.
     */
    public abstract KeyCacheObject key(int idx);

    /**
     * @return Obsolete near cache keys size.
     */
    public abstract int obsoleteNearKeysSize();

    /**
     * @param idx Obsolete near cache key index.
     * @return Obsolete near cache key.
     */
    public abstract KeyCacheObject obsoleteNearKey(int idx);

    /**
     * @param updCntr Update counter.
     * @return Update counter.
     */
    public abstract Long updateCounter(int updCntr);

    /**
     * @param idx Near key index.
     * @return Key.
     */
    public abstract KeyCacheObject nearKey(int idx);

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable public abstract CacheObject value(int idx);

    /**
     * @param idx Key index.
     * @return Value.
     */
    @Nullable public abstract CacheObject previousValue(int idx);

    /**
     * @param idx Key index.
     * @return Entry processor.
     */
    @Nullable public abstract EntryProcessor<Object, Object, Object> entryProcessor(int idx);

    /**
     * @param idx Near key index.
     * @return Value.
     */
    @Nullable public abstract CacheObject nearValue(int idx);

    /**
     * @param idx Key index.
     * @return Transform closure.
     */
    @Nullable public abstract EntryProcessor<Object, Object, Object> nearEntryProcessor(int idx);

    /**
     * @param idx Index.
     * @return Conflict version.
     */
    @Nullable public abstract GridCacheVersion conflictVersion(int idx);

    /**
     * @param idx Index.
     * @return TTL.
     */
    public abstract long ttl(int idx);

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    public abstract long nearTtl(int idx);

    /**
     * @param idx Index.
     * @return Conflict expire time.
     */
    public abstract long conflictExpireTime(int idx);

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    public abstract long nearExpireTime(int idx);

    /**
     * @return Optional arguments for entry processor.
     */
    @Nullable public abstract Object[] invokeArguments();

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    protected final void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reags flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    final boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 14;
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
            case 4:
                if (!writer.writeMessage("errs", errs))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeLong("nearFutId", nearFutId))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeUuid("nearNodeId", nearNodeId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeUuid("subjId", subjId))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeByte("syncMode", syncMode != null ? (byte)syncMode.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeInt("taskNameHash", taskNameHash))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeMessage("writeVer", writeVer))
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
            case 4:
                errs = reader.readMessage("errs");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                nearFutId = reader.readLong("nearFutId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                nearNodeId = reader.readUuid("nearNodeId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                subjId = reader.readUuid("subjId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                byte syncModeOrd;

                syncModeOrd = reader.readByte("syncMode");

                if (!reader.isLastRead())
                    return false;

                syncMode = CacheWriteSynchronizationMode.fromOrdinal(syncModeOrd);

                reader.incrementState();

            case 11:
                taskNameHash = reader.readInt("taskNameHash");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                writeVer = reader.readMessage("writeVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAtomicAbstractUpdateRequest.class);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (errs != null)
            errs.prepareMarshal(this, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (errs != null)
            errs.finishUnmarshal(this, cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder flags = new StringBuilder();

        if (skipStore())
            appendFlag(flags, "skipStore");
        if (keepBinary())
            appendFlag(flags, "keepBinary");
        if (isFlag(DHT_ATOMIC_NEAR_FLAG_MASK))
            appendFlag(flags, "near");
        if (hasResult())
            appendFlag(flags, "hasRes");
        if (replyWithoutDelay())
            appendFlag(flags, "resNoDelay");

        return S.toString(GridDhtAtomicAbstractUpdateRequest.class, this,
            "flags", flags.toString());
    }
}
