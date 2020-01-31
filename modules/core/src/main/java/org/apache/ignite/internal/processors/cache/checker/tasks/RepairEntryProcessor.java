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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;

/** Entry processor to repair inconsistent entries. */
public class RepairEntryProcessor implements EntryProcessor {
    /** Value to set. */
    private Object val;

    /** Map of nodes to corresponding versioned values */
    private Map<UUID, VersionedValue> data;

    /** deferred delete queue max size. */
    private long rmvQueueMaxSize;

    /** Force repair flag. */
    private boolean forceRepair;

    /** Start topology version. */
    private AffinityTopologyVersion startTopVer;

    /**
     *
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public RepairEntryProcessor(
        Object val,
        Map<UUID, VersionedValue> data,
        long rmvQueueMaxSize,
        boolean forceRepair,
        AffinityTopologyVersion startTopVer) {
        this.val = val;
        this.data = data;
        this.rmvQueueMaxSize = rmvQueueMaxSize;
        this.forceRepair = forceRepair;
        this.startTopVer = startTopVer;
    }

    /**
     * Do repair logic.
     *
     * @param entry Entry to fix.
     * @param arguments Arguments.
     * @return {@code True} if was successfully repaired, {@code False} otherwise.
     * @throws EntryProcessorException If failed.
     */
    @SuppressWarnings("unchecked")
    @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
        GridCacheContext cctx = cacheContext(entry);
        CacheObjectContext oCtx = cctx.cacheObjectContext();
        GridCacheVersion currKeyGridCacheVer = keyVersion(entry);

        if (topologyChanged(cctx, startTopVer))
            throw new EntryProcessorException("Topology version was changed");

        UUID locNodeId = cctx.localNodeId();
        VersionedValue versionedVal = data.get(locNodeId);

        CacheObject expectVal = Optional.ofNullable(data.get(locNodeId)).map(VersionedValue::value).orElse(null);
        CacheObject currVal = (CacheObject)entry.getValue(); //TODO if it primitive type return primitive type;

        try {
            if (expectVal == null && currVal != null
                || expectVal != null && currVal == null
                || expectVal != null && !Arrays.equals(expectVal.valueBytes(oCtx), currVal.valueBytes(oCtx)))
                return false;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        if (forceRepair) {
            if (val == null)
                entry.remove();
            else
                entry.setValue(val);

            return true;
        }

        if (versionedVal != null) {
            if (currKeyGridCacheVer.compareTo(versionedVal.version()) == 0) {
                if (val == null)
                    entry.remove();
                else
                    entry.setValue(val);

                return true;
            }

            // TODO: 23.12.19 Add optimizations here
        }
        else {
            if (currKeyGridCacheVer.compareTo(new GridCacheVersion(0, 0, 0)) == 0) {
                long recheckStartTime = minValue(VersionedValue::recheckStartTime);

                boolean inEntryTTLBounds =
                    (System.currentTimeMillis() - recheckStartTime) < Long.getLong(IGNITE_CACHE_REMOVED_ENTRIES_TTL);

                // Min available update counter for the key at all nodes.
                // It just fast solution for null value problem. We should use other way to fix it (versionedVal.updateCounter()).
                long minUpdateCntr = minValue(VersionedValue::updateCounter);
                long currUpdateCntr = updateCounter(cctx, entry.getKey());

                boolean inDeferredDelQueueBounds = ((currUpdateCntr - minUpdateCntr) < rmvQueueMaxSize);

                if ((inEntryTTLBounds && inDeferredDelQueueBounds)) {
                    if (val == null)
                        entry.remove();
                    else
                        entry.setValue(val);

                    return true;
                }
            }
        }

        return false;
    }

    /**
     *
     */
    protected GridCacheContext cacheContext(MutableEntry entry) {
        return (GridCacheContext)entry.unwrap(GridCacheContext.class);
    }

    /**
     *
     */
    protected boolean topologyChanged(GridCacheContext cctx, AffinityTopologyVersion expTop) {
        AffinityTopologyVersion currTopVer = cctx.affinity().affinityTopologyVersion();

        return !cctx.shared().exchange().lastAffinityChangedTopologyVersion((currTopVer)).equals(expTop);
    }

    /**
     * @return Current {@link GridCacheVersion}
     */
    protected GridCacheVersion keyVersion(MutableEntry entry) {
        CacheEntry verEntry = (CacheEntry)entry.unwrap(CacheEntry.class);

        return (GridCacheVersion)verEntry.version();
    }

    /**
     * @return Current update counter
     */
    protected long updateCounter(GridCacheContext cctx, Object affKey) {
        return cctx.topology().localPartition(cctx.cache().affinity().partition(affKey)).updateCounter();
    }

    /**
     * @return target min long value
     */
    private long minValue(Function<VersionedValue, Long> mapper) {
        return data.values().stream()
            .mapToLong(mapper::apply)
            .min()
            .orElseThrow(() -> new IllegalStateException("Unreachable state [mapper = " + mapper.getClass().getName() + ", data=" + data + "]."));
    }
}
